import streamlit as st
import pandas as pd
from jinja2 import Template
from datetime import datetime
import os
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import traceback
from io import BytesIO
import zipfile
import numpy as np
from weasyprint import HTML, CSS
import requests
import smtplib
from email.header import Header
from email.utils import formataddr
import shutil


# 페이지 기본 설정
st.set_page_config(
    page_title="크리에이터 보고서 생성기",
    page_icon="📊",
    layout="wide"
)

def get_smtp_info(email_account):
    """
    이메일 계정에 따라 SMTP 서버와 포트를 반환합니다.
    네이버 이메일의 경우 smtp.naver.com, Gmail의 경우 smtp.gmail.com을 사용합니다.
    """
    email_account = email_account.lower()
    if "naver.com" in email_account:
        return ("smtp.naver.com", 465)
    elif "gmail.com" in email_account:
        return ("smtp.gmail.com", 587)
    else:
        # 다른 이메일도 기본값(예: Gmail)으로 설정하거나, 필요에 따라 분기 추가
        return ("smtp.gmail.com", 587)


def normalize_creator_id(creator_id):
    """크리에이터 ID를 정규화합니다."""
    if not creator_id or pd.isna(creator_id):
        return ''
    
    # 문자열로 변환
    creator_id = str(creator_id)
    
    # 앞뒤 공백 제거 (중간 공백은 유지)
    creator_id = creator_id.strip()
    
    # 인코딩 정규화 (모든 유니코드 문자를 NFC로 정규화)
    try:
        import unicodedata
        creator_id = unicodedata.normalize('NFC', creator_id)
    except Exception as e:
        print(f"정규화 중 오류 발생: {str(e)}")
    
    # 특수 공백 문자 제거 (일반 공백은 유지)
    creator_id = creator_id.replace('\u3000', ' ')  # 전각 공백
    creator_id = creator_id.replace('\xa0', ' ')    # 줄바꿈 없는 공백
    creator_id = creator_id.strip()
    
    # 연속된 공백을 하나의 공백으로 변경
    import re
    creator_id = re.sub(r'\s+', ' ', creator_id)
    
    return creator_id


class DataValidator:
    def __init__(self, original_df, creator_info_handler):
        """데이터 검증을 위한 초기화"""
        self.original_df = original_df.copy()  # 데이터프레임 복사
        
        self.summary_row = original_df.iloc[0]  # 2행(인덱스 0)의 합계 데이터
        
        self.data_rows = original_df.iloc[1:]   # 3행(인덱스 1)부터의 실제 데이터
        
        # 아이디 컬럼 정규화
        self.data_rows.loc[:, '아이디'] = self.data_rows['아이디'].astype(str).str.strip()
        self.original_df.loc[:, '아이디'] = self.original_df['아이디'].astype(str).str.strip()
        
        self.creator_info_handler = creator_info_handler
        self.commission_rates = self._get_commission_rates()
        self.total_stats = self._calculate_total_stats()
        self.creator_stats = self._calculate_creator_stats()

    def _get_commission_rates(self):
        """크리에이터별 수수료율을 가져옵니다."""
        return {creator_id: self.creator_info_handler.get_commission_rate(creator_id) 
                for creator_id in self.creator_info_handler.get_all_creator_ids()}




    def _calculate_total_stats(self):
        """전체 통계를 계산합니다."""
        creator_revenues = self.original_df.groupby('아이디').agg({
            '대략적인 파트너 수익 (KRW)': 'sum'
        })
        total_revenue_after = sum(
            revenue * self.commission_rates.get(creator_id, 0)
            for creator_id, revenue in creator_revenues['대략적인 파트너 수익 (KRW)'].items()
        )
        
        summary_stats = {
            'creator_count': len(self.original_df['아이디'].unique()),
            'total_views_summary': self.summary_row['조회수'],
            'total_revenue_summary': self.summary_row['대략적인 파트너 수익 (KRW)'],
            'total_views_data': self.original_df['조회수'].sum(),
            'total_views_data2': self.data_rows['조회수'].sum(),
            'total_revenue_data': self.original_df['대략적인 파트너 수익 (KRW)'].sum(),
            'total_revenue_data2': self.data_rows['대략적인 파트너 수익 (KRW)'].sum(),
            'total_revenue_after': total_revenue_after
        }
        return summary_stats


    def _calculate_creator_stats(self):
        """크리에이터별 통계를 계산합니다."""
        grouped = self.original_df.groupby('아이디', as_index=False).agg({
            '조회수': 'sum',
            '대략적인 파트너 수익 (KRW)': 'sum'
        })
        return grouped

    def compare_creator_stats(self, processed_df):
        """크리에이터별 통계를 비교합니다."""
        processed_df = processed_df.copy()  # 데이터프레임 복사
        processed_df.loc[:, '아이디'] = processed_df['아이디'].astype(str).str.strip()
        
        processed_creator_stats = processed_df.groupby('아이디', as_index=False).agg({
            '조회수': 'sum',
            '대략적인 파트너 수익 (KRW)': 'sum'
        })

        merged_stats = pd.merge(
            self.creator_stats,
            processed_creator_stats,
            on='아이디',
            suffixes=('_original', '_processed'),
            how='outer'
        )
        
        # NaN 값을 0으로 채우기
        merged_stats = merged_stats.fillna(0)
        
        merged_stats['views_match'] = abs(merged_stats['조회수_original'] - merged_stats['조회수_processed']) < 1
        merged_stats['revenue_match'] = abs(
            merged_stats['대략적인 파트너 수익 (KRW)_original'] -
            merged_stats['대략적인 파트너 수익 (KRW)_processed']
        ) < 1
        return merged_stats




class CreatorInfoHandler:
    def __init__(self, info_file):
        """크리에이터 정보 파일을 읽어서 초기화합니다."""
        try:
            # 파일 읽기 시도 (UTF-8)
            if info_file.name.endswith('.csv'):
                self.creator_info = pd.read_csv(info_file, encoding='utf-8-sig')
            else:
                self.creator_info = pd.read_excel(info_file)
        except UnicodeDecodeError:
            # UTF-8 실패시 CP949로 시도
            info_file.seek(0)
            self.creator_info = pd.read_csv(info_file, encoding='cp949')
        
        # 필수 컬럼 확인
        required_columns = ['아이디', 'channel', 'percent', 'email']
        if not all(col in self.creator_info.columns for col in required_columns):
            raise ValueError("크리에이터 정보 파일에 필수 컬럼이 누락되었습니다.")
        
        # 데이터 정규화
        self.creator_info = self.creator_info[required_columns].copy()
        self.creator_info['아이디'] = self.creator_info['아이디'].fillna('')
        self.creator_info['아이디'] = self.creator_info['아이디'].apply(normalize_creator_id)
        
        # percent 컬럼 처리
        self.creator_info['percent'] = pd.to_numeric(self.creator_info['percent'], errors='coerce')
        self.creator_info['percent'] = self.creator_info['percent'].fillna(1.0)
        
        # 크리에이터 정보를 딕셔너리로 변환
        self.commission_rates = {}
        for _, row in self.creator_info.iterrows():
            creator_id = normalize_creator_id(row['아이디'])
            if creator_id:  # 빈 문자열이 아닌 경우만 저장
                self.commission_rates[creator_id] = float(row['percent'])
                print(f"수수료율 저장: {creator_id} ({creator_id.encode('utf-8')}) -> {float(row['percent'])}")
        
        # 디버깅용 출력
        print("\n크리에이터 수수료율 정보 (바이트 표현):")
        for creator_id, rate in self.commission_rates.items():
            print(f"- {creator_id} ({creator_id.encode('utf-8')}): {rate}")

    def get_commission_rate(self, creator_id):
        """크리에이터의 수수료율을 반환합니다."""
        try:
            if not creator_id or pd.isna(creator_id):
                return 1.0
            
            creator_id = normalize_creator_id(creator_id)
            
            # 디셔너리에서 수수료율 조회
            rate = self.commission_rates.get(creator_id)
            if rate is not None:
                return rate
            
            # 경고 메시지 제거 (이미 process_data에서 처리됨)
            return 1.0
            
        except Exception as e:
            print(f"수수료율 조회 중 오류 발생 ({creator_id}): {str(e)}")
            traceback.print_exc()
            return 1.0
    
    def get_email(self, creator_id):
        """크리에이터의 이메일 주소를 반환합니다."""
        try:
            creator_id = str(creator_id).strip()
            matching_rows = self.creator_info[self.creator_info['아이디'] == creator_id]
            if matching_rows.empty:
                st.warning(f"크리에이터 '{creator_id}'의 이메일 정보가 없습니다.")
                return None
            return matching_rows['email'].iloc[0]
        except Exception as e:
            st.warning(f"이메일 조회 중 오류 발생 ({creator_id}): {str(e)}")
            return None
    
    def get_all_creator_ids(self):
        """모든 크리에이터 ID를 반환합니다."""
        if self.creator_info is None or self.creator_info.empty:
            return []
        return [id for id in self.creator_info['아이디'].unique() if id and not pd.isna(id)]

def clean_numeric_value(value):
    """숫자 값을 안전하게 정수로 변환합니다."""
    try:
        if pd.isna(value):
            return 0
        if isinstance(value, str):
            value = value.replace(',', '')
        return int(float(value))
    except (ValueError, TypeError):
        return 0


def show_validation_results(original_df, processed_df, creator_info_handler):
    """검증 결과를 표시합니다."""
    st.header("🔍 처리 결과 검증")
    
    validator = DataValidator(original_df, creator_info_handler)
    
    # 전체 데이터 요약 - 표 형식 변경
    st.subheader("전체 데이터 요약")
    summary_data = {
        '전체 크리에이터 수': validator.total_stats['creator_count'],
        '총 조회수': validator.total_stats['total_views_data'],
        '총 수익': validator.total_stats['total_revenue_data'],
        '정산 후 총 수익': validator.total_stats['total_revenue_after']
    }
    summary_df = pd.DataFrame([summary_data])
    st.dataframe(summary_df.style.format({
        '총 조회수': '{:,}',
        '총 수익': '₩{:,.3f}',
        '정산 후 총 수익': '₩{:,.3f}'
    }), use_container_width=True)
    
    # 전체 데이터 검증
    st.subheader("전체 데이터 검증")
    comparison_df = pd.DataFrame({
        '원본 데이터': [
            validator.total_stats['total_views_data'],
            validator.total_stats['total_revenue_data']
        ],
        '처리 후 데이터': [
            processed_df['조회수'].sum(),
            processed_df['대략적인 파트너 수익 (KRW)'].sum()
        ],
        '일치 여부': [
            abs(validator.total_stats['total_views_data'] - processed_df['조회수'].sum()) < 1,
            abs(validator.total_stats['total_revenue_data'] - processed_df['대략적인 파트너 수익 (KRW)'].sum()) < 1
        ]
    }, index=['총 조회수', '총 수익'])
    
    st.dataframe(
        comparison_df.style.format({
            '원본 데이터': '{:,.0f}',
            '처리 후 데이터': '{:,.0f}'
        }).apply(
            lambda x: ['background-color: #e6ffe6' if v else 'background-color: #ffe6e6' for v in x], 
            subset=['일치 여부']
        ),
        use_container_width=True
    )

    # 크리에이터별 검증
    st.subheader("크리에이터별 검증")
    creator_comparison = validator.compare_creator_stats(processed_df)
    creator_comparison['수수료율'] = creator_comparison['아이디'].map(
        lambda x: creator_info_handler.get_commission_rate(x)
    )
    creator_comparison['수수료 후 수익'] = creator_comparison['대략적인 파트너 수익 (KRW)_processed'] * creator_comparison['수수료율']
    
    # 칼럼 순서 재정렬
    columns_order = [
        '아이디',
        '조회수_original',
        '조회수_processed',
        'views_match',
        '대략적인 파트너 수익 (KRW)_original',
        '대략적인 파트너 수익 (KRW)_processed',
        'revenue_match',
        '수수료율',
        '수수료 후 수익'
    ]
    
    creator_comparison = creator_comparison[columns_order]
    
    st.dataframe(
        creator_comparison.style.format({
            '조회수_original': '{:,.0f}',
            '조회수_processed': '{:,.0f}',
            '대략적인 파트너 수익 (KRW)_original': '₩{:,.0f}',
            '대략적인 파트너 수익 (KRW)_processed': '₩{:,.0f}',
            '수수료율': '{:.2%}',
            '수수료 후 수익': '₩{:,.0f}'
        }).apply(
            lambda x: ['background-color: #e6ffe6' if v else 'background-color: #ffe6e6' for v in x], 
            subset=['views_match', 'revenue_match']
        ),
        use_container_width=True
    )
    
    # 검증 결과를 세션 상태에 저장
    st.session_state['validation_summary'] = summary_df
    st.session_state['validation_comparison'] = comparison_df
    st.session_state['validation_creator_comparison'] = creator_comparison

def create_video_data(df):
    """데이터프레임에서 비디오 데이터를 추출합니다."""
    import base64
    
    video_data = []
    for _, row in df.iterrows():
        try:
            if pd.isna(row['동영상 제목']):
                continue
            
            # base64로 인코딩했다가 다시 디코딩
            title = str(row['동영상 제목'])
            title_bytes = title.encode('utf-8')
            title_base64 = base64.b64encode(title_bytes)
            title = base64.b64decode(title_base64).decode('utf-8')
            
            video_data.append({
                'title': title,
                'views': clean_numeric_value(row['조회수']),
                'revenue': clean_numeric_value(row['수수료 후 수익'])
            })
            
        except Exception as e:
            print(f"Error processing title: {str(e)}")
            video_data.append({
                'title': '[인코딩 오류]',
                'views': clean_numeric_value(row['조회수']),
                'revenue': clean_numeric_value(row['수수료 후 수익'])
            })
            
    return video_data


def generate_html_report(data):
    """HTML 보고서를 생성합니다."""
    try:
        template_path = 'templates/template.html'
        with open(template_path, 'r', encoding='utf-8') as f:
            template_str = f.read()
        
        # 데이터 검증
        for video in data['videoData']:
            video['title'] = validate_text(video['title'])
            # 디버깅용 출력
            print(f"Processing title: {video['title']}")
            print(f"Title bytes: {video['title'].encode('utf-8')}")
        
        template = Template(template_str)
        template.globals['format_number'] = lambda x, decimals=0: "{:,.{}f}".format(float(x), decimals)
        
        # 디버깅용 출력
        html_content = template.render(**data)
        print("Generated HTML sample:", html_content[:1000])
        
        return html_content
        
    except Exception as e:
        st.error(f"HTML 생성 실패 ({data['creatorName']}): {str(e)}")
        st.write(traceback.format_exc())
        return None



def create_pdf_from_html(html_content, creator_id):
    """HTML 내용을 PDF로 변환합니다."""
    try:
        print(f"[DEBUG] PDF 생성 시작 - 크리에이터: {creator_id}")
        
        # 폰트 설정
        from weasyprint import HTML, CSS
        from weasyprint.text.fonts import FontConfiguration
        font_config = FontConfiguration()
        
        # HTML 직접 생성 (외부 리소스 요청 없이)
        html_doc = HTML(
            string=html_content,
            encoding='utf-8',
            base_url=None  # base_url 명시적으로 None으로 설정
        )

        # CSS 설정
        css = CSS(string='''
            @page {
                size: A4 portrait;
                margin: 8mm;
            }

            body {
                font-family: system-ui, -apple-system, sans-serif;
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            /* RTL(Right-to-Left) 텍스트 지원 */
            [dir="rtl"] { 
                text-align: right; 
                font-family: 'Noto Sans Arabic', sans-serif;
            }
            
            .report-container {
                max-width: 100%;
                padding: 8px;
            }
            .header {
                margin-bottom: 12px;
            }
            .header h1 {
                font-size: 21px !important;
                margin-bottom: 6px;
                line-height: 1.2;
                font-weight: bold;
            }
            .header .period {
                font-size: 13px;
                margin: 6px 0;
            }
            .header .disclaimer {
                font-size: 11px;
                margin: 4px 0;
                line-height: 1.3;
            }
            .stats-grid {
                max-width: 100%;
                gap: 10px;
                margin-bottom: 10px;
            }
            .stat-card {
                padding: 10px;
            }
            .stat-card h3 {
                font-size: 13px;
                margin-bottom: 4px;
            }
            .stat-card .value {
                font-size: 18px;
            }
            .earnings-table {
                margin-top: 10px;
                font-size: 0.7em;
                border-spacing: 0;
                border-collapse: collapse;
            }
            .earnings-table th {
                font-size: 0.95em;
                padding: 2px 3px;
                line-height: 1;
            }
            .earnings-table td {
                padding: 1px 3px;
                line-height: 1;
            }
            .earnings-table tr {
                height: auto !important;
                border-bottom: 0.5px solid #e9ecef;
            }
            .earnings-table tbody tr {
                margin: 0 !important;
                padding: 0 !important;
            }
            .earnings-table th,
            .earnings-table td {
                margin: 0 !important;
                vertical-align: middle;
            }
        ''')
        
        # PDF 생성
        pdf_buffer = BytesIO()
        html_doc.write_pdf(
            target=pdf_buffer,
            stylesheets=[css],
            font_config=font_config
        )
        pdf_buffer.seek(0)

        # PDF 생성 결과 확인
        pdf_content = pdf_buffer.getvalue()
        print(f"[DEBUG] PDF 생성 완료 - 크기: {len(pdf_content)} bytes")
        
        return pdf_content
        
    except Exception as e:
        print(f"[ERROR] PDF 생성 중 오류 발생: {str(e)}")
        traceback.print_exc()
        return None



def create_validation_excel(original_df, processed_df, creator_info_handler):
    """검증 결과를 담은 엑셀 파일을 생성합니다."""
    validator = DataValidator(original_df, creator_info_handler)
    
    summary_data = {
        '항목': ['전체 크리에이터 수', '총 조회수', '총 수익', '정산 후 총 수익'],
        '값': [
            validator.total_stats['creator_count'],
            validator.total_stats['total_views_data'],
            validator.total_stats['total_revenue_data'],
            validator.total_stats['total_revenue_after']
        ]
    }
    summary_df = pd.DataFrame(summary_data)
    
    validation_data = {
        '항목': ['총 조회수', '총 수익'],
        '원본 데이터': [
            validator.total_stats['total_views_data'],
            validator.total_stats['total_revenue_data']
        ],
        '처리 후 데이터': [
            processed_df['조회수'].sum(),
            processed_df['대략적인 파트너 수익 (KRW)'].sum()
        ]
    }
    validation_df = pd.DataFrame(validation_data)
    
    creator_comparison = validator.compare_creator_stats(processed_df)
    creator_comparison['수수료율'] = creator_comparison['아이디'].map(
        lambda x: creator_info_handler.get_commission_rate(x)
    )
    creator_comparison['수수료 후 수익'] = creator_comparison['대략적인 파트너 수익 (KRW)_processed'] * creator_comparison['수수료율']
    
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        summary_df.to_excel(writer, sheet_name='전체 데이터 요약', index=False)
        validation_df.to_excel(writer, sheet_name='전체 데이터 검증', index=False)
        creator_comparison.to_excel(writer, sheet_name='크리에이터별 검증', index=False)
    
    excel_buffer.seek(0)
    return excel_buffer.getvalue()

def create_zip_file(reports_data, excel_files, original_df=None, processed_df=None, creator_info_handler=None):
    """보고서와 엑셀 파일들을 ZIP 파일로 압축합니다."""
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # HTML 보고서 및 PDF 추가
        for filename, content in reports_data.items():
            # HTML 파일 추가
            zip_file.writestr(f"reports/html/{filename}", content)
            
            # PDF 파일 생성 및 추가
            creator_id = filename.replace('_report.html', '')
            pdf_content = create_pdf_from_html(content, creator_id)
            if pdf_content:
                pdf_filename = filename.replace('.html', '.pdf')
                zip_file.writestr(f"reports/pdf/{pdf_filename}", pdf_content)
        
        # 엑셀 파일 추가
        for filename, content in excel_files.items():
            zip_file.writestr(f"excel/{filename}", content)
            
        # 검증 결과 엑셀 추가
        if all([original_df is not None, processed_df is not None, creator_info_handler is not None]):
            validation_excel = create_validation_excel(original_df, processed_df, creator_info_handler)
            zip_file.writestr("validation/validation_results.xlsx", validation_excel)
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def validate_text(text):
    """텍스트 데이터 검증"""
    if not isinstance(text, str):
        return str(text)
    try:
        # 여러 단계의 인코딩/디코딩 검증
        validated = text.encode('utf-8').decode('utf-8')
        return validated
    except Exception as e:
        print(f"Text validation error: {str(e)}")
        return text


def process_data(input_df, creator_info_handler, start_date, end_date, 
                email_user=None, email_password=None,
                progress_container=None, status_container=None, validation_container=None):
    """데이터를 처리하고 보고서를 생성합니다."""
    import unicodedata  # 여기에 추가

    try:
        # 입력 데이터프레임 처리 부분
        input_df = input_df.copy()

        # 문자열 데이터 인코딩 처리
        if '동영상 제목' in input_df.columns:
            input_df['동영상 제목'] = input_df['동영상 제목'].apply(
                lambda x: x.encode('utf-8').decode('utf-8') if isinstance(x, str) else str(x)
            )
        
        # NaN 값 처리 및 아이디 정규화
        input_df['아이디'] = input_df['아이디'].fillna('')
        input_df['아이디'] = input_df['아이디'].astype(str).str.strip()
        
        # 빈 아이디 또는 'nan' 제거
        input_df = input_df[input_df['아이디'].apply(lambda x: x != '' and x.lower() != 'nan')]
        
        # 크리에이터 목록 추출
        input_df['아이디'] = input_df['아이디'].apply(normalize_creator_id)
        unique_creators = sorted(input_df[input_df['아이디'] != '']['아이디'].unique())
        
        # 수수료율 정보 정규화하여 저장
        commission_rates = {}
        for creator_id in unique_creators:
            normalized_id = normalize_creator_id(creator_id)
            print(f"\n크리에이터 ID 매칭 시도: {normalized_id} ({normalized_id.encode('utf-8')})")
            print("저장된 수수료율 키:", [f"{k} ({k.encode('utf-8')})" for k in creator_info_handler.commission_rates.keys()])
            
            if normalized_id in creator_info_handler.commission_rates:
                rate = creator_info_handler.commission_rates[normalized_id]
                commission_rates[normalized_id] = rate
                print(f"수수료율 매칭 성공: {normalized_id} -> {rate}")
            else:
                print(f"수수료율 매칭 실패: {normalized_id}")
                commission_rates[normalized_id] = 1.0  # 기본값 설정
        
        print("\n수수료율 정보 확인:")
        for creator_id in unique_creators:
            rate = commission_rates[creator_id]  # 이미 저장된 값 사용
            print(f"- {creator_id}: {rate}")
        
        total_creators = len(unique_creators)
        if total_creators == 0:
            st.error("처리할 크리에이터 데이터가 없습니다.")
            return None, None, None
        
        reports_data = {}
        excel_files = {}
        processed_full_data = pd.DataFrame()
        failed_creators = []
        
        if progress_container:
            progress_bar = progress_container.progress(0)
            progress_text = progress_container.empty()
            progress_status = progress_container.empty()
            failed_status = progress_container.empty()
        
        for idx, creator_id in enumerate(unique_creators):
            try:
                if progress_container:
                    progress = (idx + 1) / total_creators
                    progress_bar.progress(progress)
                    progress_text.write(f"진행 상황: {idx + 1}/{total_creators} - {creator_id} 처리 중...")
                
                # 데이터 필터링 및 처리
                creator_data = input_df[input_df['아이디'] == creator_id].copy()
                if creator_data.empty:
                    failed_creators.append(f"{creator_id} (데이터 없음)")
                    continue
                
                # 수치 데이터 처리
                creator_data['조회수'] = pd.to_numeric(creator_data['조회수'], errors='coerce').fillna(0)
                creator_data['대략적인 파트너 수익 (KRW)'] = pd.to_numeric(creator_data['대략적인 파트너 수익 (KRW)'], errors='coerce').fillna(0)
                
                # 수수료율 적용 (get_commission_rate 호출하지 않고 직접 사용)
                commission_rate = commission_rates[creator_id]
                print(f"수수료율 적용: {creator_id} -> {commission_rate}")
                
                creator_data['수수료 적용 수익'] = creator_data['대략적인 파트너 수익 (KRW)'] * commission_rate
                
                # 통계 계산
                total_views = int(creator_data['조회수'].sum())
                total_revenue = int(creator_data['대략적인 파트너 수익 (KRW)'].sum())
                total_revenue_after = int(total_revenue * commission_rate)
                
                processed_full_data = pd.concat([processed_full_data, creator_data])
                
                # 수수료 적용 수익 계산
                creator_data['수수료 후 수익'] = creator_data['대략적인 파트너 수익 (KRW)'] * commission_rate
                
                # 상위 50개 데이터 필터링 (조회수)
                filtered_data = creator_data.nlargest(50, '조회수').copy()
                
                # 총계 행 추가
                total_row = pd.Series({
                    '콘텐츠': '총계',
                    '조회수': total_views,
                    '대략적인 파트너 수익 (KRW)': total_revenue,
                    '수수료 후 수익': total_revenue_after
                }, name='total')
                filtered_data = pd.concat([filtered_data, pd.DataFrame([total_row])], ignore_index=True)
                
                # 엑셀 파일 생성
                excel_buffer = BytesIO()
                filtered_data.to_excel(excel_buffer, index=False)
                excel_buffer.seek(0)
                excel_files[f"{creator_id}.xlsx"] = excel_buffer.getvalue()
                
                # 보고서 데이터 생성
                report_data = {
                    'creatorName': creator_id,
                    'period': f"{start_date.strftime('%y.%m.%d')} - {end_date.strftime('%y.%m.%d')}",
                    'totalViews': total_views,
                    'totalRevenue': total_revenue_after,  # 수수료 제용된 총 수익
                    'commission_rate': commission_rate,  # 수수료율 추가
                    'videoData': create_video_data(filtered_data)  # 필터링된 데이터 전달
                }
                
                # HTML 보고서 생성
                html_content = generate_html_report(report_data)
                if html_content:
                    reports_data[f"{creator_id}_report.html"] = html_content
                
                # PDF 보고서 생성
                pdf_content = create_pdf_from_html(html_content, creator_id)
                if pdf_content:
                    reports_data[f"{creator_id}_report.pdf"] = pdf_content
                
            except Exception as e:
                failed_creators.append(f"{creator_id} (오류: {str(e)})")
                if status_container:
                    status_container.error(f"{creator_id} 처리 중 오류: {str(e)}")
                continue
        
        if failed_creators:
            st.warning(f"처리 실패한 크리에이터들: {', '.join(failed_creators)}")
        
        if not reports_data and not excel_files:
            st.error("생성된 보고서가 없습니다.")
            return None, None, None
            
        # 처리 완료 후 상태 업데이트
        if progress_container:
            progress_text.write(f"진행 상황: {total_creators}/{total_creators} - 처리 완료")
            progress_status.write("처리 완료")
            failed_status.write(f"실패: {', '.join(failed_creators) if failed_creators else 'None'}")
            
            # 먼저 세션 상태에 데이터 저장
            st.session_state['statistics_df'] = input_df
            st.session_state['processed_df'] = processed_full_data
            
            # 그 다음 검증 결과 표시
            if not processed_full_data.empty and validation_container:
                with validation_container:
                    show_validation_results(
                        input_df,                # 직접 데이터 사용
                        processed_full_data,     # 직접 데이터 사용
                        creator_info_handler     # 현재 핸들러 사용
                    )
                    st.session_state['validation_results'] = True
        
            # 관리자에게 자동으로 이메일 발송
            if email_user and email_password:
                try:
                    smtp_server, smtp_port = get_smtp_info(email_user)
                
                    # 네이버 메일인 경우 SMTP_SSL 사용
                    if "naver.com" in email_user.lower():
                        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
                    else:
                        server = smtplib.SMTP(smtp_server, smtp_port)
                        server.starttls()
                    server.login(email_user, email_password)

                    zip_data = create_zip_file(reports_data, excel_files, input_df, processed_full_data, creator_info_handler)
                    
                    admin_msg = MIMEMultipart()
                    admin_msg["From"] = email_user
                    admin_msg["To"] = email_user
                    admin_msg["Subject"] = f"크리에이터 보고서 생성 결과 ({datetime.now().strftime('%Y-%m-%d %H:%M')})"
                    
                    admin_body = """안녕하세요,\n\n생성된 보고서를 확인용으로 발송드립니다.\n크리에이터들에게는 이메일 발송 버튼을 통해 개별적으로 발송하실 수 있습니다.\n\n감사합니다."""
                    
                    admin_msg.attach(MIMEText(admin_body, "plain"))
                    attachment = MIMEApplication(zip_data, _subtype="zip")
                    attachment.add_header('Content-Disposition', 'attachment', filename='reports.zip')
                    admin_msg.attach(attachment)
                    
                    server.send_message(admin_msg)
                    server.quit()
                    
                    if status_container:
                        status_container.success("관리자 이메일로 보고서가 발송되었습니다.")
                        st.session_state['admin_email_sent'] = True
                    
                except Exception as e:
                    if status_container:
                        status_container.error(f"관리자 이메일 발송 실패: {str(e)}")

        return reports_data, excel_files, processed_full_data
        
    except Exception as e:
        st.error(f"전체 처리 중 오류 발생: {str(e)}")
        st.write(traceback.format_exc())
        return None, None, None

def send_creator_emails(reports_data, creator_info_handler, email_user, email_password, 
                       email_subject_template, email_body_template, cc_addresses=None, bcc_addresses=None):
    """크리에이터들에게 이메일을 발송합니다."""
    failed_creators = []
    
    try:
        # SMTP 서버 연결
        placeholder = st.empty()
        placeholder.info("SMTP 서버에 연결 중...")

        # **수정된 부분: 이메일 계정에 따른 SMTP 서버 선택**
        smtp_server, smtp_port = get_smtp_info(email_user)
        
        # 네이버 계정이면 SMTP_SSL 사용 (보통 포트 465)
        if "naver.com" in email_user.lower():
            # get_smtp_info에서 반환된 포트를 사용
            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        else:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()

        placeholder.info("로그인 시도 중...")
        server.login(email_user, email_password)
        placeholder.success("SMTP 서버 연결 및 로그인 성공")

        # 크리에이터별 이메일 발송
        pdf_files = {k: v for k, v in reports_data.items() if k.endswith('_report.pdf')}
        placeholder.info(f"총 {len(pdf_files)}개의 크리에이터 보고서 처리 예정")
        
        status_placeholder = st.empty()
        for filename, content in pdf_files.items():
            creator_id = filename.replace('_report.pdf', '')
            try:
                email = creator_info_handler.get_email(creator_id)
                if not email:
                    status_placeholder.warning(f"{creator_id}: 이메일 주소 없음")
                    failed_creators.append(creator_id)
                    continue
                
                status_placeholder.info(f"{creator_id}: 이메일 발송 준비 중 ({email})")
                
                # 이메일 메시지 생성
                msg = MIMEMultipart()
                msg["From"] = formataddr(("이스트블루", email_user))  # 보내는 사람 이름 설정
                msg["To"] = email
                msg["Subject"] = Header(email_subject_template.format(creator_id=creator_id), 'utf-8')  # 제목 인코딩
                
                # (추가) CC 설정
                # cc_addresses가 있다면 "," 로 join
                if cc_addresses:
                    msg["Cc"] = ", ".join(cc_addresses)

                # (추가) BCC 설정
                if bcc_addresses:
                    msg["Bcc"] = ", ".join(bcc_addresses)

                # 템플릿에 크리에이터 ID 적용
                body = email_body_template.format(creator_id=creator_id)
                msg.attach(MIMEText(body, "plain", 'utf-8'))  # 본문 인코딩
                
                # PDF 첨부
                attachment = MIMEApplication(content, _subtype="pdf")
                attachment.add_header('Content-Disposition', 'attachment', 
                                   filename=('utf-8', '', f"{creator_id}_report.pdf"))  # 파일명 인코딩
                msg.attach(attachment)
                
                # 이메일 발송
                status_placeholder.info(f"{creator_id}: 이메일 발송 시도 중...")
                server.send_message(msg)
                status_placeholder.success(f"{creator_id}: 이메일 발송 성공")
                
            except Exception as e:
                status_placeholder.error(f"{creator_id}: 이메일 발송 실패 - {str(e)}")
                failed_creators.append(creator_id)
        
        server.quit()
        placeholder.success("SMTP 서버 연결 종료")
        
    except Exception as e:
        placeholder.error(f"SMTP 서버 연결/인증 실패: {str(e)}")
        return list(creator_info_handler.get_all_creator_ids())
    
    return failed_creators



def extract_creator_name(zip_filename):
    """압축파일명에서 크리에이터명 추출"""
    try:
        # 파일 확장자 제거
        filename_without_ext = zip_filename.replace('.zip', '')
        
        # 날짜 패턴 찾기 (YYYY-MM-DD_YYYY-MM-DD 형식)
        import re
        date_pattern = r'\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}'
        match = re.search(date_pattern, filename_without_ext)
        
        if match:
            # 날짜 패턴이 끝나는 위치 찾기
            date_end_pos = match.end()
            # 날짜 패턴 이후의 첫 공백을 찾아서 그 다음부터가 크리에이터명
            creator_name = filename_without_ext[date_end_pos:].strip()
            return creator_name
            
        return None
    except Exception as e:
        st.error(f"크리에이터명 추출 중 오류: {str(e)}")
        return None

def process_zip_files(uploaded_files):
    """여러 ZIP 파일을 처리하여 하나의 통합된 DataFrame으로 반환"""
    temp_dir = "temp_extract"
    all_data_rows = []  # 실제 데이터 행
    sum_rows = []       # 각 파일의 합계 행
    
    # 원하는 칼럼 순서 정의
    column_order = [
        '아이디',
        '콘텐츠',
        '동영상 제목',
        '동영상 게시 시간',
        '길이',
        '조회수',
        '시청 시간(단위: 시간)',
        '구독자',
        '대략적인 파트너 수익 (KRW)',
        '평균 시청 지속 시간'
    ]
    
    try:
        os.makedirs(temp_dir, exist_ok=True)
        
        for uploaded_file in uploaded_files:
            try:
                # 임시 파일로 ZIP 저장
                zip_path = os.path.join(temp_dir, uploaded_file.name)
                with open(zip_path, 'wb') as f:
                    f.write(uploaded_file.getvalue())
                
                # 크리에이터명 추출
                creator_name = extract_creator_name(uploaded_file.name.replace('.zip', ''))
                if not creator_name:
                    st.warning(f"'{uploaded_file.name}' 파일에서 크리에이터명을 추출할 수 없습니다.")
                    continue
                
                # 압축 해제
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                # '표 데이터.csv' 파일 찾기
                csv_file = None
                for root, dirs, files in os.walk(temp_dir):
                    if '표 데이터.csv' in files:
                        csv_file = os.path.join(root, '표 데이터.csv')
                        break
                
                if not csv_file:
                    st.warning(f"'{uploaded_file.name}'에서 '표 데이터.csv' 파일을 찾을 수 없습니다.")
                    continue
                
                # CSV 파일 읽기
                df = pd.read_csv(csv_file, encoding='utf-8')
                
                # '상위 500개 결과 표시' 행 제거
                df = df[df['콘텐츠'] != '상위 500개 결과 표시']
                
                # 합계 행과 데이터 행 분리
                sum_row = df.iloc[0].copy()
                data_rows = df.iloc[1:].copy()
                
                # '아이디' 칼럼 추가 및 크리에이터명 입력
                if '아이디' not in data_rows.columns:
                    data_rows.insert(0, '아이디', '')  # 첫 번째 위치에 추가
                data_rows['아이디'] = creator_name
                
                # 합계 행에도 '아이디' 칼럼 추가
                if '아이디' not in sum_row.index:
                    sum_row = pd.concat([pd.Series({'아이디': ''}), sum_row])
                
                # 합계 행과 데이터 행 저장
                sum_rows.append(sum_row)
                all_data_rows.append(data_rows)
                
                st.success(f"'{uploaded_file.name}' 처리 완료")
            
            except Exception as e:
                st.error(f"ZIP 파일 처리 중 오류 발생 ({uploaded_file.name}): {str(e)}")
        
        if not all_data_rows:
            return None
        
        # 모든 데이터 행 병합
        final_data = pd.concat(all_data_rows, axis=0, ignore_index=True)
        
        # 숫자형 칼럼 정의
        numeric_cols = ['조회수', '구독자', '길이', '시청 시간(단위: 시간)', '대략적인 파트너 수익 (KRW)']
        
        # 첫 번째 합계 행을 템플릿으로 사용
        final_sum_row = sum_rows[0].copy()
        final_sum_row['콘텐츠'] = '합계'
        final_sum_row['아이디'] = ''
        
        # 모든 합계 행의 숫자형 데이터 합산
        for col in numeric_cols:
            if col in final_sum_row.index:
                final_sum_row[col] = sum(row[col] for row in sum_rows)
        
        # 합계 행을 DataFrame으로 변환
        sum_row_df = pd.DataFrame([final_sum_row])
        
        # 최종 데이터프레임 생성
        final_df = pd.concat([sum_row_df, final_data], ignore_index=True)
        
        # 칼럼 순서 재정렬
        existing_columns = [col for col in column_order if col in final_df.columns]
        final_df = final_df[existing_columns]
        
        return final_df
    
    except Exception as e:
        st.error(f"전체 처리 중 오류 발생: {str(e)}")
        return None
    
    finally:
        # 임시 디렉토리 정리
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)





def main():
    st.title("크리에이터 정산 보고서 생성기")
    
    with st.expander("📝 사용 방법", expanded=False):
        st.markdown("""
        ### 사용 방법
        1. ZIP 파일 변환 (선택사항)
           - 여러 개의 ZIP 파일을 하나의 CSV 파일로 변환
        2. 데이터 기간 설정
        3. 크리에이터 정보 파일 업로드
        4. 통계 데이터 파일 업로드
        5. 이메일 발송 설정
        6. 보고서 생성
        """)
    
    # ZIP 파일 변환 섹션
    st.header("0️⃣ ZIP 파일 변환 (선택사항)")
    with st.expander("ZIP 파일을 CSV로 변환", expanded=False):
        zip_files = st.file_uploader(
            "ZIP 파일 업로드 (여러 개 가능)",
            type=['zip'],
            accept_multiple_files=True,
            key="zip_files",
            help="여러 개의 ZIP 파일을 하나의 CSV 파일로 변환합니다."
        )

        if zip_files:
            if st.button("ZIP 파일 변환", key="convert_zip"):
                with st.spinner('ZIP 파일 처리 중...'):
                    try:
                        converted_df = process_zip_files(zip_files)
                        if converted_df is not None:
                            # CSV 파일로 변환
                            csv_data = converted_df.to_csv(index=False, encoding='utf-8-sig')
                            
                            # 다운로드 버튼 생성
                            st.success("ZIP 파일 변환이 완료되었습니다!")
                            st.download_button(
                                label="변환된 CSV 파일 다운로드",
                                data=csv_data,
                                file_name="통합_통계_데이터.csv",
                                mime="text/csv",
                                key="download_converted_csv"
                            )
                        else:
                            st.error("ZIP 파일 처리 중 오류가 발생했습니다.")
                    except Exception as e:
                        st.error(f"변환 중 오류가 발생했습니다: {str(e)}")
    
    # 파일 업로드 섹션
    st.header("1️⃣ 데이터 파일 업로드")
    
    # 데이터 기간 설정
    st.subheader("📅 데이터 기간 설정")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("시작일", format="YYYY-MM-DD")
    with col2:
        end_date = st.date_input("종료일", format="YYYY-MM-DD")
    
    creator_info = st.file_uploader(
        "크리에이터 정보 파일 (creator_info.xlsx)", 
        type=['xlsx'], 
        key="creator_info"
    )
    statistics = st.file_uploader(
        "통계 데이터 파일 (Excel 또는 CSV)", 
        type=['xlsx', 'csv'], 
        help="Excel(.xlsx) 또는 CSV(.csv) 형식의 파일을 업로드해주세요.",
        key="statistics"
    )
    
    if not (creator_info and statistics):
        st.warning("필요한 파일을 모두 업로드해주세요.")
        st.stop()
    
    # 데이터 검증 섹션
    st.header("2️⃣ 사전 데이터 검증")
    creator_info_handler = CreatorInfoHandler(creator_info)
    
    # 파일 확장자에 따라 다르게 처리
    file_extension = statistics.name.split('.')[-1].lower()
    if file_extension == 'csv':
        statistics_df = pd.read_csv(statistics, encoding='utf-8-sig')  # UTF-8 with BOM 인코딩 사용
    else:
        statistics_df = pd.read_excel(statistics, header=0)
    validator = DataValidator(statistics_df, creator_info_handler)
    


    # 데이터 검증 표시
    st.subheader("📊 전체 통계")
    comparison_data = {
        '항목': ['총 조회수', '총 수익'],
        '합계 행': [
            f"{validator.total_stats['total_views_summary']:,}",
            f"₩{validator.total_stats['total_revenue_summary']:,.3f}"
        ],
        '실제 데이터': [
            f"{validator.total_stats['total_views_data2']:,}",
            f"₩{validator.total_stats['total_revenue_data2']:,.3f}"
        ]
    }

    views_match = abs(validator.total_stats['total_views_summary'] - validator.total_stats['total_views_data']) < 1
    revenue_match = abs(validator.total_stats['total_revenue_summary'] - validator.total_stats['total_revenue_data']) < 1
    comparison_data['일치 여부'] = ['✅' if views_match else '❌', '✅' if revenue_match else '❌']


    
    comparison_df = pd.DataFrame(comparison_data)
    st.dataframe(
        comparison_df.style.apply(
            lambda x: ['background-color: #e6ffe6' if v == '✅' else 
                    'background-color: #ffe6e6' if v == '❌' else '' 
                    for v in x],
            subset=['일치 여부']
        ),
        use_container_width=True
    )

    # 이메일 발송 설정 섹션
    st.header("3️⃣ 이메일 발송 설정")
    send_email = st.checkbox("보고서를 이메일로 발송하기", key="send_email_checkbox")
    email_user = None
    email_password = None
    cc_emails_input = None
    bcc_emails_input = None

    if send_email:
        st.info("""
        이메일 발송을 위해 이메일 계정 설정이 필요합니다:
        1. 이메일 계정 (일반 구글 계정, 네이버 계정)
        2. Gmail 앱 비밀번호 생성 방법:
           - Google 계정 관리 → 보안 → 2단계 인증 → 앱 비밀번호
           - '앱 선택'에서 '기타' 선택 후 앱 비밀번호 생성
        3. Naver 앱 비밀번호 생성 방법:
           - 네이버메일 환경설정 → POP3/IMAP 설정 → 'POP3/SMTP 사용' 사용함 체크
           - 네이버 계정 설정 → 보안설정 → 2단계 인증 → 애플리케이션 비밀번호 관리 → 애플리케이션 비밀번호 생성
        """)
        
        col1, col2 = st.columns(2)
        with col1:
            email_user = st.text_input("Gmail/Naver 계정", placeholder="example@gmail.com", key="email_user")
        with col2:
            email_password = st.text_input("Gmail/Naver 계정 비밀번호", type="password", key="email_password")

        # (추가) CC 이메일 입력
        cc_emails_input = st.text_input(
            "참조(CC) 이메일 주소 (쉼표로 구분)", 
            placeholder="예) example1@domain.com, example2@domain.com",
            key="cc_emails_input"
        )

        # (추가) BCC 이메일 입력
        bcc_emails_input = st.text_input(
            "숨은참조(BCC) 이메일 주소 (쉼표로 구분)", 
            placeholder="예) hidden1@domain.com, hidden2@domain.com",
            key="bcc_emails_input"
        )

    # 보고서 생성 버튼
    st.header("4️⃣ 보고서 생성")
    if st.button("보고서 생성 시작", type="primary", key="generate_report") or ('reports_generated' in st.session_state and st.session_state['reports_generated']):
        try:
            tab1, tab2 = st.tabs(["처리 진행 상황", "검증 결과"])
            
            with tab1:
                progress_container = st.container()
                status_container = st.container()
                
                # 저장된 상태가 있으면 표시
                if 'progress_status' in st.session_state:
                    status_container.write(st.session_state['progress_status'])
                if 'failed_status' in st.session_state:
                    status_container.write(st.session_state['failed_status'])
                if 'admin_email_status' in st.session_state:
                    status_container.write(st.session_state['admin_email_status'])
            
            with tab2:
                validation_container = st.container()
                if 'validation_results' in st.session_state and st.session_state['validation_results']:
                    with validation_container:
                        show_validation_results(
                            st.session_state['statistics_df'],     # 세션에서 데이터 가져오기
                            st.session_state['processed_df'],      # 세션에서 데이터 가져오기
                            creator_info_handler                   # 현재 핸들러 사용
                        )
            
            # 처음 보고서 생성하는 경우에만 실행
            if not ('reports_generated' in st.session_state and st.session_state['reports_generated']):
                with st.spinner('보고서 생성 중...'):
                    reports_data, excel_files, processed_df = process_data(
                        statistics_df,          # 여기서는 statistics_df를 input_df로 전달
                        creator_info_handler,
                        start_date,
                        end_date,
                        email_user=email_user,
                        email_password=email_password,
                        progress_container=progress_container,
                        status_container=status_container,
                        validation_container=validation_container
                    )
                    
                    # 세션 상태에 데이터 저장
                    if reports_data and excel_files:
                        st.session_state['reports_data'] = reports_data
                        st.session_state['creator_info_handler'] = creator_info_handler
                        st.session_state['excel_files'] = excel_files
                        st.session_state['processed_df'] = processed_df
                        
                        st.session_state['reports_generated'] = True
                        
                        # 상태 메시지 저장
                        st.session_state['progress_status'] = "처리 완료"
                        st.session_state['failed_status'] = "실패: None"
                        if 'admin_email_sent' in st.session_state:
                            st.session_state['admin_email_status'] = "관리자 이메일로 보고서가 발송되었습니다."
            
        except Exception as e:
            st.error(f"처리 중 오류가 발생했습니다: {str(e)}")
            st.write(traceback.format_exc())
        
        # 이메일 발송 섹션 (보고서 생성 후에만 표시)
        if 'reports_generated' in st.session_state and st.session_state['reports_generated']:
            st.header("5️⃣ 보고서 다운로드 및 이메일 발송")
            email_tab, download_tab = st.tabs(["이메일 발송", "보고서 다운로드"])
            
            with email_tab:
                if email_user and email_password:
                    # 이메일 내용 입력 UI
                    st.subheader("이메일 내용 설정")
                    email_subject = st.text_input(
                        "이메일 제목",
                        value="{creator_id} 크리에이터님의 음원 사용현황 보고서",
                        help="크리에이터 ID는 {creator_id}로 자동 치환됩니다."
                    )
                    
                    email_body = st.text_area(
                        "이메일 본문",
                        value="""안녕하세요! {creator_id} 크리에이터님

12월 초 예상 음원수익 전달드립니다 :)
12/1 - 12/15 사이의 예상 수익금이며,
해당 데이터는 유튜브 데이터 기반으로, 추정 수익이기 때문에 최종 정산값과는 차이가 있는 점 참고 바랍니다.
해당 수익은 25년 2월 말 정산 예정입니다.

궁금한점 있으시면 언제든지 연락주세요.
감사합니다.

루카스 드림""",
                        help="크리에이터 ID는 {creator_id}로 자동 치환됩니다.",
                        height=200
                    )
                    
                    # 이메일 발송 버튼
                    if st.button("크리에이터 이메일 발송", key="send_emails_tab"):
                        email_status = st.empty()
                        with st.spinner('이메일 발송 중...'):
                            try:
                                # (추가) CC 주소 파싱
                                cc_addresses = []
                                if cc_emails_input:
                                    cc_addresses = [cc.strip() for cc in cc_emails_input.split(',') if cc.strip()]

                                # (추가) BCC 주소 파싱
                                bcc_addresses = []
                                if bcc_emails_input:
                                    bcc_addresses = [bcc.strip() for bcc in bcc_emails_input.split(',') if bcc.strip()]

                                failed_creators = send_creator_emails(
                                    st.session_state['reports_data'],
                                    st.session_state['creator_info_handler'],
                                    email_user,
                                    email_password,
                                    email_subject,  # 사용자가 입력한 제목
                                    email_body,      # 사용자가 입력한 본문
                                    cc_addresses=cc_addresses,  # 여기서 전달
                                    bcc_addresses=bcc_addresses
                                )
                                if failed_creators:
                                    st.error(f"발송 실패한 크리에이터: {', '.join(failed_creators)}")
                                else:
                                    st.success("모든 크리에이터에게 이메일 발송이 완료되었습니다.")
                            except Exception as e:
                                st.error(f"이메일 발송 중 오류 발생: {str(e)}")
                else:
                    st.error("이메일 발송을 위해 Gmail 계정 정보가 필요합니다.")



            with download_tab:
                if all(k in st.session_state for k in ['reports_data', 'excel_files']):
                    zip_data = create_zip_file(
                        st.session_state['reports_data'],
                        st.session_state['excel_files'],
                        statistics_df,  # input_df 대신 statistics_df 사용
                        st.session_state['processed_df'],
                        creator_info_handler
                    )
                    st.download_button(
                        label="보고서 다운로드",
                        data=zip_data,
                        file_name="reports.zip",
                        mime="application/zip",
                        key="download_reports_tab"
                    )
                

if __name__ == "__main__":
    main()
