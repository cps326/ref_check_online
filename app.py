from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
from tqdm import tqdm
from openai import OpenAI
import os 
from collections import OrderedDict
import json
import streamlit as st
import io
import xlsxwriter
import chardet
import openai
import getpass
from dotenv import load_dotenv
from urllib.parse import urljoin
from urllib.parse import urlparse

# tqdm, chardet는 실제 사용이 거의 없고, openai와 OpenAI가 동시에 있어서 향후 확인 필요

import os
print(os.getcwd())
os.chdir("/home/yrjo/ref_online")
print(os.getcwd())

load_dotenv("/home/yrjo/.env")  #.env 파일 로드
if not os.environ.get("OPENAI_API_KEY"):
    os.environ["OPENAI_API_KEY"]=getpass.getpass("Enter your OpenAI API Key: ")
api_key = os.environ.get("OPENAI_API_KEY")
client = OpenAI(api_key = api_key)
#st.set_page_config(layout="wide")

st.set_page_config(layout="wide",page_title="KEI 참고문헌 온라인자료 검증도구")

# 중복 단어 제거를 위해 텍스트를 단어로 나누고, 처음 나온 단어만 남겨서 중복 제거(호출 필요가 있는지 향후 확인 필요)

def remove_duplicate_words(text):
    words = text.split()
    seen = OrderedDict()
    for word in words:
        if word not in seen:
            seen[word] = None
    return ' '.join(seen.keys())

    # 문자열 자르기, 텍스트를 최대 길이까지 자름

def truncate_string(text, max_length=10000):
    return text[:max_length]

# zrawling: URL에서 페이지 텍스트를 가져오는 핵심 함수 
# User-Agent는 브라우저인 척 해서 차단을 피하는 목적, doc-exts는 문서 확장자 모음

def crawling(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    doc_exts = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', 
                '.ppt', '.pptx', '.txt', '.csv', '.rtf']
    
    if any(ext in url for ext in doc_exts):
        try:
            # HEAD 요청으로 빠르게 확인
            response = requests.head(url, allow_redirects=True, timeout=5)

            if response.status_code == 200:
                return "파일다운가능"
            else:
                return "파일다운불가"

        except requests.exceptions.RequestException as e:
            return "파일다운불가"
            

    try:
    # 첫 번째 요청: 일반 웹페이지 html이면, get 요청으로 페이지 내용 받기, verify=false는 SSL 인증서 검증 끔(보안상 권장x)
        #print(url)
        response = requests.get(url, headers=headers, verify=False, timeout=30,allow_redirects=True)
        response_text = response.text
          # 특정 문구가 포함되어 있다면 확인 불가 처리
        if "You need to enable JavaScript to run this app" in response.text:
            soup2 = BeautifulSoup(response.text, 'html.parser')
            text = soup2.get_text(separator=' ', strip=True)
            # 텍스트 길이 기반 판별
            if len(text) < 200:
                return "확인불가"   
            
        # location.href를 추출
        match = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", response.text)
        if match:
            redirect_url = match.group(1)
            # JavaScript와 같은 스크립트로 추정되면 무시
            if "javascript:" in redirect_url.lower():
                print("JavaScript URL이 감지되어 무시합니다.")
            else:
                # 절대 경로로 변환 (필요시)
                if url.startswith("/") or url.startswith("."):
                    redirect_url = urljoin(url, redirect_url)
                
                    # 요청 전송
                    response2 = requests.get(redirect_url, headers=headers, verify=False, timeout=30, allow_redirects=True)
                    response_text = response_text + response2.text
                
        response.encoding = 'utf-8'
        
        if response.status_code == 200:
            print("IFrame")
            soup = BeautifulSoup(response_text, 'html.parser')
            # 인코딩 처리
            meta = soup.find('meta', attrs={'charset': True})
            if meta and meta['charset'] != 'utf-8':
                response.encoding = meta['charset']
                soup = BeautifulSoup(response.text, 'html.parser')
            # 메인 콘텐츠 추출
            content = soup.get_text(strip=True)
            # iframe 처리
            iframes = soup.find_all('iframe')
            iframe_contents = []
        
            for iframe in iframes:
                iframe_src = iframe.get('src')
                if iframe_src and iframe_src.strip():  # src가 None 또는 공백인지 확인
                    # 절대 URL로 변환
                    iframe_url = urljoin(url, iframe_src)
                    parsed = urlparse(iframe_url)
        
                    # 유효한 URL만 처리
                    if parsed.scheme not in ('http', 'https'):
                        continue
                    try:
                        iframe_response = requests.get(iframe_url, headers=headers, verify=False, timeout=30,allow_redirects=True)
                        if iframe_response.status_code == 200:
                            iframe_soup = BeautifulSoup(iframe_response.content, 'html.parser')
                            iframe_content = iframe_soup.get_text(strip=True)
                            iframe_contents.append(iframe_content)
                    except Exception as e:
                        print(f"iframe 요청 실패: {iframe_url}, 오류: {e}")
        
            # iframe 콘텐츠를 메인 콘텐츠에 추가
            if iframe_contents:
                content += "\n\n" + "\n\n".join(iframe_contents)

            return content
        else:
            return "O"  #성공 시 페이티 텍스트 반환하고, 실패/예외 시 "O" 반환 하는데 이건 추후에 확인
            
    except Exception as e:
        print(e)
        return "O"

        #GPT에게 “포함되면 X / 아니면 O” 로 매우 단순하게 판정하게 함
# 다만, 출력 규칙이 일반적인 직관과 반대임 (관련 내용이 있으면 X, 없으면 O)
# (보통은 “정상=O”로 쓰는 경우가 많아서 혼동 가능)

max_len = 50000
def GPTclass(x, y):
    y = crawling(y)
    if len(y) > max_len:
        y = y[0:max_len]
    if (y == "확인불가"):
        return "확인불가"
    if (y == "파일다운가능"):
        return "파일다운가능(내용확인불가)"
    if (y == "파일다운불가"):
        return "파일다운불가"
    if "확인필요" in x:
        return "O(형식오류)"
    
    retries = 0
    max_retries = 5
    while retries < max_retries:
        try:
            response = client.chat.completions.create(
                model = "gpt-4o",
                messages = [
                    {"role": "system", "content":"[[웹자료]]에서 내용이 주어진 [[정보]] 관련내용이 대략적으로 포함되어있으면 X, 관련내용이 아니거나, 빈페이지 또는 없는 페이지면 O 출력"},
                    {"role": "user",  "content": f"[[정보]]: {x}, [[웹자료]] : {y}"}
                ]
            )
            return response.choices[0].message.content
        except openai.RateLimitError as e:
            # Rate limit 오류가 발생했을 때
            print(f"Rate limit error: {e}. Retrying in {e.retry_after} seconds...")
            time.sleep(e.retry_after+2)
            retries += 1
        except Exception as e:
            # 다른 오류가 발생했을 때
            print(f"An error occurred: {e}")
            return None


def separator(entry):
    parts = [""] * 4
    
    if 'http' in entry:
        pattern_http = r',\s+(?=http)'
    else:
        pattern_http = r',\s+(?=검색일)'
    
    parts_http = re.split(pattern_http, entry)
    doc_info = parts_http[0]
    ref_info = parts_http[1] if len(parts_http) > 1 else ""
    
    # 새로운 방식: “ (U+201C) 이전을 저자, 이후를 제목으로 분리
    if '“' in doc_info and '”' in doc_info:
        match = re.match(r'(.+?),\s*?“(.*)”', doc_info)
        if match:
            parts[0] = match.group(1).strip()  # 저자
            parts[1] = f'“{match.group(2)}”'   # 제목 포함 인용부호
        else:
            parts[0] = doc_info.strip()
    else:
        parts[0] = doc_info.strip()
    
    # 나머지 ref_info 분리
    if 'http' in ref_info:
        pattern_ref = r',\s+(?=검색일)'
        parts_ref = re.split(pattern_ref, ref_info)
        parts[2] = parts_ref[0].strip()
        parts[3] = parts_ref[1].strip() if len(parts_ref) > 1 else ""
    else:
        parts[3] = ref_info.strip()

    return parts


def GPTcheck(doc):
    query = """
    당신은 각 줄마다 아래 형식에 맞는 문헌 정보가 정확히 입력되었는지 검토합니다. 각 문헌 정보는 다음의 4가지 요소로 구성되어 있어야 합니다:
    
    1. 출처 (예: 국가법령정보센터 또는 영문 저자 Meijer, J. et al.(2020) 등)
    2. 제목: 반드시 큰따옴표(" ")로 감쌈 (예: “물환경보전법 시행규칙”)
    3. URL
    4. 검색일: "검색일: yyyy.m.d." 형식
    
    **요구 사항**  
    - 각 항목은 쉼표(,)로 구분합니다. 단, 큰따옴표(" ") 안에 있는 쉼표는 필드 구분자로 간주하지 않습니다.  
    - 출처 항목에는 한 명 이상의 영문 저자가 올 수 있으며, 날짜 포함은 선택 사항입니다 (예: 국립생태원 보도자료(2017.5.26) 가능).  
    - 제목은 반드시 큰따옴표로 감싸야 하며, 누락 시 오류로 간주합니다.  
    - 보수적으로 판단하여 애매한 경우에는 오류가 아닌것으로 판단
    
    **출력 형식**  
    - 각 줄마다 `"오류여부"` 필드만 포함된 JSON으로 출력합니다.  
    - 형식이 맞으면 `"오류여부": "X"`  
    - 오류가 있으면 `"오류여부": "O(오류이유 간략히)"`
    
    **예시 입력**  
    국가법령정보센터, “물환경보전법 시행규칙”, http://www.law.go.kr/법령/물환경보전법시행규칙, 검색일: 2018.5.3.  
    국립생태원 보도자료(2017.5.26), “국립생태원, 2017년 생태공감마당 평창에서 개최”, p.8, https://www.me.go.kr/home/web/index.do?menuId=286, 검색일: 2018.7.25.  
    Dutch Ministry of Infrastructure and the Environment, http://rwsenvironment.eu/subjects/soil/publications/quality-control-and/, 검색일: 2018.5.3.  
    Meijer, J. et al.(2020), “S71 | CECSCREEN | HBM4EU CECscreen ... Zenodo.”, https://zenodo.org/records/3957497, 검색일: 2024.6.12.  
    BP4NTA, “NTA Study Reporting Tool(SRT)”, https://nontargetedanalysis.org/srt/, 검색일: 2024.5.29.
    """

    retries = 0
    max_retries = 5
    while retries < max_retries:
        try:
            response = client.chat.completions.create(
                model="gpt-5.2",
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": f"{query}"},
                    {"role": "user", "content": f"문서:{doc}"}
                ]
            )
            result = response.choices[0].message.content
            result_dict = json.loads(result)
            result_dict["원문"] = doc  # 문서 이름 추가
            return result_dict
        except openai.RateLimitError as e:
            # Rate limit 오류가 발생했을 때
            print(f"Rate limit error: {e}. Retrying in {e.retry_after} seconds...")
            time.sleep(e.retry_after+2)
            retries += 1
        except Exception as e:
            st.error(f"Error processing document: {str(e)}")
            return None


import re

def check_format(text):
    # 제목 추출
    title_match = re.search(r'"[^"]*"', text)   #쌍따옴표 ” " 수정 
    if not title_match:
        return False

    title_start = title_match.start()
    title_end = title_match.end()
    title = text[title_start:title_end].strip()

    # 제목 앞: 저자
    author = text[:title_start].strip().rstrip(',')
    if not author:
        return False

    # 제목 뒤: 나머지 항목 추출
    rest = text[title_end:].strip()

    # 쉼표 기준 나누되 큰따옴표 안 쉼표는 무시
    temp_parts = [p.strip() for p in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', rest)]

    # URL 병합 처리 (쉼표 포함 URL이 있을 경우)
    parts = []
    i = 0
    while i < len(temp_parts):
        part = temp_parts[i]
        if part.startswith("http"):
            while i + 1 < len(temp_parts) and not temp_parts[i+1].startswith("검색일") and not re.search(r'\d{4}', temp_parts[i+1]):
                part += ',' + temp_parts[i+1]
                i += 1
        parts.append(part)
        i += 1

    # 마지막 2개 항목이 있어야 하므로 확인
    if len(parts) < 2:
        return False

    # 최종 전체 항목은 저자, 제목, URL, 날짜 총 4개
    all_parts = [author, title] + parts[-2:]
    return len(all_parts) == 4
    

def process_entries(entries):
    articles = []
    for entry in entries:
        note = ""
        if not check_format(entry):
            note = "확인필요"

        check = separator(entry)
        check = ["확인필요" if item == 'NA' or item == '' else item for item in check]
        source = check[0]

        # 날짜 형식이 대강이라도 있으면, 날짜 형태가 맞지 않으면 확인필요
        if re.search(r"\d{2,4}\.\d+\.\d+", source):  # 날짜 형식이 대충이라도 있으면
            if not re.search(r"\b\d{4}\.([1-9]|1[0-2])\.([1-9]|[12][0-9]|3[01])\b", source):
                note = "확인필요"
            
        title = check[1]
        url = check[2]
        search_date = check[3].replace("검색일: ", "")
        search_date = search_date.strip()
        print(search_date)
        if not re.search(r"\b\d{4}\.([1-9]|1[0-2])\.([1-9]|[12][0-9]|3[01])\b", search_date):
            search_date = "확인필요"
            
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        try:
            response = requests.get(url, headers=headers, verify=False, timeout=30,allow_redirects=True)
            requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                url_status = "X"
            else:
                url_status = "O"
        except requests.RequestException:
            url_status = "X"
        articles.append({
            "source": source,
            "title": title,
            "URL": url,
            "search_date": search_date,
            "URL_오류여부": url_status,
            "형식체크_오류여부": note
        })
    return pd.DataFrame(articles)



def main():
    st.title("KEI 참고문헌 온라인자료 검증도구")

    # 세션 상태에 텍스트 데이터를 저장할 변수 초기화
    if 'text_data' not in st.session_state:
        st.session_state['text_data'] = ''
    if 'processed_data' not in st.session_state:
        st.session_state['processed_data'] = None

    uploaded_file = st.file_uploader("보고서 참고문헌 중 온라인자료에 해당하는 텍스트 파일(txt)를 업로드 하거나 ", type=["txt"])
    text_data = st.text_area('또는 아래에 온라인자료에 해당하는 텍스트를 입력하세요', st.session_state['text_data'], height=300)

    if st.button('검증실행'):
        progress_bar = st.progress(0)
        status_text = st.empty()

        if uploaded_file or text_data.strip():
            # 1단계: 5%
            progress_bar.progress(5)
            status_text.text("1단계: 입력 데이터 로딩 중...")

            if uploaded_file:
                data = uploaded_file.read().decode("utf-8")
            else:
                data = text_data

            entries = data.strip().splitlines()
            
            # entries = []
            # temp_entry = []
            # for line in raw_entries:
            #     if "검색일:" in line and temp_entry:
            #         entries.append(' '.join(temp_entry))
            #         temp_entry = [line]
            #     else:
            #         temp_entry.append(line)
            # if temp_entry:
            #     entries.append(' '.join(temp_entry))

            # 2단계: 5% → 15% 범위에서 점진적으로 증가
            progress_bar.progress(5)
            status_text.text("2단계: 기본 형식 및 URL 체크 중...")

            result_df = process_entries(entries)
            result_df['URL_오류여부'] = result_df['URL'].apply(lambda x: 'X' if x.startswith('http') else 'O')
            result_df['형식체크_오류여부'] = result_df.apply(lambda row: 'O' if '확인필요' in row.values else 'X', axis=1)

            
            # 3단계: 15% → 45% 범위에서 점진적 증가
            status_text.text("3단계: GPT 형식검증 수행 중...")
            GPT_check_list = []
            n3 = len(entries)
            for idx, doc in enumerate(entries):
                GPT_check_list.append(GPTcheck(doc))  # 사용자 정의 함수
                progress = 15 + int(30 * (idx + 1) / n3)
                progress_bar.progress(progress)
                status_text.text(f"3단계: GPT 형식검증 수행 중... ({idx + 1}/{n3})")
            GPT_check_df = pd.DataFrame(GPT_check_list)
            result_df['GPT_형식체크_오류여부'] = GPT_check_df['오류여부']
    

            # 4단계: 45% → 95% 점진적으로 증가
            status_text.text("4단계: GPT 기반 URL 내용 검증 중...")
            n4 = len(result_df)
            URL_check_results = []
            for i, (title_source, url) in enumerate(zip(result_df['title'] + " + " + result_df['source'], result_df['URL'])):
                URL_check_results.append(GPTclass(title_source, url))  # 사용자 정의 함수
                progress = 45 + int(50 * (i + 1) / n4)
                progress_bar.progress(progress)
                status_text.text(f"4단계: URL 확인 중... ({i + 1}/{n4})")
            result_df['GPT_URL_유효정보_오류여부'] = URL_check_results
            result_df['원문'] = GPT_check_df['원문']
            
            # 5단계: 95% → 100%
            progress_bar.progress(95)
            status_text.text("5단계: 결과 정리 및 테이블 구성 중...")

            st.dataframe(result_df)

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                result_df.to_excel(writer, index=False, sheet_name='Sheet1')
            output.seek(0)
            st.session_state.processed_data = output.read()

            progress_bar.progress(100)
            status_text.text("✅ 완료되었습니다! 결과를 확인하고 다운로드하세요.")
    if st.session_state.processed_data:
        st.download_button(
            label="엑셀로 다운로드",
            data=st.session_state.processed_data,
            file_name='result.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

if __name__ == "__main__":
    main()
