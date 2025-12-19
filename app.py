from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
from openai import OpenAI
import os
from collections import OrderedDict
import json
import streamlit as st
import io
import xlsxwriter
import openai
import time
from urllib.parse import urljoin
from urllib.parse import urlparse

# =========================
# OpenAI API Key (Cloud 중심)
# =========================
api_key = st.secrets.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
if not api_key:
    st.error("OPENAI_API_KEY가 설정되지 않았습니다. Streamlit Cloud > Secrets에 추가해주세요.")
    st.stop()

client = OpenAI(api_key=api_key)
openai.api_key = api_key

st.set_page_config(layout="wide", page_title="KEI 참고문헌 온라인자료 검증도구")


# =========================
# (선택) 텍스트 유틸
# =========================
def remove_duplicate_words(text):
    words = text.split()
    seen = OrderedDict()
    for word in words:
        if word not in seen:
            seen[word] = None
    return ' '.join(seen.keys())


def truncate_string(text, max_length=10000):
    return text[:max_length]


# =========================
# URL 상태 체크 (정상/오류/확인불가/정상(보안주의) + 메모)
# =========================
def check_url_status(url: str, timeout: int = 15) -> dict:
    if not isinstance(url, str) or not url.strip():
        return {"URL_상태": "오류", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": "URL 없음"}

    url = url.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        return {"URL_상태": "오류", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": "http/https로 시작하지 않음"}

    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        # 기본: SSL 검증 ON
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        status_code = r.status_code
        final_url = r.url

        if 200 <= status_code < 300:
            return {"URL_상태": "정상", "URL_상태코드": status_code, "URL_최종URL": final_url, "URL_메모": ""}
        else:
            return {"URL_상태": "오류", "URL_상태코드": status_code, "URL_최종URL": final_url, "URL_메모": f"HTTP {status_code}"}

    except requests.exceptions.SSLError:
        # SSL 검증 실패지만 실제 접속은 되는지 verify=False로 1회 재시도
        try:
            r2 = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, verify=False)
            status_code = r2.status_code
            final_url = r2.url

            if 200 <= status_code < 300:
                memo = "SSL 검증 실패(보안주의): verify=False로는 접속됨"
                return {"URL_상태": "정상(보안주의)", "URL_상태코드": status_code, "URL_최종URL": final_url, "URL_메모": memo}
            else:
                memo = f"SSL 검증 실패 + HTTP {status_code}(verify=False)"
                return {"URL_상태": "오류", "URL_상태코드": status_code, "URL_최종URL": final_url, "URL_메모": memo}

        except Exception as e2:
            msg = f"{type(e2).__name__}: {str(e2)[:120]}"
            return {"URL_상태": "확인불가", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": f"SSL 핸드셰이크 실패(verify=False도 실패) - {msg}"}

    except requests.exceptions.Timeout:
        return {"URL_상태": "확인불가", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": "Timeout"}
    except requests.exceptions.ConnectionError:
        return {"URL_상태": "확인불가", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": "Connection error"}
    except requests.exceptions.InvalidURL:
        return {"URL_상태": "오류", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": "Invalid URL"}
    except requests.exceptions.MissingSchema:
        return {"URL_상태": "오류", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": "URL 스키마 누락(http/https)"}
    except Exception as e:
        return {"URL_상태": "확인불가", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": f"예외: {type(e).__name__}"}


# =========================
# crawling: URL에서 페이지 텍스트 가져오기
# =========================
def crawling(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    doc_exts = ['.pdf', '.doc', '.docx', '.xls', '.xlsx',
                '.ppt', '.pptx', '.txt', '.csv', '.rtf']

    if any(ext in url for ext in doc_exts):
        try:
            response = requests.head(url, allow_redirects=True, timeout=5)
            if response.status_code == 200:
                return "파일다운가능"
            else:
                return "파일다운불가"
        except requests.exceptions.RequestException:
            return "파일다운불가"

    try:
        response = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        response_text = response.text

        if "You need to enable JavaScript to run this app" in response.text:
            soup2 = BeautifulSoup(response.text, 'html.parser')
            text = soup2.get_text(separator=' ', strip=True)
            if len(text) < 200:
                return "확인불가"

        match = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", response.text)
        if match:
            redirect_url = match.group(1)
            if "javascript:" not in redirect_url.lower():
                redirect_url = urljoin(url, redirect_url)
                response2 = requests.get(redirect_url, headers=headers, timeout=30, allow_redirects=True)
                response_text = response_text + response2.text

        response.encoding = 'utf-8'

        if response.status_code == 200:
            soup = BeautifulSoup(response_text, 'html.parser')

            meta = soup.find('meta', attrs={'charset': True})
            if meta and meta.get('charset') and meta['charset'].lower() != 'utf-8':
                response.encoding = meta['charset']
                soup = BeautifulSoup(response.text, 'html.parser')

            content = soup.get_text(strip=True)

            iframes = soup.find_all('iframe')
            iframe_contents = []

            for iframe in iframes:
                iframe_src = iframe.get('src')
                if iframe_src and iframe_src.strip():
                    iframe_url = urljoin(url, iframe_src)
                    parsed = urlparse(iframe_url)

                    if parsed.scheme not in ('http', 'https'):
                        continue
                    try:
                        iframe_response = requests.get(iframe_url, headers=headers, timeout=30, allow_redirects=True)
                        if iframe_response.status_code == 200:
                            iframe_soup = BeautifulSoup(iframe_response.content, 'html.parser')
                            iframe_content = iframe_soup.get_text(strip=True)
                            iframe_contents.append(iframe_content)
                    except Exception:
                        pass

            if iframe_contents:
                content += "\n\n" + "\n\n".join(iframe_contents)

            return content
        else:
            return "확인불가"

    except Exception:
        return "확인불가"


# =========================
# GPT 기반 URL 내용 판별
# =========================
max_len = 50000

def GPTclass(x, y):
    y = crawling(y)
    if isinstance(y, str) and len(y) > max_len:
        y = y[0:max_len]

    if y == "확인불가":
        return "확인불가"
    if y == "파일다운가능":
        return "파일다운가능(내용확인불가)"
    if y == "파일다운불가":
        return "파일다운불가"
    if "확인필요" in x:
        return "O(형식오류)"

    retries = 0
    max_retries = 5
    while retries < max_retries:
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "[[웹자료]]에서 내용이 주어진 [[정보]] 관련내용이 대략적으로 포함되어있으면 X, 관련내용이 아니거나, 빈페이지 또는 없는 페이지면 O 출력"},
                    {"role": "user", "content": f"[[정보]]: {x}, [[웹자료]] : {y}"}
                ]
            )
            return response.choices[0].message.content
        except openai.RateLimitError as e:
            time.sleep(getattr(e, "retry_after", 2) + 2)
            retries += 1
        except Exception:
            return "확인불가"


# =========================
# (추가) GPT URL 결과를 사람이 읽기 쉽게 변환 + 컬럼명 변경에 사용
# =========================
def map_gpt_url_result(v):
    if v is None:
        return "확인불가"
    if not isinstance(v, str):
        return "확인불가"

    s = v.strip()

    if s == "확인불가":
        return "확인불가"
    if "파일다운가능" in s:
        return "파일(내용확인불가)"
    if "파일다운불가" in s:
        return "확인불가"

    if s == "X" or s.startswith("X"):
        return "일치(유효)"
    if s == "O" or s.startswith("O"):
        return "불일치(오류)"

    return s


# =========================
# 참고문헌 분리
# =========================
def separator(entry):
    parts = [""] * 4

    if 'http' in entry:
        pattern_http = r',\s+(?=http)'
    else:
        pattern_http = r',\s+(?=검색일)'

    parts_http = re.split(pattern_http, entry)
    doc_info = parts_http[0]
    ref_info = parts_http[1] if len(parts_http) > 1 else ""

    if '“' in doc_info and '”' in doc_info:
        match = re.match(r'(.+?),\s*?“(.*)”', doc_info)
        if match:
            parts[0] = match.group(1).strip()
            parts[1] = f'“{match.group(2)}”'
        else:
            parts[0] = doc_info.strip()
    else:
        parts[0] = doc_info.strip()

    if 'http' in ref_info:
        pattern_ref = r',\s+(?=검색일)'
        parts_ref = re.split(pattern_ref, ref_info)
        parts[2] = parts_ref[0].strip()
        parts[3] = parts_ref[1].strip() if len(parts_ref) > 1 else ""
    else:
        parts[3] = ref_info.strip()

    return parts


# =========================
# GPT 형식 검증 (항상 dict 반환)
# =========================
def GPTcheck(doc):
    query = """
    당신은 각 줄마다 아래 형식에 맞는 문헌 정보가 정확히 입력되었는지 검토합니다. 각 문헌 정보는 다음의 4가지 요소로 구성되어 있어야 합니다:
    1. 출처
    2. 제목: 반드시 큰따옴표(" ")로 감쌈
    3. URL
    4. 검색일: "검색일: yyyy.m.d." 형식
    출력: JSON {"오류여부":"X"} 또는 {"오류여부":"O(이유)"}
    """

    retries = 0
    max_retries = 5

    while retries < max_retries:
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": f"{query}"},
                    {"role": "user", "content": f"문서:{doc}"}
                ]
            )
            raw = response.choices[0].message.content
            result_dict = json.loads(raw)

            err = result_dict.get("오류여부")
            if not err:
                err = "O(오류여부 누락)"

            return {"오류여부": err, "원문": doc}

        except openai.RateLimitError as e:
            time.sleep(getattr(e, "retry_after", 2) + 2)
            retries += 1
        except Exception as e:
            return {"오류여부": f"O(GPTcheck 실패:{type(e).__name__})", "원문": doc}


# =========================
# 규칙 기반 형식 체크
# =========================
def check_format(text):
    title_match = re.search(r'"[^"]*"', text)
    if not title_match:
        return False

    title_start = title_match.start()
    title_end = title_match.end()
    title = text[title_start:title_end].strip()

    author = text[:title_start].strip().rstrip(',')
    if not author:
        return False

    rest = text[title_end:].strip()

    temp_parts = [p.strip() for p in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', rest)]

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

    if len(parts) < 2:
        return False

    all_parts = [author, title] + parts[-2:]
    return len(all_parts) == 4


# =========================
# entries -> DataFrame
# =========================
def process_entries(entries):
    articles = []
    for entry in entries:
        note = ""
        if not check_format(entry):
            note = "확인필요"

        check = separator(entry)
        check = ["확인필요" if item == 'NA' or item == '' else item for item in check]
        source = check[0]

        if re.search(r"\d{2,4}\.\d+\.\d+", source):
            if not re.search(r"\b\d{4}\.([1-9]|1[0-2])\.([1-9]|[12][0-9]|3[01])\b", source):
                note = "확인필요"

        title = check[1]
        url = check[2]

        search_date = check[3].replace("검색일: ", "")
        search_date = search_date.strip()
        if not re.search(r"\b\d{4}\.([1-9]|1[0-2])\.([1-9]|[12][0-9]|3[01])\b", search_date):
            search_date = "확인필요"

        url_result = check_url_status(url)

        articles.append({
            "URL_상태": url_result["URL_상태"],
            "URL_메모": url_result["URL_메모"],
            "URL_상태코드": url_result["URL_상태코드"],
            "URL_최종URL": url_result["URL_최종URL"],

            "source": source,
            "title": title,
            "URL": url,
            "search_date": search_date,
            "형식체크_오류여부": note
        })

    df = pd.DataFrame(articles)

    preferred_order = [
        "URL_상태", "URL_메모", "URL_상태코드", "URL_최종URL",
        "source", "title", "URL", "search_date", "형식체크_오류여부"
    ]
    cols = [c for c in preferred_order if c in df.columns] + [c for c in df.columns if c not in preferred_order]
    return df[cols]


# =========================
# Streamlit UI
# =========================
def main():
    st.title("KEI 참고문헌 온라인자료 검증도구")

    # 세션 상태 초기화
    if "text_data" not in st.session_state:
        st.session_state["text_data"] = ""
    if "processed_data" not in st.session_state:
        st.session_state["processed_data"] = None
    if "result_df" not in st.session_state:
        st.session_state["result_df"] = None  # ✅ 결과 DF 저장 (수동 입력 유지)

    uploaded_file = st.file_uploader(
        "보고서 참고문헌 중 온라인자료에 해당하는 텍스트 파일(txt)를 업로드 하거나 ",
        type=["txt"]
    )
    text_data = st.text_area(
        "또는 아래에 온라인자료에 해당하는 텍스트를 입력하세요",
        st.session_state["text_data"],
        height=300
    )

    col_run, col_reset = st.columns([1, 1])
    with col_run:
        run_clicked = st.button("👉여기를 눌러, 검증을 실행해 주세요.")
    with col_reset:
        reset_clicked = st.button("🔃(검증 후)수동 입력/결과 초기화 버튼")

    if reset_clicked:
        st.session_state["processed_data"] = None
        st.session_state["result_df"] = None
        st.success("초기화 완료! 다시 실행하세요.")
        st.stop()

    # =========================
    # 검증 실행
    # =========================
    if run_clicked:
        progress_bar = st.progress(0)
        status_text = st.empty()

        if not (uploaded_file or text_data.strip()):
            st.warning("텍스트 파일 업로드 또는 텍스트 입력이 필요합니다.")
            st.stop()

        progress_bar.progress(5)
        status_text.text("1단계: 입력 데이터 로딩 중...")

        if uploaded_file:
            data = uploaded_file.read().decode("utf-8")
        else:
            data = text_data

        entries = data.strip().splitlines()

        progress_bar.progress(10)
        status_text.text("2단계: 기본 형식 및 URL 체크 중...")

        result_df = process_entries(entries)

        status_text.text("3단계: GPT 형식검증 수행 중...")
        GPT_check_list = []
        n3 = len(entries)

        for idx, doc in enumerate(entries):
            GPT_check_list.append(GPTcheck(doc))
            progress = 15 + int(30 * (idx + 1) / max(n3, 1))
            progress_bar.progress(progress)
            status_text.text(f"3단계: GPT 형식검증 수행 중... ({idx + 1}/{n3})")

        gpt_errors = []
        gpt_originals = []
        for r, doc in zip(GPT_check_list, entries):
            if isinstance(r, dict):
                gpt_errors.append(r.get("오류여부", "O(오류여부 없음)"))
                gpt_originals.append(r.get("원문", doc))
            else:
                gpt_errors.append("O(GPTcheck None)")
                gpt_originals.append(doc)

        result_df["GPT_형식체크_오류여부"] = gpt_errors
        result_df["원문"] = gpt_originals

        status_text.text("4단계: GPT 기반 URL 내용 검증 중...")
        n4 = len(result_df)
        URL_check_results = []

        for i, (title_source, url) in enumerate(zip(result_df["title"] + " + " + result_df["source"], result_df["URL"])):
            URL_check_results.append(GPTclass(title_source, url))
            progress = 45 + int(50 * (i + 1) / max(n4, 1))
            progress_bar.progress(progress)
            status_text.text(f"4단계: URL 확인 중... ({i + 1}/{n4})")

        # ✅ 컬럼명 변경 + X/O -> 한글 라벨 변환
        result_df["URL_내용일치여부(GPT)"] = [map_gpt_url_result(x) for x in URL_check_results]

        # ===== 수동/최종 컬럼 준비 =====
        result_df["수동_URL_상태"] = ""
        result_df["수동_메모"] = ""
        result_df["최종_URL_상태"] = result_df["URL_상태"]
        result_df["최종_URL_메모"] = result_df["URL_메모"]

        # 최종 컬럼을 앞쪽으로
        front_cols = ["최종_URL_상태", "최종_URL_메모", "URL_상태", "URL_메모", "URL_상태코드", "URL_최종URL"]
        front_cols = [c for c in front_cols if c in result_df.columns]
        result_df = result_df[front_cols + [c for c in result_df.columns if c not in front_cols]]

        progress_bar.progress(95)
        status_text.text("5단계: 결과 정리 및 수동 확인 입력 준비 중...")

        # ✅ 세션에 저장 (리런에도 수동 입력 유지)
        st.session_state["result_df"] = result_df

        progress_bar.progress(100)
        status_text.text("✅ 완료되었습니다! 아래에서 수동 확인 후 다운로드하세요.")

    # =========================
    # 결과 표시(세션에 저장된 DF 기반)
    # =========================
    if st.session_state["result_df"] is not None:
        result_df = st.session_state["result_df"]

        # ✅ (요청) Expander 헤더 배경색 스타일 적용 (버튼처럼 강조)
        st.markdown("""
        <style>
        div[data-testid="stExpander"] details summary {
            background: #ffb2d9;       /* 연한 핑크 */
            border: 1px solid #ff997f; /* 진한 테두리 */
            border-radius: 10px;
            padding: 10px 12px;
            font-weight: 700;
        }
        div[data-testid="stExpander"] details summary svg {
            margin-right: 8px;
        }
        </style>
        """, unsafe_allow_html=True)

        # ===== 수동 확인 UI (오류/확인불가만) =====
        with st.expander("🔎 담당자의 수동 확인(오류/확인불가)이 필요합니다. 여기를 눌러주세요! 아래 표가 활성화되면, URL(클릭)에 접속하여 최종 판정 결과를 입력해주세요.🤗", expanded=False):
            issue_mask = result_df["URL_상태"].isin(["오류", "확인불가"])
            issues_df = result_df.loc[issue_mask, [
                "URL_상태", "URL_메모", "URL", "source", "title", "수동_URL_상태", "수동_메모"
            ]].copy()

            if len(issues_df) == 0:
                st.info("수동 확인이 필요한(오류/확인불가) 항목이 없습니다.")
            else:
                edited = st.data_editor(
                    issues_df,
                    use_container_width=True,
                    hide_index=False,
                    column_config={
                        "URL": st.column_config.LinkColumn("URL(클릭)", display_text="열기"),
                        "수동_URL_상태": st.column_config.SelectboxColumn(
                            "수동_URL_상태(선택)",
                            options=["", "정상", "정상(보안주의)", "오류", "확인불가"],
                            help="브라우저에서 확인한 결과를 선택하세요."
                        ),
                        "수동_메모": st.column_config.TextColumn(
                            "수동_메모",
                            help="수동 확인 근거/사유를 간단히 적어두세요."
                        ),
                    },
                    disabled=["URL_상태", "URL_메모", "source", "title"],
                    key="manual_editor",
                )

                if st.button("✅ 수동 판정 적용"):
                    result_df.loc[edited.index, "수동_URL_상태"] = edited["수동_URL_상태"]
                    result_df.loc[edited.index, "수동_메모"] = edited["수동_메모"]

                    has_manual = result_df["수동_URL_상태"].astype(str).str.strip().ne("")
                    result_df.loc[has_manual, "최종_URL_상태"] = result_df.loc[has_manual, "수동_URL_상태"]

                    has_manual_memo = result_df["수동_메모"].astype(str).str.strip().ne("")
                    result_df.loc[has_manual_memo, "최종_URL_메모"] = result_df.loc[has_manual_memo, "수동_메모"]

                    st.session_state["result_df"] = result_df
                    st.success("수동 판정을 최종 값에 반영했습니다. 아래 표/엑셀에 적용됩니다.")

        # ✅ 화면에서 최종_URL_상태 색칠(기존 유지)
        def highlight_url_status(val):
            if val == "오류":
                return "background-color: #f8d7da"  # 연한 빨강
            if val == "확인불가":
                return "background-color: #fff3cd"  # 연한 노랑
            if val == "정상(보안주의)":
                return "background-color: #ffe5b4"  # 연한 주황
            return ""

        styled = result_df.style.applymap(highlight_url_status, subset=["최종_URL_상태"])
        st.dataframe(styled, use_container_width=True)

        # ✅ 엑셀 저장 + 조건부서식(최종_URL_상태 기준)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            result_df.to_excel(writer, index=False, sheet_name='Sheet1')
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']

            if "최종_URL_상태" in result_df.columns:
                status_col = result_df.columns.get_loc("최종_URL_상태")

                fmt_red = workbook.add_format({'bg_color': '#F8D7DA'})
                fmt_yel = workbook.add_format({'bg_color': '#FFF3CD'})
                fmt_org = workbook.add_format({'bg_color': '#FFE5B4'})

                start_row = 1
                end_row = len(result_df)

                worksheet.conditional_format(start_row, status_col, end_row, status_col, {
                    'type': 'text',
                    'criteria': 'containing',
                    'value': '오류',
                    'format': fmt_red
                })
                worksheet.conditional_format(start_row, status_col, end_row, status_col, {
                    'type': 'text',
                    'criteria': 'containing',
                    'value': '확인불가',
                    'format': fmt_yel
                })
                worksheet.conditional_format(start_row, status_col, end_row, status_col, {
                    'type': 'text',
                    'criteria': 'containing',
                    'value': '정상(보안주의)',
                    'format': fmt_org
                })

        output.seek(0)
        st.session_state["processed_data"] = output.read()

        if st.session_state["processed_data"]:
            st.download_button(
                label="엑셀로 다운로드",
                data=st.session_state["processed_data"],
                file_name="result.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )


if __name__ == "__main__":
    main()
