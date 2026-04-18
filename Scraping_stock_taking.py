import json
import time
import requests
import pandas as pd
import os
import ast
import numpy as np
import gspread
import traceback
from datetime import datetime, timezone, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# --- ACCOUNT CONFIGURATION ---
MY_USER = "chaulhb@nsq.vn"
MY_PASS = "chaulhb@123"

DYNAMIC_HEADERS = {}
tz_vn = timezone(timedelta(hours=7))

def get_headers_from_me_api(user, pwd):
    print("--- 🕵️ Step 1: Extracting dynamic headers from system ---")
    chrome_options = Options()
    # chrome_options.add_argument("--headless") 
    chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    wait = WebDriverWait(driver, 20)

    try:
        driver.get("https://ivt.ipos.vn/login")
        wait.until(EC.presence_of_element_located((By.XPATH, "//input[@type='text']"))).send_keys(user)
        driver.find_element(By.XPATH, "//input[@type='password']").send_keys(pwd)
        driver.find_element(By.CLASS_NAME, "ivt-login-form-button").click()
        
        print("Waiting for API authentication response (10 seconds)...")
        time.sleep(10) 

        logs = driver.get_log('performance')
        global DYNAMIC_HEADERS
        
        for entry in logs:
            log_obj = json.loads(entry['message'])['message']
            if log_obj['method'] == 'Network.requestWillBeSent':
                request = log_obj.get('params', {}).get('request', {})
                url = request.get('url', '')

                if '/auth/me' in url:
                    raw_headers = request.get('headers', {})
                    DYNAMIC_HEADERS = {k.lower(): v for k, v in raw_headers.items()}
                    DYNAMIC_HEADERS.update({
                        'content-type': 'application/json',
                        'accept': 'application/json, text/plain, */*',
                        'x-timezone': '7',
                        'language': 'vi'
                    })
                    DYNAMIC_HEADERS.pop('content-length', None)
                    if DYNAMIC_HEADERS.get('access-token'):
                        print(f"✅ Token extracted successfully!")
                        return True
        return False
    except Exception as e:
        print(f"⚠️ Selenium Error: {e}")
        return False
    finally:
        driver.quit()

def get_7_day_time_range():
    now = datetime.now(tz_vn)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    to_date_dt = today_start - timedelta(days=0,seconds=1)
    to_date_ts = int(to_date_dt.timestamp())
    from_date_dt = today_start - timedelta(days=7)
    from_date_ts = int(from_date_dt.timestamp())
    
    # Format strings for file naming (using - instead of / for Windows compatibility)
    from_str = from_date_dt.strftime('%d-%m-%Y')
    to_str = to_date_dt.strftime('%d-%m-%Y')
    
    print(f"--- Time Range: {from_date_dt.strftime('%d/%m/%Y')} - {to_date_dt.strftime('%d/%m/%Y')} ---")
    return from_date_ts, to_date_ts, from_str, to_str

def get_document_list(from_ts, to_ts):
    print(f"--- Step 2: Querying document list ---")
    url_list = (f'https://apiivt.ipos.vn/api/main/v1/service/stocktaking?'
                f'from_date={from_ts}&to_date={to_ts}&st_status=COMPLETED'
                f'&results_per_page=100&page=1&page_size=100')
    
    DYNAMIC_HEADERS['x-timestamp'] = str(int(time.time()))
    res = requests.get(url_list, headers=DYNAMIC_HEADERS)
    
    if res.status_code == 200:
        phieus = res.json().get('data', [])
        uids = [str(item.get('uid') or item.get('id')) for item in phieus]
        print(f" ✅ Found {len(uids)} documents.")
        return uids
    return []

def get_details_and_process(uids, from_str, to_str):
    if not uids:
        return None

    print(f"--- Step 3: Fetching details and exploding list_item ---")
    url_print = 'https://apiivt.ipos.vn/api/main/v1/service/stock-take/print'
    DYNAMIC_HEADERS['x-timestamp'] = str(int(time.time()))
    
    payload = {'list_uid': uids}
    res = requests.post(url_print, headers=DYNAMIC_HEADERS, json=payload)
    
    if res.status_code == 200:
        raw_data = res.json().get('data', [])
        if not raw_data:
            print("⚠️ No detailed data found.")
            return None

        df_raw = pd.DataFrame(raw_data)
        df_exploded = df_raw.explode('list_item').reset_index(drop=True)
        items_detail = pd.json_normalize(df_exploded['list_item'])
        df_final = pd.concat([df_exploded.drop(columns=['list_item']), items_detail], axis=1)

        # --- UPDATE FILE NAME AND PATH ---
        folder_path = r"D:\Làm việc - Bảo Châu\Project\Scraping_stock_taking\Data_kiem_kho"
        
        # Create folder if it doesn't exist
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            
        # Construct file name: Stocktake_Details_02-04-2026 - 08-04-2026.xlsx
        file_name = f"Stocktake_Details_{from_str} - {to_str}.xlsx"
        full_path = os.path.join(folder_path, file_name)
        
        df_final.to_excel(full_path, index=False)
        
        print(f"✅ COMPLETED: Data exported to: {full_path}")
        return df_final
    else:
        print(f"❌ API Error (Details): {res.text}")
        return None

# --- EXECUTION ---
df_result = None

def main():
    global df_result
    if get_headers_from_me_api(MY_USER, MY_PASS):
        # Now returns 4 values
        f_ts, t_ts, f_str, t_str = get_7_day_time_range() 
        list_uids = get_document_list(f_ts, t_ts)
        # Pass the date strings to the processing function
        df_result = get_details_and_process(list_uids, f_str, t_str)
        
        if df_result is not None:
            print("\n--- DATA PREVIEW ---")
            print(df_result.head())

if __name__ == "__main__":
    main()

Kiemkho = df_result.copy()
Kiemkho = Kiemkho.loc[:, ~Kiemkho.columns.duplicated()]

# 1. Convert Unix to Datetime
Kiemkho['st_date'] = pd.to_datetime(Kiemkho['st_date'], unit='s')
Kiemkho['tran_date'] = pd.to_datetime(Kiemkho['tran_date'], unit='s')
Kiemkho['created_at'] = pd.to_datetime(Kiemkho['created_at'], unit='s')
Kiemkho['updated_at'] = pd.to_datetime(Kiemkho['updated_at'], unit='s')

# 2. Handle Timezone (Localize to UTC then convert to VN time)
Kiemkho['st_date'] = Kiemkho['st_date'].dt.tz_localize('UTC').dt.tz_convert('Asia/Ho_Chi_Minh')
Kiemkho['tran_date'] = Kiemkho['tran_date'].dt.tz_localize('UTC').dt.tz_convert('Asia/Ho_Chi_Minh')
Kiemkho['created_at'] = Kiemkho['created_at'].dt.tz_localize('UTC').dt.tz_convert('Asia/Ho_Chi_Minh')
Kiemkho['updated_at'] = Kiemkho['updated_at'].dt.tz_localize('UTC').dt.tz_convert('Asia/Ho_Chi_Minh')

# 3. Format to String for display
Kiemkho['st_date'] = Kiemkho['st_date'].dt.strftime('%Y/%m/%d %H:%M:%S')
Kiemkho['tran_date'] = Kiemkho['tran_date'].dt.strftime('%Y/%m/%d %H:%M:%S')
Kiemkho['created_at'] = Kiemkho['created_at'].dt.strftime('%Y/%m/%d %H:%M:%S')
Kiemkho['updated_at'] = Kiemkho['updated_at'].dt.strftime('%Y/%m/%d %H:%M:%S')
Kiemkho = Kiemkho.sort_values(by='st_date', ascending=True)

# 0. Select required columns and rename
Kiemkho = Kiemkho[['warehouse_name','st_id', 'st_date','description','item_id','item_name','unit_id','unit_name', 'ivt_qty','wh_qty','gap_qty','gap_value']]
Kiemkho = Kiemkho.rename(columns={
    'warehouse_name': 'Chi nhánh',
    'st_id': 'Mã phiếu',
    'st_date': 'Thời gian kiểm kê',
    'description': 'Ghi chú',
    'item_id': 'Mã hàng',
    'item_name': 'Tên hàng',
    'unit_id': 'Mã đơn vị',
    'unit_name': 'Đơn vị',
    'ivt_qty': 'Số lượng tồn kho',
    'wh_qty': 'Số lượng kiểm kê',
    'gap_qty': 'Số lượng chênh lệch',
    'gap_value': 'Giá trị chênh lệch'
})

# 1. Calculate Unit Price
Kiemkho['Đơn giá'] = Kiemkho['Giá trị chênh lệch'].div(Kiemkho['Số lượng chênh lệch'])

# 2. Handle errors (division by zero or NaN)
Kiemkho['Đơn giá'] = Kiemkho['Đơn giá'].replace([np.inf, -np.inf], np.nan).fillna(0)

# 3. Rounding
Kiemkho['Đơn giá'] = Kiemkho['Đơn giá'].round(2)

# --- GOOGLE SHEETS INTEGRATION ---
path_to_json = r"D:\Làm việc - Bảo Châu\Project\service_account\cosmic-backbone-457909-d5-f38574c3b6d2.json"
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

try:
    if not os.path.exists(path_to_json):
        print(f"❌ JSON file not found at: {path_to_json}")
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_name(path_to_json, scope)
        gc = gspread.authorize(creds)
        print("✅ Authentication successful!")

        DUNG_SAI_URL = "https://docs.google.com/spreadsheets/d/1UXrGvU0IrWaxkhm9ZCFRMfVsT8SnL7UH2yr8mbq-ODQ/edit#gid=852931082"
        sh = gc.open_by_url(DUNG_SAI_URL)

        all_sheets = sh.worksheets()
        print(f"📂 File contains {len(all_sheets)} sheets. Reading indices 2 and 3...")

        df_dungsai_chung = get_as_dataframe(sh.get_worksheet(2), evaluate_formulas=True).dropna(how='all').dropna(axis=1, how='all')
        df_dungsai_cvgtn = get_as_dataframe(sh.get_worksheet(3), evaluate_formulas=True).dropna(how='all').dropna(axis=1, how='all')

        print("✅ Tolerance data loaded successfully!")
        print(f"Rows Sheet 2: {len(df_dungsai_chung)} | Rows Sheet 3: {len(df_dungsai_cvgtn)}")

except Exception:
    print("❌ DETAILED ERROR LOG:")
    print(traceback.format_exc())

# 2. Split Kiemkho into 2 parts: CVGTN and others
is_cvgtn = Kiemkho['Chi nhánh'].str.contains('Chạng Vạng Trần Não', na=False)
Kiemkho_cvgtn = Kiemkho[is_cvgtn].copy()
Kiemkho_chung = Kiemkho[~is_cvgtn].copy()

# 3. Merge with corresponding tolerance tables
Kiemkho_cvgtn = pd.merge(Kiemkho_cvgtn, 
                         df_dungsai_cvgtn, 
                         left_on=['Đơn vị'], 
                         right_on=['ĐVT sử dụng'], 
                         how='left')

Kiemkho_chung = pd.merge(Kiemkho_chung, 
                         df_dungsai_chung, 
                         left_on=['Đơn vị'], 
                         right_on=['ĐVT sử dụng'], 
                         how='left')

# 4. Concatenate back and clean up
Kiemkho = pd.concat([Kiemkho_cvgtn, Kiemkho_chung], ignore_index=True).drop(columns=['ĐVT sử dụng'])
Kiemkho['Dung sai cho phép'] = Kiemkho['Dung sai cho phép'].fillna(0)

# 5. Logic to calculate quantity exceeding tolerance
def tinh_vuot_dung_sai_excel_logic(row):
    k2 = row.get('Số lượng chênh lệch', 0)
    r2 = row.get('Dung sai cho phép', 0)
    
    try:
        k2 = float(k2)
        r2 = float(r2)
    except:
        return 0

    if k2 <= 0:
        if k2 + r2 >= 0:
            return 0  # No fine
        else:
            return abs(k2 + r2) # Absolute remaining missing quantity
    else:
        if k2 <= r2:
            return 0  # No fine
        else:
            return k2 - r2 # Excess quantity beyond tolerance

# Apply function
Kiemkho['SL vượt quá dung sai'] = Kiemkho.apply(tinh_vuot_dung_sai_excel_logic, axis=1)
Kiemkho['Giá trị phạt'] = Kiemkho['SL vượt quá dung sai'] * Kiemkho['Đơn giá']
Kiemkho = Kiemkho.sort_values(by=['Thời gian kiểm kê','Mã phiếu'], ascending=[True,True])

# --- FINAL STEP: UPLOAD DATA ---
try:
    target_url = "https://docs.google.com/spreadsheets/d/1UXrGvU0IrWaxkhm9ZCFRMfVsT8SnL7UH2yr8mbq-ODQ/edit#gid=460256009"
    spreadsheet = gc.open_by_url(target_url)
    worksheet = spreadsheet.get_worksheet(0)

    print("⏳ Clearing old data...")
    worksheet.clear()

    print("⏳ Uploading new data...")
    set_with_dataframe(
        worksheet,
        Kiemkho,
        include_index=False,
        include_column_header=True,
        row=1
    )

    print("✅ SUCCESS: All new data has been overwritten successfully!")

except Exception as e:
    print("❌ Error during data upload:")
    print(traceback.format_exc())