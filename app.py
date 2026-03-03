import streamlit as st
import pandas as pd
import requests
import math
import os
import time
import gzip
import shutil
from datetime import datetime, timedelta, timezone
import concurrent.futures

# IST Offset
IST_OFFSET = timedelta(hours=5, minutes=30)
IST = timezone(IST_OFFSET)

def get_ist_now():
    return datetime.now(IST)

# Set page configuration
st.set_page_config(page_title="Positional Stock Option Scanner", layout="wide")

# Custom CSS for compact layout and button-like tabs
st.markdown("""
    <style>
        .block-container {
            padding-top: 1rem !important;
            padding-bottom: 1rem !important;
        }
        h1 {
            font-size: 1.8rem !important;
            margin-bottom: 0rem !important;
            white-space: nowrap !important;
        }
        h2 {
            font-size: 1.1rem !important;
            padding-top: 0.2rem !important;
            margin-bottom: 0.1rem !important;
        }
        h3 {
            font-size: 1.0rem !important;
            padding-top: 0.1rem !important;
            margin-bottom: 0.1rem !important;
        }
        
        /* Tab Styling */
        .stTabs [data-baseweb="tab-list"] {
            gap: 10px;
        }
        .stTabs [data-baseweb="tab"] {
            height: 45px;
            white-space: pre-wrap;
            background-color: #f0f2f6;
            border-radius: 5px;
            padding: 10px 20px;
            font-size: 1.1rem;
            font-weight: 600;
            border: 1px solid #d6d6d6;
        }
        .stTabs [aria-selected="true"] {
            background-color: #007bff;
            color: white !important;
            border-color: #007bff;
        }
        
        /* Prevent graying out during refresh */
        .stApp {
            transition: none !important;
        }
        [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
            opacity: 1 !important;
            transition: none !important;
        }
        
        /* Hide File Uploader Instructions */
        [data-testid="stFileUploaderDropzone"] div div span {
           display: none !important;
        }
        [data-testid="stFileUploaderDropzone"] div div small {
           display: none !important;
        }
        
        /* Force Dataframe Font Weight */
        div[data-testid="stDataFrame"] {
            font-weight: 600 !important;
        }
    </style>
""", unsafe_allow_html=True)

import json
import re

# Paths for persistent storage
DATA_DIR = 'data'
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

BLACKLIST_FILE = os.path.join(DATA_DIR, 'blacklist.json')
TOKEN_FILE = os.path.join(DATA_DIR, 'token.json')
META_FILE = os.path.join(DATA_DIR, 'meta.json')
LTP_CACHE_FILE = os.path.join(DATA_DIR, 'ltp_cache.json')

FILES = {
    'Monthly': os.path.join(DATA_DIR, 'monthly.csv'),
    'Weekly': os.path.join(DATA_DIR, 'weekly.csv'),
    'Intraday': os.path.join(DATA_DIR, 'intraday.csv')
}

def load_meta():
    if os.path.exists(META_FILE):
        try:
            with open(META_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {}

def save_meta(key, date_str):
    try:
        meta = load_meta()
        meta[key] = date_str
        with open(META_FILE, 'w') as f:
            json.dump(meta, f)
    except:
        pass

def load_ltp_cache():
    if os.path.exists(LTP_CACHE_FILE):
        try:
            with open(LTP_CACHE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {}

def save_ltp_cache(new_data):
    try:
        cache = load_ltp_cache()
        cache.update(new_data)
        with open(LTP_CACHE_FILE, 'w') as f:
            json.dump(cache, f)
    except:
        pass

def extract_date_from_filename(filename):
    # Regex to find 8-digit date like 20260130
    match = re.search(r'(\d{8})', filename)
    if match:
        d = match.group(1)
        # Format as YYYY-MM-DD
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return None

def load_dhan_creds():
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, 'r') as f:
                data = json.load(f)
                return data.get('access_token', ''), data.get('client_id', '')
        except:
            pass
    return '', ''

def save_dhan_creds(access_token, client_id):
    try:
        data = {
            'date': get_ist_now().strftime('%Y-%m-%d'),
            'access_token': access_token,
            'client_id': client_id
        }
        with open(TOKEN_FILE, 'w') as f:
            json.dump(data, f)
    except:
        pass

def load_blacklist():
    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, 'r') as f:
                data = json.load(f)
                if data.get('date') == get_ist_now().strftime('%Y-%m-%d'):
                    return set(data.get('keys', []))
        except:
            pass
    return set()

def save_blacklist(keys):
    try:
        data = {
            'date': get_ist_now().strftime('%Y-%m-%d'),
            'keys': list(keys)
        }
        with open(BLACKLIST_FILE, 'w') as f:
            json.dump(data, f)
    except:
        pass

# Constant for Dhan Scrip Master
DHAN_MASTER_PATH = 'api-scrip-master.csv'

@st.cache_data
def load_dhan_master():
    if os.path.exists(DHAN_MASTER_PATH):
        try:
            # Read CSV - specific columns to save memory
            use_cols = [
                'SEM_EXM_EXCH_ID', 'SEM_SEGMENT', 'SEM_SMST_SECURITY_ID', 
                'SEM_TRADING_SYMBOL', 'SEM_EXPIRY_DATE', 'SEM_STRIKE_PRICE', 
                'SEM_OPTION_TYPE'
            ]
            df = pd.read_csv(DHAN_MASTER_PATH, usecols=use_cols)
            
            # Filter for NSE Derivatives (Futures & Options)
            # We specifically need Options for the ATM mapping
            df = df[
                (df['SEM_EXM_EXCH_ID'] == 'NSE') & 
                (df['SEM_SEGMENT'] == 'D') &
                (df['SEM_OPTION_TYPE'].isin(['CE', 'PE']))
            ].copy()
            
            # Extract Underlying Symbol from Trading Symbol
            # Format: SYMBOL-MonYear-Strike-Type (e.g. RELIANCE-Feb2026-1200-CE)
            # Regex captures everything before the first hyphen followed by MonthYear
            df['underlying_symbol'] = df['SEM_TRADING_SYMBOL'].str.extract(r'^(.*?)-[A-Z][a-z]{2}\d{4}-')
            
            # Convert Expiry
            df['expiry_dt'] = pd.to_datetime(df['SEM_EXPIRY_DATE']).dt.normalize()
            
            # Rename columns to match expected format
            df = df.rename(columns={
                'SEM_SMST_SECURITY_ID': 'instrument_key',
                'SEM_STRIKE_PRICE': 'strike_price',
                'SEM_OPTION_TYPE': 'instrument_type'
            })
            
            # Convert instrument_key to string as it might be int
            df['instrument_key'] = df['instrument_key'].astype(str)
            
            return df[['underlying_symbol', 'strike_price', 'instrument_type', 'expiry_dt', 'instrument_key']]
            
        except Exception as e:
            st.error(f"Error loading Dhan master: {e}")
            return pd.DataFrame()
    else:
        st.error(f"Dhan scrip master not found at {DHAN_MASTER_PATH}")
        return pd.DataFrame()

def process_bhavcopy(bhav_file, df_json, target_expiry_index=0):
    try:
        df_bhav = pd.read_csv(bhav_file)
        
        # Check required columns
        required_cols = ['FinInstrmTp', 'TckrSymb', 'XpryDt', 'ClsPric', 'StrkPric', 'OptnTp', 'HghPric', 'LwPric', 'LastPric']
        if not all(col in df_bhav.columns for col in required_cols):
            st.error(f"Uploaded file missing required columns: {required_cols}")
            return pd.DataFrame()

        # --- Process Bhavcopy Futures ---
        futures = df_bhav[df_bhav['FinInstrmTp'].isin(['STF', 'IDF'])].copy()
        if futures.empty:
            st.warning("No Futures data found in uploaded file.")
            return pd.DataFrame()

        futures['XpryDt'] = pd.to_datetime(futures['XpryDt'])
        
        # Filter out past expiries (Keep today and future)
        # We use IST time to match the environment's expectation
        ist_now = get_ist_now()
        today = ist_now.replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=None)
        
        futures = futures[futures['XpryDt'] >= today]
        if futures.empty:
            st.warning("No future expiries found in the uploaded file.")
            return pd.DataFrame()

        futures = futures.sort_values('XpryDt')
        
        # Identify unique expiry dates available in the bhavcopy
        available_expiries = sorted(futures['XpryDt'].unique())
        
        # Select target expiry based on index (0 for Near, 1 for Next)
        if target_expiry_index >= len(available_expiries):
            # Fallback to the latest available if index is out of range
            target_expiry = available_expiries[-1]
        else:
            target_expiry = available_expiries[target_expiry_index]

        # Filter futures for the target expiry per symbol
        near_futures = futures[futures['XpryDt'] == target_expiry].copy()
        
        # If a symbol doesn't have the target expiry, it will be skipped
        near_futures = near_futures[['TckrSymb', 'ClsPric', 'XpryDt']]
        near_futures = near_futures.rename(columns={'ClsPric': 'FuturePrice', 'XpryDt': 'FutureExpiryDate'})

        # --- Process Bhavcopy Options ---
        options = df_bhav[df_bhav['OptnTp'].isin(['CE', 'PE'])].copy()
        if options.empty:
            st.warning("No Options data found in uploaded file.")
            return pd.DataFrame()

        options['XpryDt'] = pd.to_datetime(options['XpryDt'])

        # Merge Options with selected Futures expiry
        merged = pd.merge(options, near_futures, on='TckrSymb')
        merged = merged[merged['XpryDt'] == merged['FutureExpiryDate']]
        
        # Calculate ATM
        merged['Diff'] = abs(merged['StrkPric'] - merged['FuturePrice'])
        
        # Find best strike per symbol (Minimize Diff, then tie-break with StrikePrice)
        # This ensures only ONE strike is selected per symbol, eliminating duplicates
        best_strikes = merged[['TckrSymb', 'StrkPric', 'Diff']].drop_duplicates()
        best_strikes = best_strikes.sort_values(by=['TckrSymb', 'Diff', 'StrkPric'])
        best_strikes = best_strikes.groupby('TckrSymb').first().reset_index()
        
        atm_options = pd.merge(merged, best_strikes[['TckrSymb', 'StrkPric']], on=['TckrSymb', 'StrkPric'])
        atm_rows = atm_options[['TckrSymb', 'XpryDt', 'StrkPric', 'OptnTp', 'FuturePrice', 'ClsPric', 'FinInstrmNm', 'HghPric', 'LwPric', 'LastPric']].copy()
        
        # Normalize dates for merging
        atm_rows['XpryDt'] = atm_rows['XpryDt'].dt.normalize()

        # Merge with Dhan Scrip Master
        result = pd.merge(
            atm_rows,
            df_json,
            left_on=['TckrSymb', 'StrkPric', 'OptnTp', 'XpryDt'],
            right_on=['underlying_symbol', 'strike_price', 'instrument_type', 'expiry_dt'],
            how='inner'
        )

        final_df = result[[
            'TckrSymb', 'XpryDt', 'StrkPric', 'OptnTp', 
            'FuturePrice', 'ClsPric', 'instrument_key',
            'HghPric', 'LwPric', 'LastPric'
        ]]

        final_df = final_df.rename(columns={
            'TckrSymb': 'Symbol',
            'XpryDt': 'ExpiryDate',
            'StrkPric': 'StrikePrice',
            'OptnTp': 'OptionType',
            'ClsPric': 'Trigger',
            'HghPric': 'HighPrice',
            'LwPric': 'LowPrice',
            'LastPric': 'LastPrice'
        })
        
        # Calculate Camarilla R4
        # Formula: Close + (High - Low) * 1.1 / 2
        final_df['Camarilla_R4'] = final_df['Trigger'] + (final_df['HighPrice'] - final_df['LowPrice']) * 1.1 / 2

        # Multiply Trigger by 2 (User Rule)
        if 'Trigger' in final_df.columns:
            final_df['Trigger'] = final_df['Trigger'] * 2
            
        return final_df

    except Exception as e:
        st.error(f"Error processing file: {e}")
        return pd.DataFrame()

def fetch_ltp(instrument_keys, access_token, client_id):
    if not access_token or not client_id:
        return {}
    
    url = "https://api.dhan.co/v2/marketfeed/ltp"
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'access-token': access_token,
        'client-id': client_id
    }
    
    # Dhan allows up to 1000 instruments
    batch_size = 1000
    ltp_map = {}
    
    batches = [instrument_keys[i:i + batch_size] for i in range(0, len(instrument_keys), batch_size)]
    
    def fetch_batch(batch):
        # Construct payload for NSE_FNO
        # Ensure keys are integers if Dhan expects integers, but JSON keys are strings usually.
        # Based on documentation example: "NSE_FNO":[49081,49082] -> Integers.
        # But my instrument_key is string. I should convert to int for the list.
        try:
            ids = [int(k) for k in batch]
        except:
            # Fallback if any key is not int
            ids = batch
            
        payload = {
            "NSE_FNO": ids
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    # Structure: data -> NSE_FNO -> id -> last_price
                    fno_data = data.get('data', {}).get('NSE_FNO', {})
                    result = {}
                    for key, details in fno_data.items():
                        # key is the security ID
                        last_price = details.get('last_price')
                        if last_price is not None:
                            result[str(key)] = last_price
                    return result
        except Exception as e:
            # st.error(f"Fetch error: {e}") # Debug only
            pass
        return {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_batch, batch) for batch in batches]
        for future in concurrent.futures.as_completed(futures):
            try:
                batch_result = future.result()
                if batch_result:
                    ltp_map.update(batch_result)
            except Exception:
                pass
    
    return ltp_map

def display_option_chain(df, access_token, client_id, key_suffix):
    st.caption(f"Last Updated: {get_ist_now().strftime('%H:%M:%S')} IST")
    if df.empty:
        st.info("No data to display. Please upload a valid Bhavcopy in the sidebar.")
        return

    # Fetch LTP if token provided
    if access_token and client_id:
        all_keys = df['instrument_key'].dropna().unique().tolist()
        
        # Time-based Fetch Logic
        ist_now = get_ist_now()
        current_time = ist_now.time()
        start_time = datetime.strptime("09:00", "%H:%M").time()
        end_time = datetime.strptime("15:40", "%H:%M").time()
        
        is_market_hours = start_time <= current_time <= end_time
        
        # Load Cache
        ltp_cache = load_ltp_cache()
        
        # Identify missing keys
        missing_keys = [k for k in all_keys if k not in ltp_cache]
        
        should_fetch = False
        fetch_reason = ""
        
        if is_market_hours:
            should_fetch = True
            fetch_reason = "Live Market Update"
        elif missing_keys:
            should_fetch = True
            fetch_reason = "Populating Missing Data"
        
        ltp_data = {}
        
        if should_fetch:
            keys_to_fetch = all_keys if is_market_hours else missing_keys
            # Fetch silently
            fetched_data = fetch_ltp(keys_to_fetch, access_token, client_id)
            if fetched_data:
                save_ltp_cache(fetched_data)
                # Reload cache to get complete set
                ltp_cache = load_ltp_cache()
        
        # Use data from cache
        ltp_data = {k: ltp_cache.get(k, 0.0) for k in all_keys}
        
        df['ltp'] = df['instrument_key'].map(ltp_data).fillna(0.0)
    else:
        df['ltp'] = 0.0
        st.warning("Enter Access Token and Client ID in sidebar to see live LTP.")

    # If Intraday, replace Trigger with Camarilla_R4
    if key_suffix == 'Intraday' and 'Camarilla_R4' in df.columns:
        df['Trigger'] = df['Camarilla_R4']

    # Calculate Change %
    def calculate_numeric_change(row):
        try:
            ocp = row['Trigger']
            ltp = row['ltp']
            if ocp > 0 and ltp > 0:
                return (ltp / ocp * 100)
            return 0.0
        except:
            return 0.0

    df['change_val'] = df.apply(calculate_numeric_change, axis=1)
    df['change %'] = df['change_val']

    # --- Intraday Blacklist Logic ---
    if key_suffix == 'Intraday':
        # Load existing blacklist
        blacklist = load_blacklist()
        
        # Check time condition (before 09:30)
        current_time = get_ist_now().time()
        cutoff_time = datetime.strptime("09:30", "%H:%M").time()
        
        if current_time < cutoff_time:
            # Identify new violators
            violators = df[df['change %'] >= 100]['instrument_key'].tolist()
            if violators:
                blacklist.update(violators)
                save_blacklist(blacklist)
        
        # Filter out blacklisted keys
        if blacklist:
            original_count = len(df)
            df = df[~df['instrument_key'].isin(blacklist)]
            filtered_count = len(df)
            diff = original_count - filtered_count
            # if diff > 0:
            #     st.caption(f"ℹ️ {diff} symbols hidden (Change % >= 100 before 09:30)")

    # Split Calls/Puts
    calls_df = df[df['OptionType'] == 'CE'].copy()
    puts_df = df[df['OptionType'] == 'PE'].copy()

    # Sort
    calls_df = calls_df.sort_values(by='change %', ascending=False)
    puts_df = puts_df.sort_values(by='change %', ascending=False)

    display_cols = ['Symbol', 'StrikePrice', 'Trigger', 'ltp', 'change %']
    
    # Styling
    def color_change(val):
        if isinstance(val, (int, float)):
            if val >= 100:
                return 'background-color: darkgreen; color: white'
            elif val >= 90:
                return 'background-color: lightgreen; color: black'
        return ''

    format_dict = {
        'change %': '{:.2f}%',
        'Trigger': '{:.2f}',
        'ltp': '{:.2f}',
        'StrikePrice': '{:.2f}'
    }

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Calls (CE)")
        st.dataframe(
            calls_df[display_cols].style
            .map(color_change, subset=['change %'])
            .format(format_dict)
            .set_properties(**{'font-weight': '600', 'text-align': 'center', 'font-size': '16px'}),
            hide_index=True, 
            use_container_width=True,
            height=1800
        )

    with col2:
        st.subheader("Puts (PE)")
        st.dataframe(
            puts_df[display_cols].style
            .map(color_change, subset=['change %'])
            .format(format_dict)
            .set_properties(**{'font-weight': '600', 'text-align': 'center', 'font-size': '16px'}),
            hide_index=True, 
            use_container_width=True,
            height=1800
        )

# --- Configuration Logic (Before Sidebar) ---
is_client_view = "DHAN_ACCESS_TOKEN" in st.secrets and "DHAN_CLIENT_ID" in st.secrets

if is_client_view:
    # CLIENT VIEW DEFAULTS
    access_token = st.secrets["DHAN_ACCESS_TOKEN"]
    client_id = st.secrets["DHAN_CLIENT_ID"]
    # Hide sidebar completely for clients
    st.markdown("""
    <style>
        [data-testid="stSidebar"] {display: none;}
    </style>
    """, unsafe_allow_html=True)
    
    # Default refresh settings for clients
    auto_refresh = True
    refresh_interval = 15
    target_expiry_idx = 0 # Default to current month for clients
    
else:
    # ADMIN VIEW (Show Sidebar)
    with st.sidebar:
        st.header("Configuration")
        
        # Local Token Logic
        saved_access_token, saved_client_id = load_dhan_creds()
        
        client_id = st.text_input("Dhan Client ID", value=saved_client_id)
        access_token = st.text_input("Dhan Access Token", value=saved_access_token, type="password")
        
        if (access_token and access_token != saved_access_token) or (client_id and client_id != saved_client_id):
            save_dhan_creds(access_token, client_id)

        st.markdown("---")
        st.header("Expiry Settings")
        # Expiry Selection for Monthly/Weekly/Intraday
        expiry_type = st.radio(
            "Select Expiry Month",
            options=["Current Month", "Next Month"],
            index=0,
            help="Choose which expiry month to display data for."
        )
        target_expiry_idx = 0 if expiry_type == "Current Month" else 1
    
        st.markdown("---")
        st.header("Data Management")
        
        # Dhan Scrip Master Uploader
        st.subheader("Dhan Scrip Master")
        
        if st.button("Download Latest Master"):
            with st.spinner("Downloading Scrip Master..."):
                try:
                    url = "https://images.dhan.co/api-data/api-scrip-master.csv"
                    response = requests.get(url)
                    response.raise_for_status()
                    with open(DHAN_MASTER_PATH, "wb") as f:
                        f.write(response.content)
                    st.cache_data.clear()
                    st.success("Downloaded & Updated!")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Download failed: {e}")

        up_master = st.file_uploader("Upload api-scrip-master.csv", type=['csv'], key='master_up')
        if up_master is not None:
             with open(DHAN_MASTER_PATH, "wb") as f:
                 f.write(up_master.getbuffer())
             st.cache_data.clear()
             st.success("Scrip master updated!")
             time.sleep(1)
             st.rerun()

        if os.path.exists(DHAN_MASTER_PATH):
             m_time = os.path.getmtime(DHAN_MASTER_PATH)
             st.caption(f"📅 Last Updated: {datetime.fromtimestamp(m_time).strftime('%Y-%m-%d %H:%M')}")
        
        # Monthly Uploader
        st.subheader("Monthly")
        up_m = st.file_uploader("Upload Monthly Bhavcopy", type=['csv'], key='m_up')
        if up_m is not None:
            with open(FILES['Monthly'], "wb") as f:
                f.write(up_m.getbuffer())
            # Extract and save date
            date_str = extract_date_from_filename(up_m.name)
            if date_str:
                save_meta('Monthly', date_str)
            st.success("Monthly file updated!")
        
        meta = load_meta()
        if 'Monthly' in meta and os.path.exists(FILES['Monthly']):
            st.caption(f"📅 Data Date: {meta['Monthly']}")
        elif os.path.exists(FILES['Monthly']):
            # Fallback to file time if no meta date
            m_time = os.path.getmtime(FILES['Monthly'])
            st.caption(f"📅 Last Updated: {datetime.fromtimestamp(m_time).strftime('%Y-%m-%d %H:%M')}")
        
        # Weekly Uploader
        st.subheader("Weekly")
        up_w = st.file_uploader("Upload Weekly Bhavcopy", type=['csv'], key='w_up')
        if up_w is not None:
            with open(FILES['Weekly'], "wb") as f:
                f.write(up_w.getbuffer())
            # Extract and save date
            date_str = extract_date_from_filename(up_w.name)
            if date_str:
                save_meta('Weekly', date_str)
            st.success("Weekly file updated!")

        if 'Weekly' in meta and os.path.exists(FILES['Weekly']):
            st.caption(f"📅 Data Date: {meta['Weekly']}")
        elif os.path.exists(FILES['Weekly']):
            w_time = os.path.getmtime(FILES['Weekly'])
            st.caption(f"📅 Last Updated: {datetime.fromtimestamp(w_time).strftime('%Y-%m-%d %H:%M')}")
        
        # Intraday Uploader
        st.subheader("Intraday")
        up_i = st.file_uploader("Upload Intraday Bhavcopy", type=['csv'], key='i_up')
        if up_i is not None:
            with open(FILES['Intraday'], "wb") as f:
                f.write(up_i.getbuffer())
            # Extract and save date
            date_str = extract_date_from_filename(up_i.name)
            if date_str:
                save_meta('Intraday', date_str)
            st.success("Intraday file updated!")
        
        if 'Intraday' in meta and os.path.exists(FILES['Intraday']):
            st.caption(f"📅 Data Date: {meta['Intraday']}")
        elif os.path.exists(FILES['Intraday']):
            i_time = os.path.getmtime(FILES['Intraday'])
            st.caption(f"📅 Last Updated: {datetime.fromtimestamp(i_time).strftime('%Y-%m-%d %H:%M')}")
            
        st.markdown("---")
        st.header("Auto Refresh")
        auto_refresh = st.checkbox("Enable Auto-Refresh", value=False)
        refresh_interval = st.slider("Refresh Interval (seconds)", min_value=5, max_value=60, value=15)

# --- Main Page ---
st.title("Positional Stock Option Scanner (Dhan)")
# st.caption(f"Last Updated: {get_ist_now().strftime('%H:%M:%S')} IST")

dhan_master_df = load_dhan_master()

if not dhan_master_df.empty:
    tab1, tab2, tab3 = st.tabs(["Monthly", "Weekly", "Intraday"])
    
    run_every = refresh_interval if auto_refresh else None

    with tab1:
        st.header(f"Monthly Options ({expiry_type if not is_client_view else 'Current Month'})")
        if os.path.exists(FILES['Monthly']):
            @st.fragment(run_every=run_every)
            def show_monthly():
                df_m = process_bhavcopy(FILES['Monthly'], dhan_master_df, target_expiry_index=target_expiry_idx)
                display_option_chain(df_m, access_token, client_id, "Monthly")
            show_monthly()
        else:
            st.info("Please upload a Monthly Bhavcopy in the sidebar to view data.")

    with tab2:
        st.header(f"Weekly Options ({expiry_type if not is_client_view else 'Current Month'})")
        if os.path.exists(FILES['Weekly']):
            @st.fragment(run_every=run_every)
            def show_weekly():
                df_w = process_bhavcopy(FILES['Weekly'], dhan_master_df, target_expiry_index=target_expiry_idx)
                display_option_chain(df_w, access_token, client_id, "Weekly")
            show_weekly()
        else:
            st.info("Please upload a Weekly Bhavcopy in the sidebar to view data.")

    with tab3:
        st.header(f"Intraday Options ({expiry_type if not is_client_view else 'Current Month'})")
        if os.path.exists(FILES['Intraday']):
            @st.fragment(run_every=run_every)
            def show_intraday():
                df_i = process_bhavcopy(FILES['Intraday'], dhan_master_df, target_expiry_index=target_expiry_idx)
                display_option_chain(df_i, access_token, client_id, "Intraday")
            show_intraday()
        else:
            st.info("Please upload an Intraday Bhavcopy in the sidebar to view data.")

else:
    st.error("Critical Error: Dhan Scrip Master could not be loaded.")
