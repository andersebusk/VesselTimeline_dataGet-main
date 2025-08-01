import msal
import requests
import openpyxl
import re
import unicodedata
import difflib
from datetime import datetime as dt
from io import BytesIO
import pandas as pd
from sqlalchemy import create_engine, text
import os
import numpy as np

# ==============================
# CONFIGURATION (you provide these)
# ==============================
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TARGET_SITE_DISPLAY_NAME = os.getenv("TARGET_SITE_DISPLAY_NAME")
FOLDER_PATH = os.getenv("FOLDER_PATH")
TARGET_FILE_NAME = os.getenv("TARGET_FILE_NAME")

# Database Configuration
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")  # Convert to int later if needed
DB_NAME = os.getenv("DB_NAME")
DB_TABLE = os.getenv("DB_TABLE_1")

# ==============================
# HELPER FUNCTIONS
# ==============================
def normalize_string(s):
    s = s.lower().strip()
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def string_similarity(str1, str2):
    str1_normalized = normalize_string(str1)
    str2_normalized = normalize_string(str2)
    match = difflib.SequenceMatcher(None, str1_normalized, str2_normalized)
    return match.ratio()

def find_value_columns_by_headers(sheet, header_strings):
    header_indices = {}
    for row in sheet.iter_rows(min_row=4, max_row=4):
        for index, cell in enumerate(row[:100]):
            cell_value_str = str(cell.value).strip() if cell.value else ""
            for target_header in header_strings:
                target_normalized = normalize_string(target_header)
                if target_header.startswith(("Fe magnetic", "Fe corrosive", "Fe total", "Residual TBN", "Unit")):
                    if target_normalized == normalize_string(cell_value_str):
                        header_indices[target_header] = index
                        break
                elif string_similarity(cell_value_str, target_normalized) >= 0.60:
                    header_indices[target_header] = index
    return header_indices

def parse_date(date_value):
    if isinstance(date_value, dt):
        return date_value.strftime("%Y-%m-%d")
    date_patterns = [r"(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2,4})"]
    for pattern in date_patterns:
        match = re.match(pattern, str(date_value))
        if match:
            part1, part2, year = match.groups()
            if len(year) == 2:
                year = '20' + year
            day, month = part1, part2
            try:
                date_obj = dt.strptime(f"{day}-{month}-{year}", "%d-%m-%Y")
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                return None
    return None

def map_sheet_names(workbook):
    mapping = {}
    overview_sheet = workbook["Overview"]
    for row in overview_sheet.iter_rows(min_row=2, max_row=150, values_only=True):
        if row[4] is not None and row[3] is not None:
            try:
                mapping[int(row[4])] = row[3]
            except ValueError:
                mapping[str(row[4])] = row[3]
    return mapping

def detect_max_cylinders(header_columns):
    max_unit = 0
    for key in header_columns.keys():
        match = re.match(r".*?(\d+)$", key)
        if match:
            max_unit = max(max_unit, int(match.group(1)))
    return max_unit

def process_xlsx(sheet, date_col, header_columns, max_rows=None):
    data_rows = []
    max_cylinders = detect_max_cylinders(header_columns)
    for row in sheet.iter_rows(min_row=6, max_row=max_rows or sheet.max_row, values_only=True):
        date_value = row[date_col]
        if not date_value:
            continue
        parsed_date = parse_date(date_value)
        me_load = row[1] if len(row) > 1 else 0
        me_rh_col = header_columns.get("ME rh", header_columns.get("ME", None))
        me_rh = row[me_rh_col] if me_rh_col is not None and me_rh_col < len(row) else None
        tbn_fed_index = header_columns.get("TBN of blended oil fed to engine", None)
        tbn_fed_value = row[tbn_fed_index] if tbn_fed_index is not None and tbn_fed_index < len(row) else None
        fuel_sulphur_index = header_columns.get("Fuel Sulphur Content", None)
        fuel_sulphur_value = row[fuel_sulphur_index] if fuel_sulphur_index is not None and fuel_sulphur_index < len(row) else None

        for unit_num in range(1, max_cylinders + 1):
            fe_magnetic_col = header_columns.get(f"Fe magnetic {unit_num}")
            fe_corrosive_col = header_columns.get(f"Fe corrosive {unit_num}")
            fe_total_col = header_columns.get(f"Fe total {unit_num}")
            residual_tbn_col = header_columns.get(f"Residual TBN {unit_num}")
            unit_col = header_columns.get(f"Unit {unit_num}")

            data_rows.append({
                "Date": parsed_date,
                "ME_RH": me_rh,
                "Cylinder": f"Cyl. {unit_num}",
                "Fe_Magnet": row[fe_magnetic_col] if fe_magnetic_col is not None else None,
                "Fe_Corrosion": row[fe_corrosive_col] if fe_corrosive_col is not None else None,
                "Fe_Total": row[fe_total_col] if fe_total_col is not None else None,
                "Residual_TBN": row[residual_tbn_col] if residual_tbn_col is not None else None,
                "Unit_RH": row[unit_col] if unit_col is not None else None,
                "TBN_Fed": None,
                "Fuel_Sulph": fuel_sulphur_value,
                "ME_Load": None
            })

        data_rows.append({
            "Date": parsed_date,
            "ME_RH": me_rh,
            "Cylinder": "TBN Fed",
            "Fe_Magnet": None,
            "Fe_Corrosion": None,
            "Fe_Total": None,
            "Residual_TBN": None,
            "Unit_RH": None,
            "TBN_Fed": tbn_fed_value,
            "Fuel_Sulph": fuel_sulphur_value,
            "ME_Load": None
        })

        data_rows.append({
            "Date": parsed_date,
            "ME_RH": me_rh,
            "Cylinder": "ME Load",
            "Fe_Magnet": None,
            "Fe_Corrosion": None,
            "Fe_Total": None,
            "Residual_TBN": None,
            "Unit_RH": None,
            "TBN_Fed": None,
            "Fuel_Sulph": fuel_sulphur_value,
            "ME_Load": me_load
        })
    return data_rows

# ==============================
# STEP 1: AUTHENTICATE & DOWNLOAD FILE FROM ONEDRIVE
# ==============================
print("🔑 Authenticating to Microsoft Graph...")
app = msal.ConfidentialClientApplication(CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}", client_credential=CLIENT_SECRET)
token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
access_token = token["access_token"]
headers = {"Authorization": f"Bearer {access_token}"}

site_resp = requests.get(f"https://graph.microsoft.com/v1.0/sites?search={TARGET_SITE_DISPLAY_NAME}", headers=headers)
site_id = site_resp.json()["value"][0]["id"]

folder_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{FOLDER_PATH}:/children"
files_resp = requests.get(folder_url, headers=headers).json()
file_id = next(f["id"] for f in files_resp["value"] if f["name"].lower() == TARGET_FILE_NAME.lower())

file_dl_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/content"
file_resp = requests.get(file_dl_url, headers=headers)
excel_bytes = BytesIO(file_resp.content)
workbook = openpyxl.load_workbook(excel_bytes, data_only=True, read_only=True)
print("✅ Excel downloaded from OneDrive!")

# ==============================
# STEP 2: FORMAT DATA
# ==============================
headers_to_find = [
    f"Fe magnetic {i}" for i in range(1, 13)
] + [f"Fe corrosive {i}" for i in range(1, 13)] + [
    f"Fe total {i}" for i in range(1, 13)
] + [f"Residual TBN {i}" for i in range(1, 13)] + [
    f"Unit {i}" for i in range(1, 13)
] + ["ME rh", "ME", "TBN of blended oil fed to engine", "Fuel Sulphur Content"]

sheet_mapping = map_sheet_names(workbook)
all_rows = []

for sheet_name in [s for s in workbook.sheetnames if s not in ["Overview", "Dashboard table", "Dashboard"]]:
    sheet = workbook[sheet_name]
    header_columns = find_value_columns_by_headers(sheet, headers_to_find)
    if not header_columns:
        print(f"⚠️ Skipping sheet '{sheet_name}' (no headers)")
        continue

    extracted_rows = process_xlsx(sheet, 0, header_columns)

    try:
        sheet_key = int(sheet_name)
    except ValueError:
        sheet_key = sheet_name

    vessel_name = sheet_mapping.get(sheet_key, None)
    if vessel_name is None:
        print(f"⚠️ Skipping sheet '{sheet_name}' (no valid vessel mapping found).")
        continue

    print(f"🔎 Mapping sheet '{sheet_name}' → VesselID: '{vessel_name}'")

    for row in extracted_rows:
        row["VesselID"] = vessel_name
        all_rows.append(row)

print(f"\n✅ Total rows ready for insertion: {len(all_rows)}")

# ==============================
# STEP 3: CLEAN AND INSERT INTO MYSQL
# ==============================
df = pd.DataFrame(all_rows)

# Clean numeric fields: strip whitespace and replace invalid characters
numeric_columns = ["Fe_Magnet", "Fe_Corrosion", "Fe_Total", "Residual_TBN", "Unit_RH", "TBN_Fed", "Fuel_Sulph", "ME_Load", "ME_RH"]

for col in numeric_columns:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)                           # Convert all to string
            .str.replace(r'\s+', '', regex=True)   # Remove all whitespace (including \xa0)
            .replace('-', None)                    # Replace dashes with None
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")  # Convert invalid to NaN

print("✅ Cleaned numeric fields.")

# Create DB connection
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Clear and insert into DB
with engine.begin() as conn:
    conn.execute(text(f"DELETE FROM {DB_TABLE}"))
    conn.execute(text(f"ALTER TABLE {DB_TABLE} AUTO_INCREMENT = 1"))
print(f"🗑 Cleared existing data in {DB_TABLE}.")

df.drop(columns=["ReportID"], errors="ignore").to_sql(DB_TABLE, con=engine, if_exists="append", index=False)
print(f"✅ Inserted {len(df)} rows into {DB_TABLE} in MySQL!")

# ==============================
# STEP 4: PUSH TO POWER BI STREAMING DATASET
# ==============================
print("\n🚀 Pushing data from MySQL to Power BI Streaming Dataset...")

# Power BI Config (fill in)
PBI_WORKSPACE_ID = os.getenv("PBI_WORKSPACE_ID")
PBI_TENANT_ID = os.getenv("PBI_TENANT_ID")
PBI_CLIENT_ID = os.getenv("PBI_CLIENT_ID")
PBI_CLIENT_SECRET = os.getenv("PBI_CLIENT_SECRET")


# ==============================
# AUTHENTICATE POWER BI SERVICE PRINCIPAL
# ==============================
print("🔑 Authenticating Power BI Service Principal...")
pbi_app = msal.ConfidentialClientApplication(
    client_id=PBI_CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{PBI_TENANT_ID}",
    client_credential=PBI_CLIENT_SECRET,
)
pbi_token = pbi_app.acquire_token_for_client(scopes=["https://analysis.windows.net/powerbi/api/.default"])

if "access_token" not in pbi_token:
    print("❌ Failed to acquire Power BI token:", pbi_token.get("error_description", pbi_token))
    raise SystemExit("Stopping: Authentication failed.")

pbi_access_token = pbi_token["access_token"]
pbi_headers = {"Authorization": f"Bearer {pbi_access_token}"}
print("✅ Power BI token acquired successfully.\n")

# ==============================
# VERIFY DATASET AND TABLE
# ==============================
print(f"📥 Fetching datasets from workspace: {PBI_WORKSPACE_ID}")
datasets_url = f"https://api.powerbi.com/v1.0/myorg/groups/{PBI_WORKSPACE_ID}/datasets"
resp = requests.get(datasets_url, headers=pbi_headers)

if resp.status_code != 200:
    print(f"❌ Failed to list datasets. Status: {resp.status_code}, Response: {resp.text}")
    raise SystemExit("Stopping: Could not list datasets.")

datasets = resp.json().get("value", [])
if not datasets:
    raise SystemExit("⚠️ No datasets found in this workspace.")

# Filter for streaming datasets
streaming_datasets = [ds for ds in datasets if ds.get("addRowsAPIEnabled", False)]
if not streaming_datasets:
    raise SystemExit("❌ No streaming (push API enabled) datasets found in this workspace.")

# ✅ Explicitly select the streaming dataset by name
TARGET_DATASET_NAME = "VesselData"  # <-- Change to match the dataset name you want

dataset = next((ds for ds in streaming_datasets if ds["name"].lower() == TARGET_DATASET_NAME.lower()), None)
if not dataset:
    raise SystemExit(f"❌ Streaming dataset '{TARGET_DATASET_NAME}' not found in workspace.")

PBI_DATASET_ID = dataset["id"]
print(f"🎯 Selected streaming dataset: {dataset['name']} (ID: {PBI_DATASET_ID})")

# Verify tables in selected dataset
tables_url = f"https://api.powerbi.com/v1.0/myorg/groups/{PBI_WORKSPACE_ID}/datasets/{PBI_DATASET_ID}/tables"
tables_resp = requests.get(tables_url, headers=pbi_headers)
if tables_resp.status_code != 200:
    print(f"❌ Cannot access tables. Status: {tables_resp.status_code}, Response: {tables_resp.text}")
    raise SystemExit("Stopping: Dataset table verification failed.")

tables_data = tables_resp.json().get("value", [])
if not tables_data:
    raise SystemExit("⚠️ No tables found in this dataset.")

PBI_TABLE_NAME = tables_data[0]["name"]
print(f"✅ Verified table: {PBI_TABLE_NAME}")

# ==============================
# FETCH DATA FROM MYSQL
# ==============================
print("📥 Fetching data from MySQL to push into Power BI...")
with engine.connect() as conn:
    result_df = pd.read_sql(f"SELECT * FROM {DB_TABLE}", conn)

# Convert date columns to string (ISO format)
for col in result_df.columns:
    if pd.api.types.is_datetime64_any_dtype(result_df[col]) or pd.api.types.is_object_dtype(result_df[col]):
        if "date" in col.lower():
            result_df[col] = pd.to_datetime(result_df[col], errors='coerce').dt.strftime('%Y-%m-%d')

# 🔥 Clean NaNs, infinities, and enforce None for JSON null
for col in result_df.columns:
    if pd.api.types.is_numeric_dtype(result_df[col]):
        result_df[col] = result_df[col].replace([np.inf, -np.inf], np.nan)  # Replace inf with NaN
        result_df[col] = result_df[col].where(pd.notnull(result_df[col]), None)  # Replace NaN with None
    else:
        result_df[col] = result_df[col].fillna("")  # Text columns: NaN -> ""

# ✅ Full dataframe-wide replacement to eliminate any lingering NaN/inf
result_df = result_df.replace([np.nan, np.inf, -np.inf], None)

# 🛠 Debug check: log columns with remaining bad values
bad_columns = [col for col in result_df.columns if any(x is np.nan for x in result_df[col])]
if bad_columns:
    print(f"⚠️ Columns still containing NaN: {bad_columns}")
else:
    print("✅ All NaN/inf values successfully replaced with None.")

# Convert to JSON-ready rows (None -> null in JSON)
rows_to_push = result_df.to_dict(orient="records")

# ==============================
# CLEAR OLD ROWS IN STREAMING DATASET
# ==============================
pbi_clear_url = f"https://api.powerbi.com/v1.0/myorg/groups/{PBI_WORKSPACE_ID}/datasets/{PBI_DATASET_ID}/tables/{PBI_TABLE_NAME}/rows"
print("🗑 Clearing old rows in Power BI streaming dataset...")
clear_response = requests.delete(pbi_clear_url, headers=pbi_headers)

if clear_response.status_code in [200, 202]:
    print("✅ Cleared old rows in Power BI.")
else:
    print(f"⚠️ Warning clearing rows: {clear_response.status_code} {clear_response.text}")

# ==============================
# PUSH ROWS TO POWER BI
# ==============================
print(f"📤 Pushing {len(rows_to_push)} rows to Power BI...")
batch_size = 10000
for i in range(0, len(rows_to_push), batch_size):
    batch = rows_to_push[i:i + batch_size]
    push_response = requests.post(pbi_clear_url, headers={**pbi_headers, "Content-Type": "application/json"}, json={"rows": batch})
    if push_response.status_code in [200, 202]:
        print(f"✅ Batch {i//batch_size+1} pushed successfully ({len(batch)} rows).")
    else:
        print(f"❌ Failed to push batch {i//batch_size+1}: {push_response.status_code} {push_response.text}")
        raise SystemExit("Stopping due to API error.")

print("🎉 All data successfully pushed to Power BI Streaming Dataset!")