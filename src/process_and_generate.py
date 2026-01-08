import pandas as pd
import json
import glob
import os
import math
import sys
import re
import warnings
from pathlib import Path
from datetime import datetime

# Suppress Excel validation warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# ==========================================
# 1. CONFIGURATION
# ==========================================
BASE_PATH = Path("/Users/matanya/git-repos/Israel-Pension-Tracker")
INPUT_DIRECTORY = BASE_PATH / "institution_reports"
OUTPUT_BASE_DIRECTORY = BASE_PATH / "data"
CONFIG_FILE = BASE_PATH / "config.json"
MASTER_TRACK_FILE = BASE_PATH / "master_track_list.json"
ITEMS_PER_PAGE = 10

# Columns to search for Track Name (Backup if not in Master List)
TRACK_NAME_COLUMNS = ["שם מסלול", "שם המסלול", "שם קופה", "שם הקופה", "שם מסלול השקעה"]

# Asset Name Columns
NAME_COLUMNS = [
    "שם נייר ערך", "שם המנפיק", "שם נכס", "שם הבנק", "שם הלוואה", 
    "שם קרן השקעה", "שם הנכס", "שם הנכס האחר", "שם שותף כללי קרן השקעות",
    "טיקר", "מאפיין עיקרי"
]

# Static Asset Mappings
FILE_MAPPING = {
    "מזומנים": ("Cash & Equivalents", "Cash"),
    "פיקדונות": ("Cash & Equivalents", "Deposits"),
    "מניות": ("Stocks", "Direct Holdings"),
    "קרנות סל": ("Stocks", "ETFs"),
    "אג\"ח ממשלתיות": ("Bonds", "Government Bonds"),
    "איגרות חוב ממשלתיות": ("Bonds", "Government Bonds"),
    "אג\"ח קונצרני": ("Bonds", "Corporate Bonds"),
    "איגרות חוב": ("Bonds", "Corporate Bonds"),
    "ניירות ערך מסחריים": ("Bonds", "Commercial Paper"),
    "קרנות נאמנות": ("Mutual Funds", "General"),
    "קרנות השקעה": ("Investment Funds", "Funds"),
    "התחייבות להשקעה": ("Investment Funds", "Commitments"),
    "זכויות מקרקעין": ("Real Estate", "Direct Real Estate"),
    "הלוואות": ("Loans", "Direct Loans"),
    "מסגרות אשראי": ("Loans", "Credit Lines"),
    "נגזרים": ("Derivatives", "General"),
    "חוזים עתידיים": ("Futures", "General"),
    "אופציות": ("Options", "Tradable"),
    "כתבי אופציה": ("Warrants", "Tradable"),
    "מוצרים מובנים": ("Structured Products", "General"),
    "נכסים אחרים": ("Other Assets", "General"),
    "יתרות התחייבות": ("Other Assets", "Commitment Balances"),
}

# ==========================================
# 2. DATA LOADING
# ==========================================

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def load_master_track_list():
    """Reads the external JSON mapping file."""
    if not MASTER_TRACK_FILE.exists():
        log(f"[!!!] CRITICAL ERROR: Master Track List not found at {MASTER_TRACK_FILE}")
        return None
    
    try:
        with open(MASTER_TRACK_FILE, 'r', encoding='utf-8') as f:
            track_map = json.load(f)
            
        # Ensure all IDs are strings
        cleaned_map = {str(k).strip(): str(v).strip() for k, v in track_map.items()}
        
        log(f"Loaded {len(cleaned_map)} tracks from Master List (JSON).")
        return cleaned_map

    except Exception as e:
        log(f"[!!!] CRITICAL ERROR: Could not read JSON file: {e}")
        return None

# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================

def format_currency(value_bn):
    """
    Formats a value (in Billions) to a readable string with B/M/K units.
    value_bn: Float representing Billions.
    """
    if value_bn == 0:
        return "0"
        
    abs_val = abs(value_bn)
    
    if abs_val >= 1.0:
        # Billions
        return f"{value_bn:,.2f}B"
    elif abs_val >= 0.001:
        # Millions (0.001 BN = 1 Million)
        val_m = value_bn * 1_000
        return f"{val_m:,.2f}M"
    else:
        # Thousands (0.000001 BN = 1 Thousand)
        val_k = value_bn * 1_000_000
        return f"{val_k:,.2f}K"

def get_category(filename):
    if "לא סחיר" in filename:
        if "מניות" in filename: return ("Non-Tradable Stocks", "Direct Holdings")
        if "איגרות חוב" in filename or "אג\"ח" in filename: return ("Non-Tradable Bonds", "General")
        if "אופציות" in filename: return ("Options", "Non-Tradable")
        if "כתבי אופציה" in filename: return ("Warrants", "Non-Tradable")
        if "מוצרים מובנים" in filename: return ("Structured Products", "Non-Tradable")
        if "נגזרים" in filename: return ("Derivatives", "General")
        if "ניירות ערך מסחריים" in filename: return ("Non-Tradable Bonds", "Commercial Paper")
    
    for key, (cls, sub) in FILE_MAPPING.items():
        if key in filename:
            if key == "איגרות חוב" and "ממשלתיות" in filename: continue
            return cls, sub
    return "Other Assets", "Unclassified"

def get_column_value(row, possible_columns):
    keys = [k.strip() for k in row.keys()]
    for col in possible_columns:
        if col in keys:
            val = row[col]
            if pd.notna(val) and str(val).strip() not in ['nan', 'ריק במקור']:
                return str(val).strip()
    return None

def clean_value(val):
    if pd.isna(val) or str(val).strip() in ['nan', 'ריק במקור', 'תא ללא תוכן, המשך בתא הבא']: return 0.0
    try:
        # Handle cases where value might be string with commas
        return float(str(val).replace(',', ''))
    except:
        return 0.0

def get_safe_filename(name):
    name_str = str(name)
    clean = re.sub(r'[\\/*?:"<>|]', "", name_str)
    clean = clean.replace(" ", "_")
    return f"{clean}.json"

# ==========================================
# 4. CORE PIPELINE (UPDATED FOR MENORA + FORMATTING)
# ==========================================

def detect_header_row(xls, sheet_name):
    """
    Intelligently finds the header row index by looking for 'מספר מסלול'.
    Returns the 0-based index of the header row.
    """
    try:
        # Read the first 20 rows without a header to inspect them
        df_preview = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=20)
        
        for idx, row in df_preview.iterrows():
            # Convert row to string to search for keywords
            row_str = " ".join([str(x) for x in row.values])
            
            # "מספר מסלול" is the absolute anchor for all pension files
            if "מספר מסלול" in row_str:
                return idx
                
        return 0 # Default to 0 if not found
    except Exception as e:
        return 0

def split_excel_to_csvs(file_path, target_dir):
    try:
        xls = pd.ExcelFile(file_path)
        log(f"Splitting {file_path.name} ({len(xls.sheet_names)} sheets)...")
        
        for sheet_name in xls.sheet_names:
            try:
                # 1. Detect dynamic header row (Fix for Menora/Phoenix differences)
                header_idx = detect_header_row(xls, sheet_name)
                
                # 2. Read with correct header
                df = pd.read_excel(xls, sheet_name=sheet_name, header=header_idx)
                
                # 3. Save to CSV
                csv_filename = f"{file_path.stem} - {sheet_name}.csv"
                save_path = target_dir / csv_filename
                df.to_csv(save_path, index=False, encoding='utf-8-sig')
                
            except Exception as e:
                log(f"[Warning] Skipped sheet {sheet_name}: {e}")
                pass 
        return True
    except Exception as e:
        log(f"[!!] Error reading Excel: {e}")
        return False

def process_institution_data(target_dir, inst_key, config, master_map):
    """
    Scans CSVs, builds data, and FORCE UPDATES the config for this institution.
    """
    all_tracks_data = {} 
    
    # Initialize Institution in Config (Overwrite if exists)
    if inst_key not in config['institutions']:
        config['institutions'][inst_key] = {
            "name": inst_key.replace("_", " "), 
            "tracks": {}
        }
    
    inst_tracks_config = config['institutions'][inst_key]["tracks"]
    
    csv_files = list(target_dir.glob("*.csv"))
    if not csv_files: return {}

    log(f"Scanning {len(csv_files)} CSVs...")

    for f in csv_files:
        if any(x in f.name for x in ["מיפוי סעיפים", "File Name Info", "סכום נכסים", "עמוד פתיחה"]): continue
        
        default_cls, default_sub = get_category(f.name)
        is_etf_file = "קרנות סל" in f.name
        
        try:
            df = pd.read_csv(f)
            df.columns = [c.strip() for c in df.columns]
            
            # Validation: Ensure we found the header
            if 'מספר מסלול' not in df.columns: 
                # This usually means the file is empty or structure is unrecognizable
                continue
                
            # Flexible Value Column Detection (Fix for "שווי הוגן (באלפי ש""ח)")
            val_col = next((c for c in df.columns if "שווי" in c and "הוגן" in c and "באלפי" in c), None)
            
            # Fallback for standard naming
            if not val_col:
                val_col = next((c for c in df.columns if "שווי" in c and "שוק" in c), None)
            
            if not val_col: continue
            
            class_col = "סיווג הקרן" if "סיווג הקרן" in df.columns else None

            for _, row in df.iterrows():
                try:
                    raw_id = row['מספר מסלול']
                    if pd.isna(raw_id) or str(raw_id).strip() in ['nan', 'ריק במקור']: continue
                    track_id = str(int(float(raw_id)))
                except:
                    continue
                
                # --- FORCE NAMING LOGIC ---
                # 1. Priority: Master JSON
                if track_id in master_map:
                    inst_tracks_config[track_id] = master_map[track_id]
                
                # 2. Priority: CSV Header (only if not in Master)
                elif track_id not in inst_tracks_config:
                    found_name = get_column_value(row, TRACK_NAME_COLUMNS)
                    if found_name:
                        inst_tracks_config[track_id] = found_name
                    else:
                        inst_tracks_config[track_id] = f"Unknown Track {track_id}"
                # --------------------------
                
                # Extract Data
                val = clean_value(row[val_col])
                val_bn = val / 1_000_000.0
                if abs(val_bn) < 1e-9: continue
                
                name = get_column_value(row, NAME_COLUMNS) or "Unknown Asset"
                cls, sub = default_cls, default_sub
                
                if is_etf_file and class_col:
                    c_val = str(row[class_col])
                    if "אג\"ח" in c_val or "אג”ח" in c_val: cls, sub = "Bonds", "ETFs"

                if track_id not in all_tracks_data: all_tracks_data[track_id] = {}
                if cls not in all_tracks_data[track_id]: all_tracks_data[track_id][cls] = {}
                if sub not in all_tracks_data[track_id][cls]: all_tracks_data[track_id][cls][sub] = []
                
                all_tracks_data[track_id][cls][sub].append({"name": name, "value": val_bn})
                
        except Exception as e:
            # log(f"Error processing row in {f.name}: {e}")
            pass 

    return all_tracks_data

def generate_jsons(target_dir, all_tracks_data, inst_key, config):
    track_map = config['institutions'][inst_key]['tracks']
    manifest_entries = []

    for t_id, data_store in all_tracks_data.items():
        t_name = track_map.get(t_id, f"Track {t_id}")
        
        total_assets = sum(sum(i['value'] for i in s) for c in data_store.values() for s in c.values())
        if total_assets == 0: continue

        asset_classes = []
        breakdown = {}
        
        for c_name, subs in data_store.items():
            c_total = sum(sum(i['value'] for i in s_list) for s_list in subs.values())
            c_pct = (c_total / total_assets) * 100
            
            # Added formattedValue
            asset_classes.append({
                "name": c_name, 
                "value": round(c_total, 4), 
                "formattedValue": format_currency(c_total),
                "percentage": round(c_pct, 2)
            })
            
            c_breakdown = []
            for s_name, items in subs.items():
                s_total = sum(i['value'] for i in items)
                s_pct_class = (s_total / c_total * 100) if c_total else 0
                
                grouped = {}
                for i in items: grouped[i['name']] = grouped.get(i['name'], 0) + i['value']
                sorted_h = sorted([{"name":k,"value":v} for k,v in grouped.items()], key=lambda x:x['value'], reverse=True)
                
                all_holdings = []
                for h in sorted_h:
                    h_pct = (h['value'] / s_total * 100) if s_total else 0
                    all_holdings.append({
                        "name": h['name'], 
                        "value": round(h['value'], 4), 
                        "formattedValue": format_currency(h['value']),
                        "percentage": round(h_pct, 2)
                    })
                
                total_items = len(all_holdings)
                total_pages = math.ceil(total_items / ITEMS_PER_PAGE)
                paginated = [all_holdings[i:i + ITEMS_PER_PAGE] for i in range(0, total_items, ITEMS_PER_PAGE)]
                
                c_breakdown.append({
                    "subclass": s_name, 
                    "value": round(s_total, 4), 
                    "formattedValue": format_currency(s_total),
                    "percentageOfClass": round(s_pct_class, 2),
                    "itemCount": total_items, 
                    "totalPages": total_pages, 
                    "holdingsPages": paginated
                })
            c_breakdown.sort(key=lambda x: x['value'], reverse=True)
            breakdown[c_name] = c_breakdown
            
        asset_classes.sort(key=lambda x: x['percentage'], reverse=True)
        safe_filename = get_safe_filename(t_name)
        
        final_obj = {
            "fundName": t_name, 
            "trackId": t_id, 
            "totalAssetsBN": round(total_assets, 2), 
            "formattedTotalAssets": format_currency(total_assets),
            "assetClasses": asset_classes, 
            "breakdown": breakdown
        }
        
        with open(target_dir / safe_filename, 'w', encoding='utf-8') as f:
            json.dump(final_obj, f, indent=2, ensure_ascii=False)
            
        manifest_entries.append({"id": t_id, "name": t_name, "file": safe_filename})
    
    log(f"Generated {len(manifest_entries)} track JSONs for {inst_key}.")
    return sorted(manifest_entries, key=lambda x: x['name'])

# ==========================================
# 5. MAIN
# ==========================================

def main():
    # 1. Load Master Map First
    log("Loading Master Track List...")
    master_map = load_master_track_list()
    
    if not master_map:
        log("\n[!!!] ABORTING: Master Track List failed to load.")
        sys.exit(1)

    # 2. Initialize fresh config
    log("Initializing fresh config...")
    config = {"institutions": {}} 
    
    if not INPUT_DIRECTORY.exists():
        print(f"Error: Input dir missing: {INPUT_DIRECTORY}")
        return

    excel_files = list(INPUT_DIRECTORY.glob("*.xlsx"))
    global_manifest = []

    OUTPUT_BASE_DIRECTORY.mkdir(parents=True, exist_ok=True)

    for excel_path in excel_files:
        log(f"--- Processing: {excel_path.name} ---")
        inst_key = excel_path.stem
        target_dir = OUTPUT_BASE_DIRECTORY / inst_key
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # A. Split Excel
        if split_excel_to_csvs(excel_path, target_dir):
            
            # B. Process Data & Update Config (Force)
            all_data = process_institution_data(target_dir, inst_key, config, master_map)
            
            # C. Generate JSONs
            tracks_list = generate_jsons(target_dir, all_data, inst_key, config)
            
            # D. Add to Manifest
            inst_name = config['institutions'][inst_key].get("name", inst_key)
            global_manifest.append({
                "id": inst_key,
                "name": inst_name,
                "directory": inst_key,
                "tracks": tracks_list
            })

    # 3. Force Write Config
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    log("Force-updated config.json")

    # 4. Force Write Manifest
    with open(OUTPUT_BASE_DIRECTORY / "manifest.json", 'w', encoding='utf-8') as f:
        json.dump(global_manifest, f, indent=2, ensure_ascii=False)
    
    log(f"--- Pipeline Complete. All files regenerated. ---")

if __name__ == "__main__":
    main()
