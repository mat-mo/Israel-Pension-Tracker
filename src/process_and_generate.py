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
MAPPING_FILE = BASE_PATH / "master_country_currency_map.json"

ITEMS_PER_PAGE = 10

# Columns to search for Track Name
TRACK_NAME_COLUMNS = ["שם מסלול", "שם המסלול", "שם קופה", "שם הקופה", "שם מסלול השקעה"]

# Asset Name Columns
NAME_COLUMNS = [
    "שם נייר ערך", "שם המנפיק", "שם נכס", "שם הבנק", "שם הלוואה", 
    "שם קרן השקעה", "שם הנכס", "שם הנכס האחר", "שם שותף כללי קרן השקעות",
    "טיקר", "מאפיין עיקרי"
]

# Country Columns
COUNTRY_COLUMNS = [
    "מדינה לפי חשיפה כלכלית", "מדינת הרישום", "מדינת התאגדות",
    "מדינת מיקום נדל\"ן", "מקום המסחר", "מדינה", "ארץ", "Country"
]

# Currency Columns
CURRENCY_COLUMNS = ["מטבע פעילות", "מטבע", "סוג מטבע", "בסיס הצמדה", "Currency", "Linkage Base"]

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

# --- Runtime Dictionaries (Populated from JSON) ---
COUNTRY_LOOKUP = {}      # Maps match_string -> Emoji (e.g., "ארהב" -> "🇺🇸")
EMOJI_TO_NAME = {}       # Maps Emoji -> Display Name (e.g., "🇺🇸" -> "USA")
CURRENCY_LOOKUP = {}     # Maps match_string -> Currency Code (e.g., "dollar" -> "USD")
HEDGED_KEYWORDS = ["מנוטרל", "גידור", "hedged", "currency hedged", "נטרול"]

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def load_mappings():
    """
    Reads master_country_currency_map.json (LIST FORMAT) and builds optimized lookup dictionaries.
    """
    global COUNTRY_LOOKUP, EMOJI_TO_NAME, CURRENCY_LOOKUP
    
    if not MAPPING_FILE.exists():
        log(f"[!!!] CRITICAL: Mapping file not found at {MAPPING_FILE}")
        return False
        
    try:
        with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
            data_list = json.load(f)
            
        count = 0
        for entry in data_list:
            emoji = entry.get("emoji", "❓")
            name = entry.get("name", "Unknown")
            curr_code = entry.get("currency_code", "ILS")
            matches = entry.get("match_strings", [])
            
            # 1. Map Emoji to Display Name
            EMOJI_TO_NAME[emoji] = name
            
            # 2. Map all Match Strings to the Emoji & Currency
            for s in matches:
                clean_s = str(s).strip()
                COUNTRY_LOOKUP[clean_s] = emoji
                COUNTRY_LOOKUP[clean_s.lower()] = emoji # Case insensitive support
                
                # Build Currency Lookup based on these keywords too
                # (Only if string length > 2 to avoid bad matches like 'US')
                if len(clean_s) > 2:
                    CURRENCY_LOOKUP[clean_s.lower()] = curr_code
                    
            count += 1
            
        log(f"Mappings loaded successfully for {count} entries.")
        return True
    except Exception as e:
        log(f"[!!!] Error loading mappings: {e}")
        return False

def load_master_track_list():
    if not MASTER_TRACK_FILE.exists(): return None
    try:
        with open(MASTER_TRACK_FILE, 'r', encoding='utf-8') as f:
            track_map = json.load(f)
        return {str(k).strip(): str(v).strip() for k, v in track_map.items()}
    except Exception as e:
        log(f"[!!!] CRITICAL ERROR: Could not read Master Track List: {e}")
        return None

def format_currency(value_bn):
    if value_bn == 0: return "0"
    abs_val = abs(value_bn)
    if abs_val >= 1.0: return f"{value_bn:,.2f}B"
    elif abs_val >= 0.001: return f"{(value_bn * 1_000):,.2f}M"
    else: return f"{(value_bn * 1_000_000):,.2f}K"

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

def get_country_emoji(row, asset_class=""):
    """Determines Country Emoji using the loaded JSON logic."""
    
    # 1. Check specific country columns (Exact Match)
    val = get_column_value(row, COUNTRY_COLUMNS)
    if val:
        clean_raw = str(val).strip()
        # Direct lookup
        if clean_raw in COUNTRY_LOOKUP: return COUNTRY_LOOKUP[clean_raw]
        # Clean quotes
        clean_no_quotes = clean_raw.replace('"', '').replace("'", "")
        if clean_no_quotes in COUNTRY_LOOKUP: return COUNTRY_LOOKUP[clean_no_quotes]
        # Lowercase check
        if clean_raw.lower() in COUNTRY_LOOKUP: return COUNTRY_LOOKUP[clean_raw.lower()]

    # 2. Check Asset Name for Keywords (Thematic/Regional/Country names)
    asset_name = get_column_value(row, NAME_COLUMNS)
    if asset_name:
        name_lower = str(asset_name).strip().lower()
        
        # Scan for ANY known match string in the name
        for match_str, emoji in COUNTRY_LOOKUP.items():
            # Skip very short keys like "IL", "US" to prevent "TRUST" matching "US"
            if len(match_str) > 3 and match_str in name_lower:
                return emoji

    # 3. Fallback: General Israel/Abroad column
    val_general = get_column_value(row, ["ישראל/חו\"ל", "ישראל/חו''ל", "Israel/Abroad"])
    if val_general:
        if "ישראל" in str(val_general) or "Israel" in str(val_general):
             return "🇮🇱"

    # 4. Smart Default for Cash/Loans
    if asset_class in ["Cash & Equivalents", "Loans"]: return "🇮🇱"
    
    # 5. Generic Fallback
    return "🌎"

def detect_currency(row, country_emoji, asset_name):
    name_str = str(asset_name).lower() if asset_name else ""
    
    # 1. Hedging Check
    if any(k in name_str for k in HEDGED_KEYWORDS): return "ILS"

    # 2. Explicit Column Check
    val = get_column_value(row, CURRENCY_COLUMNS)
    if val:
        clean = str(val).strip().lower()
        # Check against loaded keywords
        for match_str, code in CURRENCY_LOOKUP.items():
            if match_str in clean: return code
        if "צמוד מדד" in clean: return "ILS" 
    
    # 3. Name Inference (Scan name for "Dollar", "Euro", etc from JSON)
    for match_str, code in CURRENCY_LOOKUP.items():
        if len(match_str) > 3 and match_str in name_str: 
            return code

    # 4. Country Inference
    if country_emoji == "🇺🇸": return "USD"
    if country_emoji == "🇬🇧": return "GBP"
    if country_emoji == "🇯🇵": return "JPY"
    if country_emoji in ["🇪🇺", "🇫🇷", "🇩🇪", "🇳🇱", "🇮🇹", "🇪🇸"]: return "EUR"
    
    return "ILS"

def clean_value(val):
    s = str(val).strip()
    if pd.isna(val) or s in ['nan', 'ריק במקור', 'תא ללא תוכן, המשך בתא הבא']: return 0.0
    try:
        s = s.replace('−', '-').replace('–', '-').replace(',', '')
        if s.startswith('(') and s.endswith(')'):
            s = '-' + s[1:-1].strip()
        return float(s)
    except:
        return 0.0

def get_safe_filename(name):
    name_str = str(name)
    clean = re.sub(r'[\\/*?:"<>|]', "", name_str)
    clean = clean.replace(" ", "_")
    return f"{clean}.json"

def detect_header_row(xls, sheet_name):
    try:
        df_preview = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=20)
        for idx, row in df_preview.iterrows():
            row_str = " ".join([str(x) for x in row.values])
            if "מספר מסלול" in row_str: return idx
        return 0
    except: return 0

def split_excel_to_csvs(file_path, target_dir):
    try:
        xls = pd.ExcelFile(file_path)
        log(f"Splitting {file_path.name}...")
        for sheet_name in xls.sheet_names:
            try:
                header_idx = detect_header_row(xls, sheet_name)
                df = pd.read_excel(xls, sheet_name=sheet_name, header=header_idx)
                csv_filename = f"{file_path.stem} - {sheet_name}.csv"
                df.to_csv(target_dir / csv_filename, index=False, encoding='utf-8-sig')
            except Exception as e: pass 
        return True
    except Exception as e:
        log(f"[!!] Error reading Excel: {e}")
        return False

# ==========================================
# 3. CORE LOGIC
# ==========================================

def process_institution_data(target_dir, inst_key, config, master_map):
    all_tracks_data = {} 
    
    if inst_key not in config['institutions']:
        config['institutions'][inst_key] = { "name": inst_key.replace("_", " "), "tracks": {} }
    
    inst_tracks_config = config['institutions'][inst_key]["tracks"]
    csv_files = list(target_dir.glob("*.csv"))

    log(f"Scanning {len(csv_files)} CSVs...")

    for f in csv_files:
        if any(x in f.name for x in ["מיפוי סעיפים", "File Name Info", "סכום נכסים", "עמוד פתיחה"]): continue
        default_cls, default_sub = get_category(f.name)
        is_etf_file = "קרנות סל" in f.name
        
        try:
            df = pd.read_csv(f)
            df.columns = [c.strip() for c in df.columns]
            if 'מספר מסלול' not in df.columns: continue
            
            val_col = next((c for c in df.columns if "שווי" in c and "הוגן" in c and "באלפי" in c), None)
            if not val_col: val_col = next((c for c in df.columns if "שווי" in c and "שוק" in c and "באלפי" in c), None)
            if not val_col: val_col = next((c for c in df.columns if "שווי" in c and "הוגן" in c), None)
            if not val_col: val_col = next((c for c in df.columns if "שווי" in c and "שוק" in c), None)
            if not val_col: val_col = next((c for c in df.columns if "שווי" in c), None)
            
            if not val_col: continue 
            class_col = "סיווג הקרן" if "סיווג הקרן" in df.columns else None

            for _, row in df.iterrows():
                try:
                    raw_id = row['מספר מסלול']
                    if pd.isna(raw_id) or str(raw_id).strip() in ['nan', 'ריק במקור']: continue
                    track_id = str(int(float(raw_id)))
                except: continue
                
                if track_id in master_map: inst_tracks_config[track_id] = master_map[track_id]
                elif track_id not in inst_tracks_config:
                    found_name = get_column_value(row, TRACK_NAME_COLUMNS)
                    inst_tracks_config[track_id] = found_name if found_name else f"Unknown Track {track_id}"
                
                val = clean_value(row[val_col])
                val_bn = val / 1_000_000.0
                # KEEPING THRESHOLD VERY LOW TO CATCH EVERYTHING
                if abs(val_bn) < 1e-12: continue 
                
                name = get_column_value(row, NAME_COLUMNS) or "Unknown Asset"
                cls, sub = default_cls, default_sub
                if is_etf_file and class_col:
                    c_val = str(row[class_col])
                    if "אג\"ח" in c_val or "אג”ח" in c_val: cls, sub = "Bonds", "ETFs"

                emoji = get_country_emoji(row, cls) 
                currency = detect_currency(row, emoji, name)

                if track_id not in all_tracks_data: all_tracks_data[track_id] = {}
                if cls not in all_tracks_data[track_id]: all_tracks_data[track_id][cls] = {}
                if sub not in all_tracks_data[track_id][cls]: all_tracks_data[track_id][cls][sub] = []
                
                all_tracks_data[track_id][cls][sub].append({
                    "name": name, 
                    "value": val_bn,
                    "emoji": emoji,
                    "currency": currency 
                })
        except Exception: pass 
    return all_tracks_data

def calculate_geo_sunburst(data_store):
    country_groups = {} 
    for cls_name, subclasses in data_store.items():
        for sub_items in subclasses.values():
            for item in sub_items:
                emoji = item.get("emoji", "")
                country_key = emoji if emoji else "Other"
                if country_key not in country_groups: country_groups[country_key] = {}
                if cls_name not in country_groups[country_key]: country_groups[country_key][cls_name] = 0.0
                country_groups[country_key][cls_name] += abs(item["value"]) 

    sunburst_data = []
    for country_key, assets_dict in country_groups.items():
        asset_children = []
        children_sum = 0.0
        for cls_name, abs_val in assets_dict.items():
            if abs_val > 1e-12:
                asset_children.append({ "name": cls_name, "value": round(abs_val, 6), "formattedValue": format_currency(abs_val) })
                children_sum += abs_val
        if not asset_children: continue
        asset_children.sort(key=lambda x: x["value"], reverse=True)
        display_name = EMOJI_TO_NAME.get(country_key, "Global" if country_key == "Other" else country_key)
        sunburst_data.append({ "name": display_name, "value": round(children_sum, 6), "formattedValue": format_currency(children_sum), "children": asset_children })
    sunburst_data.sort(key=lambda x: x["value"], reverse=True)
    return sunburst_data

def calculate_currency_sunburst(data_store):
    currency_groups = {} 
    for cls_name, subclasses in data_store.items():
        for sub_items in subclasses.values():
            for item in sub_items:
                curr = item.get("currency", "ILS")
                if curr not in currency_groups: currency_groups[curr] = {}
                if cls_name not in currency_groups[curr]: currency_groups[curr][cls_name] = 0.0
                currency_groups[curr][cls_name] += abs(item["value"]) 

    sunburst_data = []
    for curr, assets_dict in currency_groups.items():
        asset_children = []
        children_sum = 0.0
        for cls_name, abs_val in assets_dict.items():
            if abs_val > 1e-12:
                asset_children.append({ "name": cls_name, "value": round(abs_val, 6), "formattedValue": format_currency(abs_val) })
                children_sum += abs_val
        if not asset_children: continue
        asset_children.sort(key=lambda x: x["value"], reverse=True)
        sunburst_data.append({ "name": curr, "value": round(children_sum, 6), "formattedValue": format_currency(children_sum), "children": asset_children })
    sunburst_data.sort(key=lambda x: x["value"], reverse=True)
    return sunburst_data

def generate_jsons(target_dir, all_tracks_data, inst_key, config):
    track_map = config['institutions'][inst_key]['tracks']
    manifest_entries = []
    
    # NEW: Accumulator for Institution AUM
    inst_total_aum = 0.0

    for t_id, data_store in all_tracks_data.items():
        t_name = track_map.get(t_id, f"Track {t_id}")
        
        # NET Assets for Display and Main Pie
        total_assets = sum(sum(i['value'] for i in s) for c in data_store.values() for s in c.values())
        if total_assets == 0: continue
        
        # Add to Institution Total
        inst_total_aum += total_assets

        asset_classes = []
        breakdown = {}
        
        for c_name, subs in data_store.items():
            c_net = sum(sum(i['value'] for i in s_list) for s_list in subs.values())
            c_pct = (c_net / total_assets) * 100
            
            asset_classes.append({
                "name": c_name, 
                "value": round(c_net, 4), 
                "formattedValue": format_currency(c_net), 
                "percentage": round(c_pct, 2)
            })
            
            c_breakdown = []
            for s_name, items in subs.items():
                s_net = sum(i['value'] for i in items)
                s_pct_class = (s_net / c_net * 100) if c_net else 0
                
                grouped = {}
                name_to_emoji = {}
                
                for i in items: 
                    grouped[i['name']] = grouped.get(i['name'], 0) + i['value']
                    if i['emoji']: name_to_emoji[i['name']] = i['emoji']

                sorted_h = sorted([
                    {"name": k, "value": v, "emoji": name_to_emoji.get(k, "")} 
                    for k,v in grouped.items()
                ], key=lambda x: abs(x['value']), reverse=True)
                
                all_holdings = []
                for h in sorted_h:
                    h_pct = (h['value'] / s_net * 100) if s_net else 0
                    all_holdings.append({
                        "name": h['name'], 
                        "value": round(h['value'], 4), 
                        "formattedValue": format_currency(h['value']),
                        "percentage": round(h_pct, 2),
                        "countryEmoji": h['emoji']
                    })
                
                total_items = len(all_holdings)
                total_pages = math.ceil(total_items / ITEMS_PER_PAGE)
                paginated = [all_holdings[i:i + ITEMS_PER_PAGE] for i in range(0, total_items, ITEMS_PER_PAGE)]
                
                c_breakdown.append({
                    "subclass": s_name, 
                    "value": round(s_net, 4), 
                    "formattedValue": format_currency(s_net),
                    "percentageOfClass": round(s_pct_class, 2),
                    "itemCount": total_items, 
                    "totalPages": total_pages, 
                    "holdingsPages": paginated
                })
            c_breakdown.sort(key=lambda x: x['value'], reverse=True)
            breakdown[c_name] = c_breakdown
            
        asset_classes.sort(key=lambda x: x['percentage'], reverse=True)
        
        geo_sunburst_data = calculate_geo_sunburst(data_store)
        currency_sunburst_data = calculate_currency_sunburst(data_store)

        safe_filename = get_safe_filename(t_name)
        
        final_obj = {
            "fundName": t_name, 
            "trackId": t_id, 
            "totalAssetsBN": round(total_assets, 2), 
            "formattedTotalAssets": format_currency(total_assets),
            "assetClasses": asset_classes, 
            "breakdown": breakdown,
            "geoSunburst": geo_sunburst_data,
            "currencySunburst": currency_sunburst_data
        }
        
        with open(target_dir / safe_filename, 'w', encoding='utf-8') as f:
            json.dump(final_obj, f, indent=2, ensure_ascii=False)
            
        manifest_entries.append({"id": t_id, "name": t_name, "file": safe_filename})
    
    return sorted(manifest_entries, key=lambda x: x['name']), inst_total_aum

def main():
    log("Starting Pipeline...")
    if not load_mappings():
        return

    log("Loading Master Track List...")
    master_map = load_master_track_list() or {}
    config = {"institutions": {}} 
    
    if not INPUT_DIRECTORY.exists(): 
        log(f"Input directory {INPUT_DIRECTORY} not found.")
        return
        
    excel_files = list(INPUT_DIRECTORY.glob("*.xlsx"))
    global_manifest = []

    OUTPUT_BASE_DIRECTORY.mkdir(parents=True, exist_ok=True)

    for excel_path in excel_files:
        log(f"--- Processing: {excel_path.name} ---")
        inst_key = excel_path.stem
        target_dir = OUTPUT_BASE_DIRECTORY / inst_key
        target_dir.mkdir(parents=True, exist_ok=True)
        
        if split_excel_to_csvs(excel_path, target_dir):
            all_data = process_institution_data(target_dir, inst_key, config, master_map)
            tracks_list, total_aum = generate_jsons(target_dir, all_data, inst_key, config)
            inst_name = config['institutions'][inst_key].get("name", inst_key)
            
            formatted_aum = format_currency(total_aum)
            
            global_manifest.append({
                "id": inst_key, 
                "name": inst_name, 
                "directory": inst_key, 
                "tracks": tracks_list,
                "totalAUM": formatted_aum
            })

    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    with open(OUTPUT_BASE_DIRECTORY / "manifest.json", 'w', encoding='utf-8') as f:
        json.dump(global_manifest, f, indent=2, ensure_ascii=False)
    
    log(f"--- Pipeline Complete. ---")

if __name__ == "__main__":
    main()
