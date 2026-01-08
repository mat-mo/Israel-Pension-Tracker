import pandas as pd
import json
import glob
import os
import math
import re

# ==========================================
# CONFIGURATION
# ==========================================
INPUT_PATTERN = "*.csv"
ITEMS_PER_PAGE = 10
OUTPUT_DIR = "output_data"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Track Names Mapping
TRACK_MAPPING = {
    2172: "Harel Pension Gilad General",
    2175: "Harel Pension Stocks",
    2174: "Harel Pension Bonds",
    2173: "Harel Pension Halacha",
    13415: "Harel Pension S&P 500",
    9823: "Harel Pension Track Up to 50",
    9827: "Harel Pension Track 50-60",
    9829: "Harel Pension Track 60+",
    15277: "Harel Pension Passive Stocks (Tradable)",
    15276: "Harel Pension Passive Bonds (Tradable)",
    9097: "Harel Pension Short Term Shekel",
    14198: "Harel Pension Flexible Index"
}

# Standard Mappings
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

NAME_COLUMNS = [
    "שם נייר ערך", "שם המנפיק", "שם נכס", "שם הבנק", "שם הלוואה", 
    "שם קרן השקעה", "שם הנכס", "שם הנכס האחר", "שם שותף כללי קרן השקעות",
    "טיקר", "מאפיין עיקרי"
]

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

def get_asset_name(row):
    keys = [k.strip() for k in row.keys()]
    for col in NAME_COLUMNS:
        if col in keys:
            val = row[col]
            if pd.notna(val) and str(val).strip() not in ['nan', 'ריק במקור']:
                return str(val).strip()
    return "Unknown Asset"

def clean_value(val):
    if pd.isna(val) or str(val).strip() in ['nan', 'ריק במקור']: return 0.0
    try:
        return float(str(val).replace(',', ''))
    except:
        return 0.0

def get_safe_filename(name):
    """
    Converts a track name into a safe filename.
    Example: "Harel Pension S&P 500" -> "Harel_Pension_S&P_500.json"
    """
    # Remove characters invalid in filenames
    clean = re.sub(r'[\\/*?:"<>|]', "", name)
    # Replace spaces with underscores
    clean = clean.replace(" ", "_")
    return f"{clean}.json"

# ==========================================
# MAIN LOGIC
# ==========================================

def main():
    all_tracks_data = {t_id: {} for t_id in TRACK_MAPPING}
    files = glob.glob(INPUT_PATTERN)
    
    print(f"Found {len(files)} CSV files. Processing...")

    # 1. Aggregation
    for f in files:
        if any(x in f for x in ["מיפוי סעיפים", "File Name Info", "סכום נכסים", "עמוד פתיחה"]):
            continue
        
        default_cls, default_sub = get_category(f)
        is_etf_file = "קרנות סל" in f
        
        try:
            df = pd.read_csv(f)
            df.columns = [c.strip() for c in df.columns]
            
            if 'מספר מסלול' not in df.columns: continue
            val_col = next((c for c in df.columns if "שווי הוגן" in c and "באלפי" in c), None)
            if not val_col: continue
            class_col = "סיווג הקרן" if "סיווג הקרן" in df.columns else None

            for _, row in df.iterrows():
                try:
                    raw_id = row['מספר מסלול']
                    if pd.isna(raw_id) or str(raw_id).strip() in ['nan', 'ריק במקור']: continue
                    track_id = int(float(raw_id))
                except:
                    continue
                
                if track_id not in TRACK_MAPPING: continue
                
                val = clean_value(row[val_col])
                val_bn = val / 1_000_000.0
                if abs(val_bn) < 1e-9: continue
                
                name = get_asset_name(row)
                cls, sub = default_cls, default_sub
                
                if is_etf_file and class_col:
                    c_val = str(row[class_col])
                    if "אג\"ח" in c_val or "אג”ח" in c_val:
                        cls, sub = "Bonds", "ETFs"

                if cls not in all_tracks_data[track_id]:
                    all_tracks_data[track_id][cls] = {}
                if sub not in all_tracks_data[track_id][cls]:
                    all_tracks_data[track_id][cls][sub] = []
                
                all_tracks_data[track_id][cls][sub].append({"name": name, "value": val_bn})
                
        except Exception as e:
            print(f"Skipping {f}: {e}")

    # 2. Generation
    print("Generating named JSON files...")
    manifest_tracks = []

    for t_id, t_name in TRACK_MAPPING.items():
        data_store = all_tracks_data[t_id]
        
        total_assets = 0.0
        for c in data_store:
            for s in data_store[c]:
                for item in data_store[c][s]:
                    total_assets += item['value']
        
        if total_assets == 0:
            print(f"Skipping empty track: {t_name}")
            continue

        asset_classes = []
        breakdown = {}
        
        for c_name, subs in data_store.items():
            c_total = sum(sum(i['value'] for i in s_list) for s_list in subs.values())
            c_pct = (c_total / total_assets) * 100
            
            asset_classes.append({
                "name": c_name,
                "value": round(c_total, 4),
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
                        "percentage": round(h_pct, 2)
                    })
                
                total_items = len(all_holdings)
                total_pages = math.ceil(total_items / ITEMS_PER_PAGE)
                paginated = [all_holdings[i:i + ITEMS_PER_PAGE] for i in range(0, total_items, ITEMS_PER_PAGE)]
                
                c_breakdown.append({
                    "subclass": s_name,
                    "value": round(s_total, 4),
                    "percentageOfClass": round(s_pct_class, 2),
                    "itemCount": total_items,
                    "totalPages": total_pages,
                    "holdingsPages": paginated
                })
            
            c_breakdown.sort(key=lambda x: x['value'], reverse=True)
            breakdown[c_name] = c_breakdown
            
        asset_classes.sort(key=lambda x: x['percentage'], reverse=True)
        
        # Determine Safe Filename
        safe_filename = get_safe_filename(t_name)
        
        final_obj = {
            "fundName": t_name,
            "trackId": str(t_id),
            "totalAssetsBN": round(total_assets, 2),
            "assetClasses": asset_classes,
            "breakdown": breakdown
        }
        
        out_path = os.path.join(OUTPUT_DIR, safe_filename)
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(final_obj, f, indent=2, ensure_ascii=False)
            
        manifest_tracks.append({
            "id": str(t_id),
            "name": t_name,
            "file": safe_filename  # The new filename
        })
        print(f"Created: {safe_filename}")

    # 3. Manifest
    inst_id = "512267592" # Harel ID
    manifest = [{
        "id": inst_id,
        "name": "Harel Pension",
        "directory": OUTPUT_DIR,
        "tracks": sorted(manifest_tracks, key=lambda x: x['name']) # Sort by name alphabetically
    }]

    with open("manifest.json", 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
        
    print("\nDone. Manifest updated.")

if __name__ == "__main__":
    main()
