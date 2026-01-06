import pandas as pd
import json
import glob
import os
import math

# ==========================================
# CONFIGURATION
# ==========================================
INPUT_PATTERN = "512267592*.csv"
ITEMS_PER_PAGE = 10

# Define the tracks you want to process
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

# Standard Asset Mappings
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

# Columns to search for the asset name
NAME_COLUMNS = [
    "שם נייר ערך", "שם המנפיק", "שם נכס", "שם הבנק", "שם הלוואה", 
    "שם קרן השקעה", "שם הנכס", "שם הנכס האחר", "שם שותף כללי קרן השקעות",
    "טיקר", "מאפיין עיקרי"
]

def get_category(filename):
    """Determine category based on filename."""
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

def get_name(row):
    """Extract asset name from possible columns."""
    keys = [k.strip() for k in row.keys()]
    for col in NAME_COLUMNS:
        if col in keys:
            val = row[next(k for k in row.keys() if k.strip() == col)]
            if pd.notna(val) and str(val).strip() not in ['nan', 'ריק במקור']:
                return str(val).strip()
    return "Unknown Asset"

def clean_val(val):
    """Clean numeric values from CSV."""
    if pd.isna(val) or str(val).strip() == 'ריק במקור': return 0.0
    try:
        return float(str(val).replace(',', ''))
    except:
        return 0.0

# ==========================================
# 1. LOAD AND PROCESS DATA
# ==========================================

# Data structure: { track_id: { class_name: { subclass_name: [items...] } } }
all_tracks_data = {t_id: {} for t_id in TRACK_MAPPING}

files = glob.glob(INPUT_PATTERN)
print(f"Found {len(files)} files to process.")

for f in files:
    # Skip metadata files
    if any(x in f for x in ["מיפוי סעיפים", "File Name Info", "סכום נכסים", "עמוד פתיחה"]):
        continue
    
    default_cls, default_sub = get_category(f)
    is_etf_file = "קרנות סל" in f
    
    try:
        # Read CSV
        df = pd.read_csv(f)
        df.columns = [c.strip() for c in df.columns]
        
        # Validation
        if 'מספר מסלול' not in df.columns: continue
        
        # Find value column (usually 'שווי הוגן')
        val_col = next((c for c in df.columns if "שווי הוגן" in c and "באלפי" in c), None)
        if not val_col: continue
        
        # Column for ETF classification
        class_col = "סיווג הקרן" if "סיווג הקרן" in df.columns else None

        # Iterate rows
        for _, row in df.iterrows():
            try:
                raw_track_id = row['מספר מסלול']
                if pd.isna(raw_track_id) or str(raw_track_id).strip() == 'ריק במקור': continue
                track_id = int(raw_track_id)
            except:
                continue
                
            # Only process tracks we care about
            if track_id not in TRACK_MAPPING: continue
            
            val = clean_val(row[val_col])
            val_bn = val / 1_000_000.0 # Convert to Billions
            
            if abs(val_bn) < 1e-9: continue # Skip zeros
            
            name = get_name(row)
            cls, sub = default_cls, default_sub
            
            # Reclassify Bond ETFs (often appear in Stocks file)
            if is_etf_file and class_col:
                c_val = str(row[class_col])
                if "אג\"ח" in c_val or "אג”ח" in c_val:
                    cls, sub = "Bonds", "ETFs"
            
            # Initialize dict structure if needed
            if cls not in all_tracks_data[track_id]:
                all_tracks_data[track_id][cls] = {}
            if sub not in all_tracks_data[track_id][cls]:
                all_tracks_data[track_id][cls][sub] = []
            
            # Add item
            all_tracks_data[track_id][cls][sub].append({
                "name": name, 
                "value": val_bn
            })
            
    except Exception as e:
        print(f"Error processing file {f}: {e}")

# ==========================================
# 2. GENERATE JSON FILES
# ==========================================

print("Generating JSON files...")

for t_id, t_name in TRACK_MAPPING.items():
    data_store = all_tracks_data[t_id]
    
    # Calculate Total Assets for this track
    total_assets = 0.0
    for c in data_store:
        for s in data_store[c]:
            for item in data_store[c][s]:
                total_assets += item['value']
                
    if total_assets == 0:
        print(f"Warning: No data found for track {t_id} ({t_name})")
        continue

    asset_classes = []
    breakdown = {}
    
    # Process each asset class
    for c_name, subs in data_store.items():
        # Calculate class total
        c_total = sum(sum(i['value'] for i in s_list) for s_list in subs.values())
        c_pct = (c_total / total_assets) * 100
        
        asset_classes.append({
            "name": c_name,
            "value": round(c_total, 4),
            "percentage": round(c_pct, 2)
        })
        
        # Process subclasses
        c_breakdown = []
        for s_name, items in subs.items():
            s_total = sum(i['value'] for i in items)
            s_pct_class = (s_total / c_total * 100) if c_total else 0
            
            # Aggregate duplicate names (e.g. same stock bought in different batches)
            grouped = {}
            for i in items:
                grouped[i['name']] = grouped.get(i['name'], 0) + i['value']
            
            # Sort holdings by value
            sorted_h = sorted(
                [{"name": k, "value": v} for k, v in grouped.items()], 
                key=lambda x: x['value'], 
                reverse=True
            )
            
            # Create final holdings list
            all_holdings_data = []
            for h in sorted_h:
                h_pct = (h['value'] / s_total * 100) if s_total else 0
                all_holdings_data.append({
                    "name": h['name'],
                    "value": round(h['value'], 4),
                    "percentage": round(h_pct, 2)
                })
            
            # PAGINATION LOGIC
            total_items = len(all_holdings_data)
            total_pages = math.ceil(total_items / ITEMS_PER_PAGE)
            # Split list into chunks
            paginated_list = [all_holdings_data[i:i + ITEMS_PER_PAGE] for i in range(0, total_items, ITEMS_PER_PAGE)]
            
            c_breakdown.append({
                "subclass": s_name,
                "value": round(s_total, 4),
                "percentageOfClass": round(s_pct_class, 2),
                "itemCount": total_items,
                "totalPages": total_pages,
                "holdingsPages": paginated_list
            })
        
        # Sort subclasses by size
        c_breakdown.sort(key=lambda x: x['value'], reverse=True)
        breakdown[c_name] = c_breakdown
        
    # Sort asset classes by size
    asset_classes.sort(key=lambda x: x['percentage'], reverse=True)
    
    # Final Object
    final_obj = {
        "fundName": t_name,
        "trackId": str(t_id),
        "totalAssetsBN": round(total_assets, 2),
        "assetClasses": asset_classes,
        "breakdown": breakdown
    }
    
    # Write to File
    filename = f"{t_id}.json"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(final_obj, f, indent=2, ensure_ascii=False)
        print(f"Created: {filename}")
    except Exception as e:
        print(f"Error saving {filename}: {e}")

print("Done!")
