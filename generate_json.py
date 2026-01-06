import pandas as pd
import json
import os
import re
import shutil

# --- Configuration ---
OUTPUT_DIR = "data"

# Map filenames to Asset Classes
files_config = {
    '520004896_in_p_0325.xlsx - מניות מבכ ויהש.csv': {'class': 'Stocks', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - איגרות חוב ממשלתיות.csv': {'class': 'Gov Bonds', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - איגרות חוב.csv': {'class': 'Corp Bonds', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - קרנות סל.csv': {'class': 'ETFs', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - קרנות השקעה.csv': {'class': 'Investment Funds', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - זכויות מקרקעין.csv': {'class': 'Real Estate', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - קרנות נאמנות.csv': {'class': 'Mutual Funds', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - כתבי אופציה.csv': {'class': 'Warrants', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - אופציות.csv': {'class': 'Options', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - חוזים עתידיים.csv': {'class': 'Futures', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - מוצרים מובנים.csv': {'class': 'Structured Products', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - לא סחיר איגרות חוב.csv': {'class': 'Non-Tradable Bonds', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - לא סחיר מניות מבכ ויהש.csv': {'class': 'Non-Tradable Stocks', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - לא סחיר כתבי אופציה.csv': {'class': 'Non-Tradable Warrants', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - לא סחיר אופציות.csv': {'class': 'Non-Tradable Options', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - לא סחיר נגזרים אחרים.csv': {'class': 'Derivatives', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - לא סחיר מוצרים מובנים.csv': {'class': 'Non-Tradable Structured', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - לא סחיר איגרות חוב מיועדות.csv': {'class': 'Designated Bonds', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - לא סחיר איגרות חוב ממשלתיות.csv': {'class': 'Non-Tradable Gov Bonds', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - לא סחיר ניירות ערך מסחריים.csv': {'class': 'Non-Tradable Commercial Paper', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - השקעה בחברות מוחזקות.csv': {'class': 'Held Companies', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - אפיק השקעה מובטח תשואה.csv': {'class': 'Guaranteed Yield', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - ניירות ערך מסחריים.csv': {'class': 'Commercial Paper', 'top_holding': True, 'include_in_general': True},
    '520004896_in_p_0325.xlsx - מזומנים ושווי מזומנים.csv': {'class': 'Cash & Equivalents', 'top_holding': True, 'include_in_general': False},
    '520004896_in_p_0325.xlsx - פיקדונות מעל 3 חודשים.csv': {'class': 'Deposits', 'top_holding': True, 'include_in_general': False},
    '520004896_in_p_0325.xlsx - הלוואות.csv': {'class': 'Loans', 'top_holding': True, 'include_in_general': False},
    '520004896_in_p_0325.xlsx - נכסים אחרים.csv': {'class': 'Other Assets', 'top_holding': True, 'include_in_general': True},
}

hierarchy_map = {
    'Stocks': ('Stocks', 'Direct Holdings'),
    'ETFs': ('Stocks', 'ETFs'),
    'Gov Bonds': ('Bonds', 'Government Bonds'),
    'Corp Bonds': ('Bonds', 'Corporate Bonds'),
    'Real Estate': ('Real Estate', 'Direct Real Estate'),
    'Cash & Equivalents': ('Cash & Equivalents', 'Cash'),
    'Deposits': ('Cash & Equivalents', 'Deposits'),
    'Loans': ('Loans', 'Direct Loans'),
    'Non-Tradable Stocks': ('Non-Tradable Stocks', 'Direct Holdings'),
    'Investment Funds': ('Investment Funds', 'Funds'),
    'Derivatives': ('Derivatives', 'General'),
    'Non-Tradable Structured': ('Structured Products', 'Non-Tradable'),
    'Structured Products': ('Structured Products', 'Tradable'),
    'Non-Tradable Bonds': ('Non-Tradable Bonds', 'General'),
    'Mutual Funds': ('Mutual Funds', 'General'),
    'Futures': ('Futures', 'General'),
    'Options': ('Options', 'Tradable'),
    'Non-Tradable Options': ('Options', 'Non-Tradable'),
    'Warrants': ('Warrants', 'Tradable'),
    'Non-Tradable Warrants': ('Warrants', 'Non-Tradable'),
    'Other Assets': ('Other Assets', 'General'),
    'Held Companies': ('Other Assets', 'Held Companies'),
    'Guaranteed Yield': ('Other Assets', 'Guaranteed Yield'),
    'Commercial Paper': ('Other Assets', 'Commercial Paper'),
    'Designated Bonds': ('Bonds', 'Designated Bonds'),
    'Non-Tradable Gov Bonds': ('Bonds', 'Non-Tradable Gov'),
    'Non-Tradable Commercial Paper': ('Other Assets', 'Non-Tradable CP')
}

INSTITUTIONS = {
    "520004896": "Migdal_Insurance",  # Using underscores for safe directory names
    "520042540": "Menora_Mivtachim",
    "513814814": "Altshuler_Shaham"
}

TRACK_NAMES = {
    "17012": "General Track (Klali)",
    "17013": "Stock Track (Menayot)",
    "9606": "S&P 500 Track",
    "15463": "Yield Guaranteed",
    "15464": "Pension Balanced"
}

# --- 1. Processing Data ---
db = {}

def get_institution_id(filename):
    match = re.match(r'^(\d{9})', filename)
    return match.group(1) if match else "Unknown"

# Data Aggregation Loop
for filename, config in files_config.items():
    raw_class = config['class']
    inst_id = get_institution_id(filename)
    # Default to ID if name not found
    inst_dir_name = INSTITUTIONS.get(inst_id, f"Inst_{inst_id}")
    inst_display_name = inst_dir_name.replace("_", " ")

    if inst_id not in db:
        db[inst_id] = {
            "name": inst_display_name,
            "dir_name": inst_dir_name,
            "tracks": {}
        }

    main_cls, sub_cls = hierarchy_map.get(raw_class, ('Other Assets', 'General'))
    
    try:
        df = pd.read_csv(filename, header=0)
        if 'מספר מסלול' not in df.columns:
            df = pd.read_csv(filename, header=1)
            
        track_col = next((c for c in df.columns if 'מספר מסלול' in str(c)), None)
        value_col = next((c for c in df.columns if 'שווי הוגן' in str(c) and 'אלפי' in str(c)), None)
        if not value_col: value_col = next((c for c in df.columns if 'שווי הוגן' in str(c)), None)
        if not value_col: value_col = next((c for c in df.columns if 'שווי הנכסים' in str(c)), None)

        name_col = None
        possible_names = ['שם הנכס', 'שם נייר ערך', 'שם המנפיק', 'שם הלוואה', 'שם הבנק', 'שם קרן']
        for p in possible_names:
            found = next((c for c in df.columns if p in str(c)), None)
            if found: name_col = found; break
        
        pct_class_col = next((c for c in df.columns if 'שיעור מנכסי אפיק' in str(c)), None)

        if not track_col or not value_col: continue

        df['val_clean'] = pd.to_numeric(df[value_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        if pct_class_col:
             df['pct_class_clean'] = pd.to_numeric(df[pct_class_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        else:
             df['pct_class_clean'] = 0

        for track_id, group in df.groupby(track_col):
            track_id = str(track_id).split('.')[0]
            if track_id in ['nan', 'ריק במקור']: continue
            
            if track_id not in db[inst_id]["tracks"]:
                db[inst_id]["tracks"][track_id] = {
                    "total": 0,
                    "main_classes": {},
                    "sub_classes": {}
                }
            
            t_data = db[inst_id]["tracks"][track_id]
            sum_val = group['val_clean'].sum()
            
            t_data['total'] += sum_val
            t_data['main_classes'][main_cls] = t_data['main_classes'].get(main_cls, 0) + sum_val
            
            if main_cls not in t_data['sub_classes']:
                t_data['sub_classes'][main_cls] = {}
            if sub_cls not in t_data['sub_classes'][main_cls]:
                t_data['sub_classes'][main_cls][sub_cls] = {'value': 0, 'holdings': []}
                
            t_data['sub_classes'][main_cls][sub_cls]['value'] += sum_val
            
            if name_col:
                top_chunk = group.sort_values(by='val_clean', ascending=False).head(20)
                for _, row in top_chunk.iterrows():
                    t_data['sub_classes'][main_cls][sub_cls]['holdings'].append({
                        'name': str(row[name_col]),
                        'value': row['val_clean'],
                        'pct_class': row['pct_class_clean'] * 100
                    })

    except Exception:
        continue

# --- 2. File Generation ---

# Ensure output directory exists
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR)

manifest = []

for inst_id, inst_data in db.items():
    inst_dir = os.path.join(OUTPUT_DIR, inst_data["dir_name"])
    os.makedirs(inst_dir, exist_ok=True)
    
    inst_entry = {
        "id": inst_id,
        "name": inst_data["name"],
        "directory": inst_data["dir_name"],
        "tracks": []
    }
    
    for track_id, t_data in inst_data["tracks"].items():
        if t_data['total'] < 1000: continue
        
        total_bn = t_data['total'] / 1000000.0
        track_name = TRACK_NAMES.get(track_id, f"Track {track_id}")
        
        # Build Track JSON
        track_json = {
            "trackId": track_id,
            "fundName": track_name,
            "totalAssetsBN": round(total_bn, 3),
            "assetClasses": [],
            "breakdown": {}
        }
        
        # Asset Classes
        for m_cls, m_val in t_data['main_classes'].items():
            if m_val == 0: continue
            track_json['assetClasses'].append({
                "name": m_cls,
                "value": round(m_val / 1000000.0, 4),
                "percentage": round((m_val / t_data['total']) * 100, 2)
            })
        track_json['assetClasses'].sort(key=lambda x: x['value'], reverse=True)
        
        # Breakdown
        for m_cls, subs in t_data['sub_classes'].items():
            track_json['breakdown'][m_cls] = []
            for s_name, s_data in subs.items():
                if s_data['value'] == 0: continue
                
                tops = sorted(s_data['holdings'], key=lambda x: x['value'], reverse=True)[:5]
                holdings_list = []
                for h in tops:
                    holdings_list.append({
                        "name": h['name'],
                        "value": round(h['value'] / 1000000.0, 5),
                        "percentage": round((h['value'] / s_data['value']) * 100, 2)
                    })
                    
                track_json['breakdown'][m_cls].append({
                    "subclass": s_name,
                    "value": round(s_data['value'] / 1000000.0, 4),
                    "percentageOfClass": round((s_data['value'] / t_data['main_classes'][m_cls]) * 100, 2),
                    "topHoldings": holdings_list
                })
            track_json['breakdown'][m_cls].sort(key=lambda x: x['value'], reverse=True)

        # Save Track File
        file_path = os.path.join(inst_dir, f"{track_id}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(track_json, f, ensure_ascii=False)
            
        # Add to Manifest
        inst_entry["tracks"].append({
            "id": track_id,
            "name": track_name,
            "file": f"{track_id}.json",
            "size_bn": round(total_bn, 2)
        })
    
    # Sort tracks in manifest by size
    inst_entry["tracks"].sort(key=lambda x: x['size_bn'], reverse=True)
    
    if inst_entry["tracks"]:
        manifest.append(inst_entry)

# Save Manifest
with open(os.path.join(OUTPUT_DIR, 'manifest.json'), 'w', encoding='utf-8') as f:
    json.dump(manifest, f, indent=2, ensure_ascii=False)

print("Site generation complete!")
