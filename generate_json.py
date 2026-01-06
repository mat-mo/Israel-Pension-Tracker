import pandas as pd
import json
import re

# ... (Include the full `files_config` and `hierarchy_map` dictionaries from the previous step here) ...

# Mapping Institution IDs to Names
INSTITUTIONS = {
    "520004896": "Migdal Insurance",
    "520042540": "Menora Mivtachim",
    "513814814": "Altshuler Shaham",
    "520004078": "Harel Insurance",
    "520017450": "The Phoenix",
    "520023185": "Clal Insurance"
}

# Mapping Track IDs to Names
TRACK_NAMES = {
    "17012": "General Track (Maslul Klali)",
    "17013": "Stock Track (Maslul Menayot)",
    "76": "Participating Policy (Track 76)",
    "9606": "S&P 500 Track",
    "15463": "Pension Yield Guaranteed",
    "15464": "Pension Balanced"
}

db = {}

def get_institution_id(filename):
    match = re.match(r'^(\d{9})', filename)
    return match.group(1) if match else "Unknown"

# Processing Loop
for filename, config in files_config.items(): # Changed from raw_class to config
    raw_class = config['class'] # Extract class string
    inst_id = get_institution_id(filename)
    inst_name = INSTITUTIONS.get(inst_id, f"Institution {inst_id}")
    
    if inst_id not in db:
        db[inst_id] = {"name": inst_name, "tracks": {}}
        
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

        if not track_col or not value_col: continue

        df['val_clean'] = pd.to_numeric(df[value_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

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
                top_chunk = group.sort_values(by='val_clean', ascending=False).head(10)
                for _, row in top_chunk.iterrows():
                    t_data['sub_classes'][main_cls][sub_cls]['holdings'].append({
                        'name': str(row[name_col]),
                        'value': row['val_clean']
                    })

    except Exception:
        continue

# Construct Final JSON
final_output = []

for inst_id, inst_data in db.items():
    inst_obj = {
        "institutionId": inst_id,
        "institutionName": inst_data["name"],
        "tracks": []
    }
    
    for track_id, t_data in inst_data["tracks"].items():
        if t_data['total'] < 1000: continue
        
        total_bn = t_data['total'] / 1000000.0
        
        track_obj = {
            "trackId": track_id,
            "fundName": TRACK_NAMES.get(track_id, f"Track {track_id}"),
            "totalAssetsBN": round(total_bn, 3),
            "assetClasses": [],
            "breakdown": {}
        }
        
        for m_cls, m_val in t_data['main_classes'].items():
            if m_val == 0: continue
            track_obj['assetClasses'].append({
                "name": m_cls,
                "value": round(m_val / 1000000.0, 4),
                "percentage": round((m_val / t_data['total']) * 100, 2)
            })
        track_obj['assetClasses'].sort(key=lambda x: x['value'], reverse=True)
        
        for m_cls, subs in t_data['sub_classes'].items():
            track_obj['breakdown'][m_cls] = []
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
                    
                track_obj['breakdown'][m_cls].append({
                    "subclass": s_name,
                    "value": round(s_data['value'] / 1000000.0, 4),
                    "percentageOfClass": round((s_data['value'] / t_data['main_classes'][m_cls]) * 100, 2),
                    "topHoldings": holdings_list
                })
            track_obj['breakdown'][m_cls].sort(key=lambda x: x['value'], reverse=True)
            
        inst_obj['tracks'].append(track_obj)
        
    inst_obj['tracks'].sort(key=lambda x: x['totalAssetsBN'], reverse=True)
    if inst_obj['tracks']:
        final_output.append(inst_obj)

print(json.dumps(final_output, indent=2, ensure_ascii=False))
