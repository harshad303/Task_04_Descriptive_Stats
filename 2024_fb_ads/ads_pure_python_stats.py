import csv
import json
import math
from collections import defaultdict, Counter
from statistics import mean, stdev

import sys

# Redirect print output to a text file
sys.stdout = open("pure_python_summary_output.txt", "w", encoding="utf-8")


# === STEP 1: Load and Clean Data ===
csv_path = "2024_fb_ads/2024_fb_ads_president_scored_anon.csv"
 

with open(csv_path, mode="r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    data = list(reader)

# === STEP 2: Unpack JSON-like Columns ===
json_columns = ['delivery_by_region', 'demographic_distribution']

def is_float(val: str) -> bool:
    try:
        float(val)
        return True
    except:
        return False

def clean_value(val):
    if val is None:
        return None
    if isinstance(val, str):
        val = val.strip()
        if val == '':
            return None
        if is_float(val):
            return float(val)
        return val
    elif isinstance(val, (int, float)):
        return float(val)
    else:
        return None


def unpack_json_columns(data, json_cols):
    for row in data:
        for col in json_cols:
            try:
                parsed = json.loads(row[col].replace("'", '"'))
                if isinstance(parsed, dict):
                    for key, nested in parsed.items():
                        if isinstance(nested, dict):
                            for subkey, value in nested.items():
                                new_col = f"{col}_{key}_{subkey}"
                                row[new_col] = value
                        else:
                            new_col = f"{col}_{key}"
                            row[new_col] = nested
                row[col] = None  # Remove original JSON column
            except Exception:
                row[col] = None  # In case of error or empty
    return data

data = unpack_json_columns(data, json_columns)

# === STEP 3: Descriptive Stats ===
def describe_column(col_name, values):
    numeric_vals = [v for v in values if isinstance(v, float)]
    non_null_vals = [v for v in values if v is not None]

    print(f"\n--- Column: {col_name} ---")
    print(f"Count: {len(non_null_vals)}")

    if numeric_vals:
        print(f"Mean: {mean(numeric_vals):.2f}")
        print(f"Min: {min(numeric_vals)}")
        print(f"Max: {max(numeric_vals)}")
        if len(numeric_vals) > 1:
            print(f"Std Dev: {stdev(numeric_vals):.2f}")
    else:
        counter = Counter(non_null_vals)
        print(f"Unique Values: {len(counter)}")
        most_common = counter.most_common(1)
        if most_common:
            print(f"Most Frequent: {most_common[0][0]} ({most_common[0][1]} times)")

# === STEP 4: Dataset-Level Analysis ===
print("\n========== OVERALL DATASET SUMMARY ==========")
all_columns = sorted(data[0].keys())

for col in all_columns:
    values = [clean_value(row.get(col)) for row in data]
    describe_column(col, values)

# === STEP 5: Grouped Analysis ===
def group_and_describe(data, group_keys):
    print(f"\n========== GROUPED BY {group_keys} ==========")
    grouped = defaultdict(list)
    for row in data:
        key = tuple(row[k] for k in group_keys)
        grouped[key].append(row)

    for group, rows in list(grouped.items())[:3]:  # Limit output for brevity
        print(f"\n--- Group: {group} ---")
        for col in all_columns:
            values = [clean_value(r.get(col)) for r in rows]
            describe_column(col, values)

group_and_describe(data, ["page_id"])
group_and_describe(data, ["page_id", "ad_id"])
# Restore standard output (optional)
sys.stdout.close()
sys.stdout = sys.__stdout__
print("✅ All summary stats written to pure_python_summary_output.txt")
