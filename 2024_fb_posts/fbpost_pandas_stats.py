import pandas as pd
import json
import time
import sys
from pathlib import Path

def safe_json_loads(value):
    if pd.isna(value) or value == '':
        return None
    try:
        if isinstance(value, str):
            value = value.replace("'", '"')
        return json.loads(value)
    except Exception:
        return None

def unpack_json_column(df, column_name):
    print(f"Unpacking {column_name}...")
    parsed = df[column_name].apply(safe_json_loads)
    new_columns = {}
    for idx, parsed_val in parsed.items():
        if isinstance(parsed_val, dict):
            for key, value in parsed_val.items():
                if isinstance(value, dict):
                    for subkey, subvalue in value.items():
                        col_name = f"{column_name}_{key}_{subkey}"
                        if col_name not in new_columns:
                            new_columns[col_name] = [None] * len(df)
                        new_columns[col_name][idx] = subvalue
                else:
                    col_name = f"{column_name}_{key}"
                    if col_name not in new_columns:
                        new_columns[col_name] = [None] * len(df)
                    new_columns[col_name][idx] = value
    if new_columns:
        new_df = pd.DataFrame(new_columns, index=df.index)
        df = pd.concat([df.drop(columns=[column_name]), new_df], axis=1)
    else:
        df = df.drop(columns=[column_name])
    return df

def main():
    start_time = time.time()
    output_file = open('fbpost_pandas_summary_output.txt', 'w', encoding='utf-8')
    sys.stdout = output_file
    try:
        print("Loading dataset...")
        load_start = time.time()
        df = pd.read_csv('2024_fb_posts_president_scored_anon.csv')
        load_time = time.time() - load_start
        print(f"Dataset loaded in {load_time:.2f} seconds. Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}\n")

        # Unpack JSON columns if present
        json_columns = [col for col in ['delivery_by_region', 'demographic_distribution'] if col in df.columns]
        unpack_start = time.time()
        for col in json_columns:
            df = unpack_json_column(df, col)
        unpack_time = time.time() - unpack_start
        print(f"JSON unpacking completed in {unpack_time:.2f} seconds. New shape: {df.shape}\n")

        print("========== OVERALL DATASET SUMMARY ==========")
        print(f"Total rows: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        print(f"Columns: {sorted(df.columns.tolist())}\n")

        # Overall describe
        print("--- DESCRIBE (include='all') ---")
        print(df.describe(include='all').transpose())
        print()

        # Non-numeric columns: value_counts, nunique, most frequent
        object_cols = df.select_dtypes(include=['object']).columns.tolist()
        for col in object_cols:
            print(f"--- Column: {col} ---")
            non_null = df[col].notna().sum()
            nunique = df[col].nunique(dropna=True)
            vc = df[col].value_counts(dropna=True)
            most_freq = vc.idxmax() if not vc.empty else None
            most_freq_count = vc.max() if not vc.empty else 0
            print(f"  Non-null count: {non_null}")
            print(f"  Unique values: {nunique}")
            print(f"  Most frequent: '{most_freq}' ({most_freq_count} times)")
            print()

        # Grouped by page_id
        if 'page_id' in df.columns:
            print("========== GROUPED BY page_id ==========")
            page_groups = df.groupby('page_id')
            n_groups = page_groups.ngroups
            print(f"Number of unique page_ids: {n_groups}")
            for i, (page_id, group) in enumerate(page_groups):
                if i >= 5:
                    print(f"... and {n_groups - 5} more groups")
                    break
                print(f"--- Page ID: {page_id} ---")
                print(f"Number of posts: {len(group)}")
                print(group.describe(include='all').transpose())
                print()

        # Grouped by page_id + post_id
        if 'page_id' in df.columns and 'post_id' in df.columns:
            print("========== GROUPED BY (page_id, post_id) ==========")
            page_post_groups = df.groupby(['page_id', 'post_id'])
            n_groups2 = page_post_groups.ngroups
            print(f"Number of unique (page_id, post_id) combinations: {n_groups2}")
            for i, ((page_id, post_id), group) in enumerate(page_post_groups):
                if i >= 3:
                    print(f"... and {n_groups2 - 3} more groups")
                    break
                print(f"--- Page ID: {page_id}, Post ID: {post_id} ---")
                print(f"Number of records: {len(group)}")
                print(group.describe(include='all').transpose())
                print()

        total_time = time.time() - start_time
        print("========== TIMING SUMMARY ==========")
        print(f"Data loading: {load_time:.2f} seconds")
        print(f"JSON unpacking: {unpack_time:.2f} seconds")
        print(f"Total execution time: {total_time:.2f} seconds")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        sys.stdout = sys.__stdout__
        output_file.close()
        print("\n✅ All pandas stats written to fbpost_pandas_summary_output.txt")

if __name__ == "__main__":
    main() 