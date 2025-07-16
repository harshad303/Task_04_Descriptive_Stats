import polars as pl
import json
import time
import sys

def safe_json_loads(value):
    if value is None or value == '':
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
    for idx, parsed_val in enumerate(parsed):
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
        new_df = pl.DataFrame(new_columns)
        df = df.with_columns([new_df[col] for col in new_df.columns])
        df = df.drop(column_name)
    else:
        df = df.drop(column_name)
    return df

def most_frequent_value(df, col):
    try:
        vc = df[col].value_counts().to_pandas()
        if len(vc) > 0 and vc.shape[0] > 0:
            value = vc.iloc[0, 0]
            count = vc.iloc[0, 1]
            return f"{value} ({count} times)"
        return "No data"
    except Exception as e:
        return f"Error: {e}"

def main():
    start_time = time.time()
    output_file = open('fbpost_polars_summary_output.txt', 'w', encoding='utf-8')
    sys.stdout = output_file
    try:
        print("Loading dataset...")
        load_start = time.time()
        df = pl.read_csv('2024_fb_posts_president_scored_anon.csv', null_values=['', ' '], ignore_errors=True)
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
        print(f"Total rows: {df.height}")
        print(f"Total columns: {df.width}")
        print(f"Columns: {sorted(df.columns)}\n")

        # Numeric columns
        numeric_cols = [col for col, dtype in df.schema.items() if dtype in [pl.Int64, pl.Float64, pl.Int32, pl.Float32]]
        string_cols = [col for col, dtype in df.schema.items() if dtype == pl.Utf8]

        if numeric_cols:
            print("NUMERIC COLUMNS:")
            print("-" * 30)
            desc = df.select(numeric_cols).describe()
            print(desc)
            print()

        if string_cols:
            print("STRING COLUMNS:")
            print("-" * 30)
            for col in string_cols:
                non_null = df[col].drop_nulls().len()
                nunique = df[col].n_unique()
                mf = most_frequent_value(df, col)
                print(f"Column: {col}")
                print(f"  Non-null count: {non_null}")
                print(f"  Unique values: {nunique}")
                print(f"  Most frequent: {mf}")
                print()

        # Grouped by page_id
        if 'page_id' in df.columns:
            print("========== GROUPED BY page_id ==========")
            n_groups = df['page_id'].n_unique()
            print(f"Number of unique page_ids: {n_groups}")
            groups = df.groupby('page_id').groups
            for i, (page_id, idxs) in enumerate(groups.items()):
                if i >= 5:
                    print(f"... and {n_groups - 5} more groups")
                    break
                group = df.take(idxs)
                print(f"--- Page ID: {page_id} ---")
                print(f"Number of posts: {group.height}")
                if numeric_cols:
                    print("Numeric columns summary:")
                    print(group.select(numeric_cols).describe())
                if string_cols:
                    print("String columns summary:")
                    for col in string_cols[:3]:
                        non_null = group[col].drop_nulls().len()
                        nunique = group[col].n_unique()
                        mf = most_frequent_value(group, col)
                        print(f"  {col}: {nunique} unique, most frequent: {mf}")
                print()

        # Grouped by page_id + post_id
        if 'page_id' in df.columns and 'post_id' in df.columns:
            print("========== GROUPED BY (page_id, post_id) ==========")
            n_groups2 = df.select([pl.col('page_id'), pl.col('post_id')]).n_unique()
            print(f"Number of unique (page_id, post_id) combinations: {n_groups2}")
            groups2 = df.groupby(['page_id', 'post_id']).groups
            for i, (keys, idxs) in enumerate(groups2.items()):
                if i >= 3:
                    print(f"... and {n_groups2 - 3} more groups")
                    break
                group = df.take(idxs)
                print(f"--- Page ID: {keys[0]}, Post ID: {keys[1]} ---")
                print(f"Number of records: {group.height}")
                if numeric_cols:
                    print("Numeric columns summary:")
                    print(group.select(numeric_cols).describe())
                if string_cols:
                    print("String columns summary:")
                    for col in string_cols[:2]:
                        non_null = group[col].drop_nulls().len()
                        nunique = group[col].n_unique()
                        mf = most_frequent_value(group, col)
                        print(f"  {col}: {nunique} unique, most frequent: {mf}")
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
        print("\n✅ All polars stats written to fbpost_polars_summary_output.txt")

if __name__ == "__main__":
    main() 