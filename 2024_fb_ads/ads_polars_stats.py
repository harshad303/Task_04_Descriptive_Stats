import polars as pl
import json
import sys
import argparse

def most_frequent_value(df, col):
    """Get the most frequent value and its count for a column."""
    try:
        vc = df[col].value_counts().to_pandas()
        if len(vc) > 0 and vc.shape[0] > 0:
            value = vc.iloc[0, 0]  # First column is the value
            count = vc.iloc[0, 1]  # Second column is the count
            return f"{value} ({count} times)"
        return "No data"
    except Exception as e:
        return f"Error: {e}"

def safe_json_loads(value):
    """Safely parse JSON strings, handling malformed or empty values."""
    if value is None or value == '':
        return None
    try:
        # Replace single quotes with double quotes for valid JSON
        if isinstance(value, str):
            value = value.replace("'", '"')
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None

def unpack_json_column(df, column_name):
    """Unpack a JSON column into flat columns using Polars."""
    print(f"Processing {column_name}...")
    
    # Parse JSON safely using apply
    parsed_data = df.select(
        pl.col(column_name).map_elements(safe_json_loads, return_dtype=pl.Object).alias("parsed")
    )
    
    # Collect all new columns
    new_columns = {}
    
    # Get the parsed data as a list
    parsed_list = parsed_data["parsed"].to_list()
    
    for idx, parsed in enumerate(parsed_list):
        if parsed is None or not isinstance(parsed, dict):
            continue
            
        for key, value in parsed.items():
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
    
    # Create new dataframe with unpacked columns
    if new_columns:
        new_df = pl.DataFrame(new_columns)
        # Concatenate with original dataframe (excluding the JSON column)
        df = pl.concat([df.drop(column_name), new_df], how="horizontal")
    else:
        # Just remove the original JSON column if no new columns were created
        df = df.drop(column_name)
    
    return df

def describe_numeric_columns(df):
    """Generate descriptive statistics for numeric columns."""
    # Get numeric columns by checking dtypes
    numeric_cols = [col for col in df.columns if df.schema[col] in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]]
    if numeric_cols:
        print("--- NUMERIC COLUMNS DESCRIPTIVE STATISTICS ---")
        numeric_df = df.select(numeric_cols).describe()
        print(numeric_df)
        print()
    return numeric_cols

def describe_string_columns(df):
    """Generate descriptive statistics for string columns."""
    # Get string columns by checking dtypes
    string_cols = [col for col in df.columns if df.schema[col] == pl.Utf8]
    if string_cols:
        print("--- STRING COLUMNS DESCRIPTIVE STATISTICS ---")
        for col in sorted(string_cols):
            if col in df.columns:
                non_null_count = df.select(pl.col(col).is_not_null().sum()).item()
                if non_null_count > 0:  # Skip columns with all nulls
                    unique_count = df.select(pl.col(col).n_unique()).item()
                    most_frequent = most_frequent_value(df, col)
                    
                    print(f"--- Column: {col} ---")
                    print(f"Unique Values: {unique_count}")
                    print(f"Most Frequent: {most_frequent}")
                    print()
    return string_cols

def grouped_analysis(df, group_cols, title, limit_groups=5):
    """Perform grouped analysis on specified columns."""
    print(f"========== {title} ==========")
    
    if not all(col in df.columns for col in group_cols):
        print(f"Warning: Some group columns {group_cols} not found in dataset")
        return
    
    # Get unique groups
    unique_groups = df.select(group_cols).unique()
    print(f"Number of unique {title.lower()}: {len(unique_groups)}")
    print()
    
    # Perform grouped analysis
    grouped = df.group_by(group_cols)
    
    # Get numeric and string columns
    numeric_cols = [col for col in df.columns if df.schema[col] in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]]
    string_cols = [col for col in df.columns if df.schema[col] == pl.Utf8]
    
    # Show first few groups
    group_count = 0
    for group_key, group_df in grouped:
        if group_count >= limit_groups:
            #remaining = len(grouped) - limit_groups
            #print(f"... and {remaining} more groups")
            break
            
        print(f"--- Group: {group_key} ---")
        print(f"Number of records: {len(group_df)}")
        
        # Numeric summary for this group
        if numeric_cols:
            try:
                group_numeric = group_df.select(numeric_cols).describe()
                print("Numeric columns summary:")
                print(group_numeric)
                print()
            except Exception as e:
                print(f"Error in numeric summary: {e}")
                print()
        
        # String summary for this group
        if string_cols:
            print("String columns summary:")
            printed = False
            for col in sorted(string_cols):
                try:
                    if col in group_df.columns:
                        non_null_count = group_df.select(pl.col(col).is_not_null().sum()).item()
                        if non_null_count > 0:
                            unique_count = group_df.select(pl.col(col).n_unique()).item()
                            value_counts = group_df.select(pl.col(col).value_counts()).to_pandas()
                            if len(value_counts) > 0 and value_counts.shape[0] > 0:
                                first_row = value_counts.iloc[0]
                                if len(first_row) >= 2:
                                    most_frequent = first_row.iloc[0]  # First column is the value
                                    most_frequent_count = first_row.iloc[1]  # Second column is the count
                                    print(f"  {col}: {unique_count} unique values, most frequent: '{most_frequent}' ({most_frequent_count} times)")
                                    printed = True
                except Exception as e:
                    print(f"  {col}: Error processing - {e}")
            if not printed:
                print("  (No non-null string columns in this group)")
            print()
        
        group_count += 1

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Analyze Facebook Ads dataset using Polars')
    parser.add_argument('--sample', type=int, help='Limit number of rows for testing')
    args = parser.parse_args()
    
    # Redirect output to file
    output_file = open('polars_summary_output.txt', 'w', encoding='utf-8')
    sys.stdout = output_file
    
    try:
        # === STEP 1: Load Dataset ===
        print("Loading dataset...")
        csv_path = "../2024_fb_ads/2024_fb_ads_president_scored_anon.csv"
        
        # Load with optional sampling
        if args.sample:
            df = pl.read_csv(csv_path).head(args.sample)
            print(f"Loaded sample of {args.sample} rows")
        else:
            df = pl.read_csv(csv_path)
        
        print(f"Dataset loaded successfully. Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print()

        # === STEP 2: Unpack JSON Columns ===
        print("Unpacking JSON columns...")
        json_columns = ['delivery_by_region', 'demographic_distribution']
        
        # Unpack each JSON column
        for col in json_columns:
            if col in df.columns:
                df = unpack_json_column(df, col)
        
        print(f"JSON unpacking complete. New shape: {df.shape}")
        print()

        # === STEP 3: Overall Dataset Summary ===
        print("========== OVERALL DATASET SUMMARY ==========")
        print(f"Dataset shape: {df.shape}")
        print(f"Total rows: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        print()
        
        # Sort columns alphabetically
        sorted_columns = sorted(df.columns)
        print(f"All columns (sorted): {sorted_columns}")
        print()
        
        # Descriptive statistics for numeric columns
        numeric_cols = describe_numeric_columns(df)
        
        # Descriptive statistics for string columns
        string_cols = describe_string_columns(df)
        
        # === STEP 4: Grouped Analysis by page_id ===
        grouped_analysis(df, ['page_id'], "GROUPED BY page_id", limit_groups=5)
        
        # === STEP 5: Grouped Analysis by (page_id, ad_id) ===
        grouped_analysis(df, ['page_id', 'ad_id'], "GROUPED BY (page_id, ad_id)", limit_groups=3)

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        # Close output file and restore normal print output
        sys.stdout.close()
        sys.stdout = sys.__stdout__
        print("✅ All polars stats written to polars_summary_output.txt")

if __name__ == "__main__":
    main() 