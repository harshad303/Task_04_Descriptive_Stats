import pandas as pd
import json
import sys
from pathlib import Path

# Redirect output to file
output_file = open('pandas_summary_output.txt', 'w', encoding='utf-8')
sys.stdout = output_file

try:
    # === STEP 1: Load Dataset ===
    print("Loading dataset...")
    csv_path = "../2024_fb_ads/2024_fb_ads_president_scored_anon.csv"
    df = pd.read_csv(csv_path)
    print(f"Dataset loaded successfully. Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print()

    # === STEP 2: Unpack JSON Columns ===
    print("Unpacking JSON columns...")
    json_columns = ['delivery_by_region', 'demographic_distribution']
    
    def safe_json_loads(value):
        """Safely parse JSON strings, handling malformed or empty values."""
        if pd.isna(value) or value == '':
            return None
        try:
            # Replace single quotes with double quotes for valid JSON
            if isinstance(value, str):
                value = value.replace("'", '"')
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return None
    
    def unpack_json_column(df, column_name):
        """Unpack a JSON column into flat columns."""
        print(f"Processing {column_name}...")
        
        # Parse JSON safely
        parsed_data = df[column_name].apply(safe_json_loads)
        
        # Collect all new columns
        new_columns = {}
        
        for idx, parsed in enumerate(parsed_data):
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
            new_df = pd.DataFrame(new_columns, index=df.index)
            # Concatenate with original dataframe (excluding the JSON column)
            df = pd.concat([df.drop(columns=[column_name]), new_df], axis=1)
        else:
            # Just remove the original JSON column if no new columns were created
            df = df.drop(columns=[column_name])
        
        return df
    
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
    
    # Separate numeric and non-numeric columns
    numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
    object_columns = df.select_dtypes(include=['object']).columns.tolist()
    
    print(f"Numeric columns ({len(numeric_columns)}): {numeric_columns}")
    print(f"Object columns ({len(object_columns)}): {object_columns}")
    print()
    
    # Descriptive statistics for numeric columns
    if len(numeric_columns) > 0:
        print("--- NUMERIC COLUMNS DESCRIPTIVE STATISTICS ---")
        numeric_df = df[numeric_columns].describe()
        print(numeric_df)
        print()
    
    # Descriptive statistics for object columns
    if len(object_columns) > 0:
        print("--- OBJECT COLUMNS DESCRIPTIVE STATISTICS ---")
        for col in sorted(object_columns):
            if col in df.columns:  # Check if column still exists
                non_null_count = df[col].notna().sum()
                if non_null_count > 0:  # Skip columns with all nulls
                    unique_count = df[col].nunique()
                    most_frequent = df[col].value_counts().idxmax()
                    most_frequent_count = df[col].value_counts().max()
                    
                    print(f"Column: {col}")
                    print(f"  Non-null count: {non_null_count}")
                    print(f"  Unique values: {unique_count}")
                    print(f"  Most frequent: '{most_frequent}' (appears {most_frequent_count} times)")
                    print()
    
    # === STEP 4: Grouped Analysis by page_id ===
    print("========== GROUPED BY page_id ==========")
    if 'page_id' in df.columns:
        page_groups = df.groupby('page_id')
        print(f"Number of unique page_ids: {df['page_id'].nunique()}")
        print()
        
        # Limit to first few groups to avoid overwhelming output
        for i, (page_id, group) in enumerate(page_groups):
            if i >= 5:  # Limit to first 5 groups
                print(f"... and {len(page_groups) - 5} more groups")
                break
                
            print(f"--- Page ID: {page_id} ---")
            print(f"Number of ads: {len(group)}")
            
            # Numeric summary for this group
            if len(numeric_columns) > 0:
                try:
                    group_numeric = group[numeric_columns].describe()
                    print("Numeric columns summary:")
                    print(group_numeric)
                    print()
                except Exception as e:
                    print(f"Error in numeric summary: {e}")
                    print()
            
            # Object summary for this group
            if len(object_columns) > 0:
                print("Object columns summary:")
                for col in sorted(object_columns):
                    try:
                        if col in group.columns and group[col].notna().sum() > 0:
                            unique_count = group[col].nunique()
                            most_frequent = group[col].value_counts().idxmax()
                            most_frequent_count = group[col].value_counts().max()
                            
                            print(f"  {col}: {unique_count} unique values, most frequent: '{most_frequent}' ({most_frequent_count} times)")
                    except Exception as e:
                        print(f"  {col}: Error processing - {e}")
                print()
    
    # === STEP 5: Grouped Analysis by (page_id, ad_id) ===
    print("========== GROUPED BY (page_id, ad_id) ==========")
    if 'page_id' in df.columns and 'ad_id' in df.columns:
        page_ad_groups = df.groupby(['page_id', 'ad_id'])
        print(f"Number of unique (page_id, ad_id) combinations: {len(page_ad_groups)}")
        print()
        
        # Show first few groups to avoid overwhelming output
        for i, ((page_id, ad_id), group) in enumerate(page_ad_groups):
            if i >= 3:  # Limit to first 3 groups
                print(f"... and {len(page_ad_groups) - 3} more groups")
                break
                
            print(f"--- Page ID: {page_id}, Ad ID: {ad_id} ---")
            print(f"Number of records: {len(group)}")
            
            # Numeric summary for this group
            if len(numeric_columns) > 0:
                try:
                    group_numeric = group[numeric_columns].describe()
                    print("Numeric columns summary:")
                    print(group_numeric)
                    print()
                except Exception as e:
                    print(f"Error in numeric summary: {e}")
                    print()
            
            # Object summary for this group
            if len(object_columns) > 0:
                print("Object columns summary:")
                for col in sorted(object_columns):
                    try:
                        if col in group.columns and group[col].notna().sum() > 0:
                            unique_count = group[col].nunique()
                            most_frequent = group[col].value_counts().idxmax()
                            most_frequent_count = group[col].value_counts().max()
                            
                            print(f"  {col}: {unique_count} unique values, most frequent: '{most_frequent}' ({most_frequent_count} times)")
                    except Exception as e:
                        print(f"  {col}: Error processing - {e}")
                print()

except Exception as e:
    print(f"Error occurred: {str(e)}")
    import traceback
    traceback.print_exc()

finally:
    # Close output file and restore normal print output
    sys.stdout.close()
    sys.stdout = sys.__stdout__
    print("✅ All pandas stats written to pandas_summary_output.txt") 