import pandas as pd
import time
import sys

def clean_booleans(df, bool_cols):
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().map({'true': True, 'false': False})
    return df

def main():
    start_time = time.time()
    output_file = open('tw_post_pandas_summary_output.txt', 'w', encoding='utf-8')
    sys.stdout = output_file
    try:
        load_start = time.time()
        df = pd.read_csv('2024_tw_posts_president_scored_anon.csv', dtype=str)
        load_time = time.time() - load_start
        print(f"Loaded {len(df)} rows, {len(df.columns)} columns in {load_time:.2f} seconds.")
        print(f"Columns: {list(df.columns)}")
        # Data cleaning
        numeric_cols = ['retweetCount', 'replyCount', 'likeCount', 'quoteCount', 'viewCount', 'bookmarkCount']
        bool_cols = ['isReply', 'isRetweet', 'isQuote']
        cat_cols = ['source', 'lang', 'month_year']
        text_cols = ['illuminating_scored_message']
        all_focus = numeric_cols + cat_cols + bool_cols + text_cols
        # Convert numeric columns
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        # Clean booleans
        df = clean_booleans(df, bool_cols)
        # Parse dates
        if 'createdAt' in df.columns:
            df['createdAt'] = pd.to_datetime(df['createdAt'], errors='coerce')
        # Handle missing values
        df = df.replace({'': pd.NA, 'nan': pd.NA, None: pd.NA})
        print("\n===== OVERALL SUMMARY =====")
        for col in df.columns:
            print(f"Column: {col}")
            col_data = df[col]
            non_null = col_data.notna().sum()
            print(f"  count: {len(col_data)}")
            print(f"  non_null: {non_null}")
            if col in numeric_cols:
                print(f"  mean: {col_data.mean(skipna=True)}")
                print(f"  min: {col_data.min(skipna=True)}")
                print(f"  max: {col_data.max(skipna=True)}")
                print(f"  std: {col_data.std(skipna=True)}")
            elif col in bool_cols:
                true_count = (col_data == True).sum()
                false_count = (col_data == False).sum()
                print(f"  true_count: {true_count}")
                print(f"  false_count: {false_count}")
            else:
                nunique = col_data.nunique(dropna=True)
                vc = col_data.value_counts(dropna=True)
                most_freq = vc.idxmax() if not vc.empty else None
                most_freq_count = vc.max() if not vc.empty else 0
                print(f"  unique: {nunique}")
                print(f"  most_frequent: {most_freq}")
                print(f"  most_frequent_count: {most_freq_count}")
            print()
        # Groupby source
        group_start = time.time()
        if 'source' in df.columns:
            print(f"\n===== GROUPED BY source =====")
            groups = df.groupby('source')
            print(f"Number of groups: {df['source'].nunique(dropna=True)}")
            for i, (gkey, group) in enumerate(groups):
                if i >= 5:
                    print(f"... and {df['source'].nunique(dropna=True) - 5} more groups")
                    break
                print(f"\n--- source: {gkey} ---")
                print(f"Records: {len(group)}")
                for col in all_focus:
                    if col in group.columns:
                        col_data = group[col]
                        non_null = col_data.notna().sum()
                        print(f"  {col}:")
                        print(f"    count: {len(col_data)}")
                        print(f"    non_null: {non_null}")
                        if col in numeric_cols:
                            print(f"    mean: {col_data.mean(skipna=True)}")
                            print(f"    min: {col_data.min(skipna=True)}")
                            print(f"    max: {col_data.max(skipna=True)}")
                            print(f"    std: {col_data.std(skipna=True)}")
                        elif col in bool_cols:
                            true_count = (col_data == True).sum()
                            false_count = (col_data == False).sum()
                            print(f"    true_count: {true_count}")
                            print(f"    false_count: {false_count}")
                        else:
                            nunique = col_data.nunique(dropna=True)
                            vc = col_data.value_counts(dropna=True)
                            most_freq = vc.idxmax() if not vc.empty else None
                            most_freq_count = vc.max() if not vc.empty else 0
                            print(f"    unique: {nunique}")
                            print(f"    most_frequent: {most_freq}")
                            print(f"    most_frequent_count: {most_freq_count}")
                print()
        group_time = time.time() - group_start
        print(f"\nGroupby analysis completed in {group_time:.2f} seconds.")
        total_time = time.time() - start_time
        print(f"\n===== TIMING =====")
        print(f"Data loading: {load_time:.2f} seconds")
        print(f"Groupby: {group_time:.2f} seconds")
        print(f"Total execution time: {total_time:.2f} seconds")
        print("\n# Performance: Pandas is typically much faster than pure Python for large datasets due to vectorized operations.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        sys.stdout = sys.__stdout__
        output_file.close()
        print("\n✅ All pandas stats written to tw_post_pandas_summary_output.txt")

if __name__ == "__main__":
    main() 