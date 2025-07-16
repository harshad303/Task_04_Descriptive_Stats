import csv
import time
import math
from collections import Counter, defaultdict
from typing import Any, Dict, List, Tuple, Union

def is_missing(val):
    return val is None or val == '' or str(val).lower() == 'nan'

def try_parse_number(val):
    try:
        if '.' in val:
            return float(val)
        else:
            return int(val)
    except Exception:
        return None

def try_parse_bool(val):
    if str(val).lower() in ['true', 'false']:
        return str(val).lower() == 'true'
    return None

def clean_value(val):
    if is_missing(val):
        return None
    val = val.strip()
    # Try bool
    b = try_parse_bool(val)
    if b is not None:
        return b
    # Try number
    n = try_parse_number(val)
    if n is not None:
        return n
    return val

def analyze_column(col_values: List[Any]) -> Dict[str, Any]:
    non_nulls = [v for v in col_values if v is not None]
    result = {'count': len(col_values), 'non_null': len(non_nulls)}
    if not non_nulls:
        return result
    # Numeric
    if all(isinstance(v, (int, float)) for v in non_nulls):
        mean = sum(non_nulls) / len(non_nulls)
        minv = min(non_nulls)
        maxv = max(non_nulls)
        std = math.sqrt(sum((x - mean) ** 2 for x in non_nulls) / (len(non_nulls) - 1)) if len(non_nulls) > 1 else 0
        result.update({'mean': mean, 'min': minv, 'max': maxv, 'std': std})
    # Boolean
    elif all(isinstance(v, bool) for v in non_nulls):
        counts = Counter(non_nulls)
        result.update({'true_count': counts[True], 'false_count': counts[False]})
    # String/categorical
    else:
        str_vals = [str(v) for v in non_nulls]
        counts = Counter(str_vals)
        most_common = counts.most_common(1)[0] if counts else (None, 0)
        result.update({'unique': len(counts), 'most_frequent': most_common[0], 'most_frequent_count': most_common[1]})
    return result

def analyze_groupby(rows: List[Dict[str, Any]], group_col: str, columns: List[str], focus_cols: List[str], output_func):
    # Group rows
    groups = defaultdict(list)
    for row in rows:
        key = row.get(group_col)
        groups[key].append(row)
    output_func(f"\n===== GROUPED BY {group_col} =====")
    output_func(f"Number of groups: {len(groups)}")
    for i, (gkey, grows) in enumerate(groups.items()):
        if i >= 5:
            output_func(f"... and {len(groups) - 5} more groups")
            break
        output_func(f"\n--- {group_col}: {gkey} ---")
        output_func(f"Records: {len(grows)}")
        for col in focus_cols:
            col_vals = [row.get(col) for row in grows]
            stats = analyze_column(col_vals)
            output_func(f"  {col}: {stats}")

def main():
    start_time = time.time()
    output_file = open('tw_post_pure_python_summary_output.txt', 'w', encoding='utf-8')
    def out(s):
        print(s)
        output_file.write(str(s) + '\n')
    try:
        load_start = time.time()
        with open('2024_tw_posts_president_scored_anon.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames
            rows = []
            for row in reader:
                clean_row = {k: clean_value(v) for k, v in row.items()}
                rows.append(clean_row)
        load_time = time.time() - load_start
        out(f"Loaded {len(rows)} rows, {len(columns)} columns in {load_time:.2f} seconds.")
        out(f"Columns: {columns}")
        # Focus columns
        numeric_cols = ['retweetCount', 'replyCount', 'likeCount', 'quoteCount', 'viewCount', 'bookmarkCount']
        cat_cols = ['source', 'lang', 'month_year']
        bool_cols = ['isReply', 'isRetweet', 'isQuote']
        text_cols = ['illuminating_scored_message']
        all_focus = numeric_cols + cat_cols + bool_cols + text_cols
        # Overall stats
        out("\n===== OVERALL SUMMARY =====")
        for col in columns:
            col_vals = [row.get(col) for row in rows]
            stats = analyze_column(col_vals)
            out(f"Column: {col}")
            for k, v in stats.items():
                out(f"  {k}: {v}")
            out("")
        # Groupby source
        group_start = time.time()
        analyze_groupby(rows, 'source', columns, all_focus, out)
        group_time = time.time() - group_start
        out(f"\nGroupby analysis completed in {group_time:.2f} seconds.")
        total_time = time.time() - start_time
        out(f"\n===== TIMING =====")
        out(f"Data loading: {load_time:.2f} seconds")
        out(f"Groupby: {group_time:.2f} seconds")
        out(f"Total execution time: {total_time:.2f} seconds")
    except Exception as e:
        out(f"Error: {e}")
        import traceback
        traceback.print_exc(file=output_file)
    finally:
        output_file.close()
        print("\n✅ All results written to tw_post_pure_python_summary_output.txt")

if __name__ == "__main__":
    main() 