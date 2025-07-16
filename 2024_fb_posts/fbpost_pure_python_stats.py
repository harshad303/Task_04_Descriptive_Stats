#!/usr/bin/env python3
"""
Pure Python Facebook Posts Data Analysis Script
Uses only standard library modules: csv, json, statistics, time, collections
"""

import csv
import json
import statistics
import time
from collections import defaultdict, Counter
from typing import Dict, List, Any, Union, Tuple

def safe_json_loads(value: str) -> Union[Dict, List, None]:
    """Safely parse JSON strings, handling malformed or empty values."""
    if not value or value.strip() == '':
        return None
    try:
        # Replace single quotes with double quotes for valid JSON
        value = value.replace("'", '"')
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None

def clean_value(value: str) -> Union[str, float, int, None]:
    """Clean and convert values appropriately."""
    if not value or value.strip() == '':
        return None
    
    value = value.strip()
    
    # Try to convert to number
    try:
        if '.' in value:
            return float(value)
        else:
            return int(value)
    except ValueError:
        return value

def is_numeric(value: Any) -> bool:
    """Check if a value is numeric."""
    return isinstance(value, (int, float)) and value is not None

def unpack_json_column(data: List[Dict], column_name: str) -> Tuple[List[Dict], List[str]]:
    """Unpack a JSON column into flat columns."""
    print(f"Processing {column_name}...")
    
    new_columns = []
    all_keys = set()
    
    # First pass: collect all possible keys
    for row in data:
        if column_name in row:
            parsed = safe_json_loads(row[column_name])
            if isinstance(parsed, dict):
                for key, value in parsed.items():
                    if isinstance(value, dict):
                        for subkey in value.keys():
                            all_keys.add(f"{key}_{subkey}")
                    else:
                        all_keys.add(key)
    
    # Second pass: add new columns to each row
    for row in data:
        if column_name in row:
            parsed = safe_json_loads(row[column_name])
            if isinstance(parsed, dict):
                for key, value in parsed.items():
                    if isinstance(value, dict):
                        for subkey, subvalue in value.items():
                            col_name = f"{column_name}_{key}_{subkey}"
                            row[col_name] = clean_value(str(subvalue))
                    else:
                        col_name = f"{column_name}_{key}"
                        row[col_name] = clean_value(str(value))
        
        # Remove original JSON column
        if column_name in row:
            del row[column_name]
    
    return data, list(all_keys)

def calculate_numeric_stats(values: List[Union[int, float]]) -> Dict[str, Union[int, float, None]]:
    """Calculate numeric statistics."""
    if not values:
        return {"count": 0, "mean": None, "min": None, "max": None, "std": None}
    
    return {
        "count": len(values),
        "mean": statistics.mean(values),
        "min": min(values),
        "max": max(values),
        "std": statistics.stdev(values) if len(values) > 1 else 0
    }

def calculate_string_stats(values: List[str]) -> Dict[str, Any]:
    """Calculate string statistics."""
    if not values:
        return {"count": 0, "unique": 0, "most_frequent": None, "most_frequent_count": 0}
    
    counter = Counter(values)
    most_common = counter.most_common(1)[0] if counter else (None, 0)
    
    return {
        "count": len(values),
        "unique": len(counter),
        "most_frequent": most_common[0],
        "most_frequent_count": most_common[1]
    }

def analyze_column(data: List[Dict], column_name: str) -> Dict[str, Any]:
    """Analyze a single column."""
    values = [row.get(column_name) for row in data if row.get(column_name) is not None]
    
    if not values:
        return {"type": "empty", "count": 0}
    
    # Check if column is numeric
    numeric_values = [v for v in values if is_numeric(v)]
    if len(numeric_values) == len(values):
        # Cast to proper type for numeric analysis
        numeric_values_typed: List[Union[int, float]] = [v for v in numeric_values if isinstance(v, (int, float))]
        return {"type": "numeric", **calculate_numeric_stats(numeric_values_typed)}
    else:
        # Convert all values to strings for string analysis
        string_values = [str(v) for v in values]
        return {"type": "string", **calculate_string_stats(string_values)}

def analyze_grouped_data(data: List[Dict], group_columns: List[str]) -> Dict[str, Dict]:
    """Analyze data grouped by specified columns."""
    groups = defaultdict(list)
    
    # Group data
    for row in data:
        group_key = tuple(row.get(col) for col in group_columns)
        groups[group_key].append(row)
    
    # Analyze each group
    group_analyses = {}
    for group_key, group_data in groups.items():
        group_name = " + ".join(str(key) for key in group_key)
        group_analyses[group_name] = {
            "count": len(group_data),
            "columns": {}
        }
        
        # Analyze each column in the group
        if group_data:
            all_columns = set()
            for row in group_data:
                all_columns.update(row.keys())
            
            for col in sorted(all_columns):
                group_analyses[group_name]["columns"][col] = analyze_column(group_data, col)
    
    return group_analyses

def main():
    """Main analysis function."""
    start_time = time.time()
    
    # Redirect output to file
    output_file = open('fbpost_pure_python_summary_output.txt', 'w', encoding='utf-8')
    
    # Redirect print to file
    import sys
    original_stdout = sys.stdout
    sys.stdout = output_file
    
    try:
        print("Loading Facebook posts dataset...")
        load_start = time.time()
        
        # Load CSV data
        data = []
        with open('2024_fb_posts_president_scored_anon.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Clean all values
                cleaned_row = {}
                for key, value in row.items():
                    cleaned_row[key] = clean_value(value)
                data.append(cleaned_row)
        
        load_time = time.time() - load_start
        print(f"Dataset loaded in {load_time:.2f} seconds")
        print(f"Total rows: {len(data)}")
        
        # Identify JSON columns
        json_columns = []
        if data:
            sample_row = data[0]
            for col in sample_row.keys():
                if col in ['delivery_by_region', 'demographic_distribution']:
                    json_columns.append(col)
        
        # Unpack JSON columns
        unpack_start = time.time()
        for col in json_columns:
            data, new_cols = unpack_json_column(data, col)
            print(f"Unpacked {col} into {len(new_cols)} new columns")
        
        unpack_time = time.time() - unpack_start
        print(f"JSON unpacking completed in {unpack_time:.2f} seconds")
        
        # Overall dataset analysis
        print("\n" + "="*50)
        print("OVERALL DATASET SUMMARY")
        print("="*50)
        
        if data:
            all_columns = set()
            for row in data:
                all_columns.update(row.keys())
            
            print(f"Total rows: {len(data)}")
            print(f"Total columns: {len(all_columns)}")
            print(f"Columns: {sorted(all_columns)}")
            print()
            
            # Analyze each column
            numeric_columns = []
            string_columns = []
            
            for col in sorted(all_columns):
                analysis = analyze_column(data, col)
                if analysis["type"] == "numeric":
                    numeric_columns.append(col)
                elif analysis["type"] == "string":
                    string_columns.append(col)
            
            # Print numeric column summaries
            if numeric_columns:
                print("NUMERIC COLUMNS:")
                print("-" * 30)
                for col in numeric_columns:
                    analysis = analyze_column(data, col)
                    stats = analysis
                    print(f"Column: {col}")
                    print(f"  Count: {stats['count']}")
                    print(f"  Mean: {stats['mean']:.2f}")
                    print(f"  Min: {stats['min']}")
                    print(f"  Max: {stats['max']}")
                    print(f"  Std: {stats['std']:.2f}")
                    print()
            
            # Print string column summaries
            if string_columns:
                print("STRING COLUMNS:")
                print("-" * 30)
                for col in string_columns:
                    analysis = analyze_column(data, col)
                    stats = analysis
                    print(f"Column: {col}")
                    print(f"  Count: {stats['count']}")
                    print(f"  Unique values: {stats['unique']}")
                    print(f"  Most frequent: '{stats['most_frequent']}' ({stats['most_frequent_count']} times)")
                    print()
        
        # Grouped analysis by page_id
        print("\n" + "="*50)
        print("GROUPED BY page_id")
        print("="*50)
        
        group_start = time.time()
        page_groups = analyze_grouped_data(data, ['page_id'])
        
        # Show first few groups
        group_count = 0
        for group_name, group_analysis in page_groups.items():
            if group_count >= 5:  # Limit output
                remaining = len(page_groups) - 5
                print(f"... and {remaining} more groups")
                break
            
            print(f"\nGroup: {group_name}")
            print(f"Records: {group_analysis['count']}")
            
            # Show numeric columns for this group
            numeric_cols = []
            string_cols = []
            for col, analysis in group_analysis['columns'].items():
                if analysis['type'] == 'numeric':
                    numeric_cols.append(col)
                elif analysis['type'] == 'string':
                    string_cols.append(col)
            
            if numeric_cols:
                print("  Numeric columns:")
                for col in numeric_cols[:3]:  # Limit to first 3
                    analysis = group_analysis['columns'][col]
                    print(f"    {col}: mean={analysis['mean']:.2f}, min={analysis['min']}, max={analysis['max']}")
            
            if string_cols:
                print("  String columns:")
                for col in string_cols[:3]:  # Limit to first 3
                    analysis = group_analysis['columns'][col]
                    print(f"    {col}: {analysis['unique']} unique, most frequent='{analysis['most_frequent']}' ({analysis['most_frequent_count']} times)")
            
            group_count += 1
        
        group_time = time.time() - group_start
        print(f"\nGrouped analysis completed in {group_time:.2f} seconds")
        
        # Grouped analysis by page_id + post_id
        print("\n" + "="*50)
        print("GROUPED BY page_id + post_id")
        print("="*50)
        
        group2_start = time.time()
        page_post_groups = analyze_grouped_data(data, ['page_id', 'post_id'])
        
        # Show first few groups
        group_count = 0
        for group_name, group_analysis in page_post_groups.items():
            if group_count >= 3:  # Limit output
                remaining = len(page_post_groups) - 3
                print(f"... and {remaining} more groups")
                break
            
            print(f"\nGroup: {group_name}")
            print(f"Records: {group_analysis['count']}")
            
            # Show numeric columns for this group
            numeric_cols = []
            string_cols = []
            for col, analysis in group_analysis['columns'].items():
                if analysis['type'] == 'numeric':
                    numeric_cols.append(col)
                elif analysis['type'] == 'string':
                    string_cols.append(col)
            
            if numeric_cols:
                print("  Numeric columns:")
                for col in numeric_cols[:2]:  # Limit to first 2
                    analysis = group_analysis['columns'][col]
                    print(f"    {col}: mean={analysis['mean']:.2f}, min={analysis['min']}, max={analysis['max']}")
            
            if string_cols:
                print("  String columns:")
                for col in string_cols[:2]:  # Limit to first 2
                    analysis = group_analysis['columns'][col]
                    print(f"    {col}: {analysis['unique']} unique, most frequent='{analysis['most_frequent']}' ({analysis['most_frequent_count']} times)")
            
            group_count += 1
        
        group2_time = time.time() - group2_start
        print(f"\nGrouped analysis completed in {group2_time:.2f} seconds")
        
        # Final timing summary
        total_time = time.time() - start_time
        print(f"\n" + "="*50)
        print("TIMING SUMMARY")
        print("="*50)
        print(f"Data loading: {load_time:.2f} seconds")
        print(f"JSON unpacking: {unpack_time:.2f} seconds")
        print(f"Overall analysis: {total_time - load_time - unpack_time:.2f} seconds")
        print(f"Total execution time: {total_time:.2f} seconds")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Restore stdout and close file
        sys.stdout = original_stdout
        output_file.close()
        print("\n✅ All results written to fbpost_pure_python_summary_output.txt")

if __name__ == "__main__":
    main() 