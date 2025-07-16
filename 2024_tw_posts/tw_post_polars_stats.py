import polars as pl
import time
import sys
import os

def safe_fmt(val, precision=2):
    return f"{val:.{precision}f}" if isinstance(val, (int, float)) else "N/A"

def main():
    start = time.time()
    output_file = open('tw_post_polars_summary_output.txt', 'w', encoding='utf-8')
    def dual_print(*args, **kwargs):
        print(*args, **kwargs)
        print(*args, **kwargs, file=output_file)
    try:
        # === STEP 1: Load Dataset ===
        csv_path = os.path.join(os.path.dirname(__file__), '2024_tw_posts_president_scored_anon.csv')
        load_start = time.time()
        df = pl.read_csv(csv_path, null_values=['', 'nan'], try_parse_dates=True)
        load_time = time.time() - load_start
        dual_print(f"Loaded {df.height} rows, {df.width} columns in {load_time:.2f} seconds.")
        dual_print(f"Columns: {list(df.columns)}")

        # === STEP 2: Define Columns ===
        target_numeric_cols = [
            'retweetCount', 'replyCount', 'likeCount',
            'quoteCount', 'viewCount', 'bookmarkCount'
        ]
        categorical_cols = [col for col in df.columns if col not in target_numeric_cols]

        # === STEP 3: Overall Descriptive Stats ===
        dual_print("\n" + "="*60)
        dual_print("\U0001F4CA OVERALL DESCRIPTIVE STATISTICS")
        dual_print("="*60)

        # ➕ Numeric Summary
        dual_print("\n\U0001F522 Numerical Columns Summary")
        numeric_summary = df.select(target_numeric_cols).describe()
        dual_print(numeric_summary)

        # 🔠 Categorical Summary
        dual_print("\n\U0001F521 Categorical Columns Summary")
        for col in categorical_cols:
            try:
                vc_df = df.select(pl.col(col).value_counts())
                vc_df = vc_df.sort('counts', descending=True)
                vc = vc_df.to_series(0)
                unique = vc.len()
                most_common = vc[0][0] if unique > 0 else "N/A"
                freq = vc[0][1] if unique > 0 else 0
                dual_print(f"  • {col:40} → unique={unique}, most_common=({most_common}, {freq})")
            except Exception as e:
                dual_print(f"  • {col:40} → ❌ Error: {str(e)}")

        # === STEP 5: Grouped by 'source' ===
        dual_print("\n" + "="*60)
        dual_print("\U0001F4CC STATISTICS GROUPED BY 'source'")
        dual_print("="*60)

        grouped_source = df.group_by("source").agg([
            pl.col(c).mean().alias(f"{c}_mean") for c in target_numeric_cols
        ] + [
            pl.col(c).std().alias(f"{c}_std") for c in target_numeric_cols
        ] + [
            pl.col(c).min().alias(f"{c}_min") for c in target_numeric_cols
        ] + [
            pl.col(c).max().alias(f"{c}_max") for c in target_numeric_cols
        ] + [
            pl.len().alias("row_count")
        ])

        for row in grouped_source.head(3).iter_rows():
            dual_print(f"\n▶ Group: source = {row[0]}")
            for i, col in enumerate(target_numeric_cols):
                offset = 1 + i
                mean_val = row[offset]
                std_val = row[offset + len(target_numeric_cols)]
                min_val = row[offset + 2 * len(target_numeric_cols)]
                max_val = row[offset + 3 * len(target_numeric_cols)]

                dual_print(f"   - {col:40}: "
                      f"mean={safe_fmt(mean_val)}, "
                      f"std={safe_fmt(std_val)}, "
                      f"min={min_val if min_val is not None else 'N/A'}, "
                      f"max={max_val if max_val is not None else 'N/A'}")

        # === STEP 6: Grouped by ('source', 'lang') ===
        dual_print("\n" + "="*60)
        dual_print("\U0001F4CC STATISTICS GROUPED BY ('source', 'lang')")
        dual_print("="*60)

        grouped_source_lang = df.group_by(["source", "lang"]).agg([
            pl.col(c).mean().alias(f"{c}_mean") for c in target_numeric_cols
        ])

        for row in grouped_source_lang.head(3).iter_rows():
            dual_print(f"\n▶ Group: (source, lang) = ({row[0]}, {row[1]})")
            for i, col in enumerate(target_numeric_cols):
                val = row[2 + i]
                dual_print(f"   - {col:40}: mean={safe_fmt(val)}")

        # === Done ===
        dual_print("\n✅ Script completed successfully!")
        dual_print(f"⏱️ Total execution time: {time.time() - start:.2f} seconds")
    except Exception as e:
        dual_print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        output_file.close()
        print("\n✅ All polars stats written to tw_post_polars_summary_output.txt")

if __name__ == "__main__":
    main() 