# Task 04: Descriptive Statistics Analysis - Multi-Dataset Comparison

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![Pandas](https://img.shields.io/badge/pandas-%3E=2.0.0-lightgrey)](https://pandas.pydata.org/)
[![Polars](https://img.shields.io/badge/polars-%3E=0.20.0-lightgrey)](https://pola-rs.github.io/polars/)

---

## Overview

This project presents a comparative analysis of descriptive statistics computation across three large-scale social media datasets from the 2024 US Presidential election:
- **Twitter Posts** 
- **Facebook Posts**
- **Facebook Ads**

Three implementation approaches are provided for each dataset:
- **Pure Python** (standard library only)
- **Pandas** (industry-standard DataFrame library)
- **Polars** (modern, high-performance DataFrame library)

The goal is to evaluate performance, robustness, and usability for large, real-world social media data.

---

## File Structure

```text
TASK_04_DESCRIPTIVE_STATS/
├── 2024_tw_posts/
│   ├── tw_post_pandas_stats.py
│   ├── tw_post_pure_python_stats.py
│   ├── tw_post_polars_stats.py
│   └── [output files]
├── 2024_fb_posts/
│   ├── fbpost_pandas_stats.py
│   ├── fbpost_pure_python_stats.py
│   ├── fbpost_polars_stats.py
│   └── [output files]
└── 2024_fb_ads/
    ├── ads_pandas_stats.py
    ├── ads_pure_python_stats.py
    ├── ads_polars_stats.py
    └── [output files]
```

---

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/harshad303/Task_04_Descriptive_Stats.git
   cd Task_04_Descriptive_Stats
   ```
2. **(Recommended) Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   # Or install individually:
   pip install pandas polars
   ```

---

## Usage

1. **Place the relevant dataset CSVs in the appropriate subfolder.**
   - *Datasets are NOT included in this repository.*
2. **Run the desired script:**
   ```bash
   python 2024_tw_posts/tw_post_pandas_stats.py
   python 2024_tw_posts/tw_post_pure_python_stats.py
   python 2024_tw_posts/tw_post_polars_stats.py
   # ...similarly for Facebook posts and ads
   ```
3. **Output files** (e.g., `.txt` summaries) will be generated in the same folder as the script.

---

## Performance Comparison

| Approach      | Data Loading (s) | Total Time (s) | Notes                |
|---------------|------------------|----------------|----------------------|
| Pure Python   | 1.01             | 2.14           | Robust, slowest      |
| Pandas        | 0.19             | 0.57           | Fast, user-friendly  |
| Polars        | 0.05             | N/A*           | Fastest, API issues  |

*Polars encountered API errors (see below) but demonstrated superior loading speed.

---

## Technical Challenges

- **Polars API Instability:**
  - Encountered breaking changes in the `map_dict` and groupby APIs, requiring multiple code adjustments.
  - Some advanced groupby and value_counts operations required workarounds due to evolving syntax.
- **Large Dataset Handling:**
  - Ensured memory efficiency and robust error handling for files with tens of thousands of rows and dozens of columns.
- **Output Consistency:**
  - Matched output formatting and statistics across all three approaches for fair comparison.

---

## Key Findings

### Summary of Findings Across All Three Datasets

**Pure Python**: Most verbose and error-prone, but gives full control over statistical logic. Required manual implementation of groupby operations and data type handling. Recommended only for educational purposes or highly constrained environments where dependencies aren't available.

**Pandas**: Easiest to use and most expressive for both descriptive statistics and complex grouping operations. Excellent documentation and error messages. Ideal for rapid prototyping, data exploration, and most production analytics workflows.

**Polars**: Fastest loading and most memory-efficient across all dataset sizes, especially noticeable with larger Facebook datasets. However, required debugging due to evolving API (map_dict deprecation). Best for production pipelines and big data scenarios.

### Cross-Dataset Insights

- Twitter posts showed highest engagement variance
- Facebook ads demonstrated different statistical distributions  
- Facebook posts revealed [add your specific findings]

Polars consistently outperformed on raw speed, while Pandas offered the best balance of ease-of-use and analytical power. Pure Python implementation revealed how much computational abstraction modern libraries provide.

---

## Recommendations 

- **Use Pandas** for most data analysis tasks: it is fast, well-documented, and widely supported.
- **Try Polars** for very large datasets or when performance is critical, but be prepared for API changes and evolving documentation.
- **Understand Pure Python** approaches for foundational learning and debugging, but use DataFrame libraries for production work.
- **Always use .gitignore** to avoid committing large or sensitive datasets.
- **Document your workflow** and compare results across tools for reproducibility.

---

## Dataset Availability

> **Note:** Datasets are NOT included in this repository due to size and privacy constraints. Please contact the instructor or project lead for access.

---


## License

This project is for academic use only. Please contact the author for reuse or redistribution requests. 