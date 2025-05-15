# README: Data Science Projects (Weeks 1–5)

This repository contains code and analysis from my data science projects, developed during a class focused on data manipulation, processing, and analysis using 
tools like PySpark, DuckDB, Polars, and Pandas. Below is a breakdown of each week's project and its respective objectives. The final project includes an 
analysis of NYC rideshare data, focusing on tips, base fares, and driver earnings.

## Table of Contents

- [Week 1: Data Processing with Python](#week-1-data-processing-with-python)
- [Week 2: Comparing DuckDB, Polars, and Pandas](#week-2-comparing-duckdb-polars-and-pandas)
- [Week 3: Data Transformation and Aggregation with Polars](#week-3-data-transformation-and-aggregation-with-polars)
- [Week 4: Color Analysis and Parquet Data](#week-4-color-analysis-and-parquet-data)
- [Week 5: Comparing Polars and PySpark](#week-5-comparing-polars-and-pyspark)
- [Final Project: NYC Rideshare Data Analysis](#final-project-nyc-rideshare-data-analysis)

## Week 1: Data Processing with Python

In Week 1, the focus was on basic data processing using Python and the `csv` module. The task was to analyze a CSV file containing timestamped color data and 
calculate the most frequently used color and pixel location within a given time range. The program takes command-line arguments for the start and end date/time, 
reads the data, and then computes and prints the most common color and coordinate.

### Key Concepts:

* **Command-line arguments** for input validation.
* **Datetime parsing** and range filtering.
* **Counting occurrences** of colors and coordinates using Python’s `Counter`.

### Key Libraries:

* `sys`, `csv`
* `datetime`, `time`
* `collections.Counter`

---

## Week 2: Comparing DuckDB, Polars, and Pandas

In Week 2, I compared three different data processing tools: **DuckDB**, **Polars**, and **Pandas**. The goal was to analyze the same dataset using each library and 
evaluate their performance, ease of use, and efficiency. The project involved:

* **Data reading**: Using each library to read and process a large CSV file.
* **Filtering and aggregation**: Comparing how each library handles filtering data based on timestamps and aggregating results
  (finding the most common color and coordinates).
* **Performance evaluation**: Timing the execution of operations to determine which library is the fastest and most memory-efficient.

### Key Concepts:

* **Comparative analysis** of data processing tools.
* **Efficiency evaluation** in terms of speed and memory usage.
* **SQL-like querying** using DuckDB vs. DataFrame-based manipulation with Polars and Pandas.

### Key Libraries:

* `duckdb`
* `polars`
* `pandas`
* `sys`, `time`

---

## Week 3: Data Transformation and Aggregation with Polars

Week 3 involved using **Polars**, a high-performance DataFrame library, to perform data transformation and aggregation. I loaded the data, processed timestamps, and calculated the most common pixel colors and coordinates within a given time range. Additionally, I introduced data cleaning steps and aggregated data by user interaction.

### Key Concepts:

* **Polars for efficient data processing**.
* **Grouping and aggregation** of data.
* **Handling missing or inconsistent data**.

### Key Libraries:

* `polars`
* `sys`, `time`
* `datetime`

---

## Week 4: Color Analysis and Parquet Data

In Week 4, the focus shifted to analyzing color data in a **Parquet** file format, which is more efficient for larger datasets. The code normalizes the timestamp values, converts them to datetime format, and ranks pixel colors based on user interactions. Additionally, it investigates the most frequently painted coordinates and visualizes the results.

### Key Concepts:

* **Parquet file format** for storing large datasets.
* **Data normalization** and cleaning for consistency.
* **Color ranking and user interactions**.

### Key Libraries:

* `polars`
* `matplotlib`
* `numpy`
* `pandas`

---

## Week 5: NYC Rideshare Data Analysis

In Week 5, I compared **Polars** and **PySpark** for processing the dataset. The objective was to find the closest color to Cal Poly green using both tools and evaluate their performance in terms of speed, memory efficiency, and ease of use.

### Key Concepts:

* **Color distance calculation** using Euclidean distance to find the closest match to Cal Poly green.
* **Data sorting and grouping** based on color proximity.

### Key Libraries:

* `pyspark`
* `polars`
* `math`, `pyarrow`

---

## Final Project: NYC Rideshare Data Analysis

For the final project, I performed a deeper analysis of NYC rideshare data with a focus on understanding the relationship between driver pay and tips. This involved using **Polars** to clean and transform the data, calculate metrics like total earnings, tip proportions, and average trip durations, and visualize the results. The analysis also involved filtering out outliers, identifying trends over time, and plotting distributions.

### Key Tasks:

* **Data cleaning**: Removed outliers, ensured consistency.
* **Earnings analysis**: Explored tip distribution, total earnings, and tip-to-base-fare ratios.
* **Visualization**: Created multiple visualizations, including boxplots, histograms, and time series plots.

---

This repository showcases my ability to process, clean, and analyze large datasets using both SQL and modern data science tools like **Polars**, **PySpark**, and **Pandas**. The final project applies these techniques to NYC rideshare data, providing insights into driver earnings, tipping behavior, and the factors influencing ride compensation.
