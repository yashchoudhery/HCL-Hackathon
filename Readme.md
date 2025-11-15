# HCL Hackathon
# Retail Transactional Dataset - Exploratory Data Analysis

## 📊 Dataset Overview

**Dataset:** Retail Transactional Dataset  

### Basic Statistics
- **Total Records:** 302,010 transactions
- **Total Columns:** 30 features
- **Memory Usage:** 402.71 MB
- **Time Period:** March 1, 2023 - February 29, 2024 (365 days)
- **Duplicate Records:** 4 (0.00%)

---

## 🔍 What We Found

### 1. Data Structure
- **Numerical Columns:** 10 (IDs, demographics, financial metrics)
- **Categorical Columns:** 20 (customer info, product details, order status)
- **High Cardinality:** Address (99%), Amount (99%), Phone (99%), Transaction_ID (97%)
- **Low Cardinality:** Gender (2), Year (2), Income (3), Customer Segment (3)

### 2. Data Quality Assessment

**Overall Quality Score: 97.32%**

#### Issues Identified
- **Missing Values:** 8,099 rows (2.68%) with missing data across 30 columns
  - Highest missing: Name (382), Phone (362), Total_Purchases (361)
  - Overall missing rate: 0.09% of all cells
- **Duplicate Records:** 8 exact duplicates (0.00%)
- **Outliers:** 49,808 outliers detected in Year column (16.49%)
- **Clean Records:** 293,905 rows (97.32%)

### 3. Date/Time Parsing Success

**Date Column:**
- Successfully parsed: 301,651 dates (99.88%)
- Failed to parse: 0 dates
- Handled mixed formats: `9/18/2023`, `05-08-23`, `01-10-24`
- Date range: March 1, 2023 to February 29, 2024

**Time Column:**
- Successfully parsed: 301,660 times (99.88%)
- Note: Time values parsed with today's date (artifact of parsing process)

### 4. Customer Demographics

**Geographic Distribution:**
- **Top Countries:** USA (31.53%), UK (20.88%), Germany (17.49%), Australia (15.02%), Canada (15.01%)
- **Top Cities:** Chicago (7.17%), Portsmouth (6.67%), San Francisco (4.04%)
- **States:** 54 unique states/regions

**Customer Profile:**
- **Gender:** Male (62.12%), Female (37.88%)
- **Income:** Medium (43.12%), Low (31.90%), High (24.98%)
- **Segments:** Regular (48.42%), New (30.20%), Premium (21.38%)
- **Age Range:** 18-70 years (Mean: 35.48, Median: 32)

### 5. Transaction Patterns

**Financial Metrics:**
- **Average Transaction Amount:** $255.16
- **Amount Range:** $10.00 - $500.00
- **Average Total Amount:** $1,367.65
- **Total Amount Range:** $10.00 - $4,999.63
- **Average Purchases per Transaction:** 5.36 items

**Distribution Characteristics:**
- Total_Amount shows right-skewed distribution (Skewness: 0.97)
- Age distribution is right-skewed (Skewness: 0.65)
- Most numeric variables show uniform distributions

### 6. Product & Sales Insights

**Product Categories:**
- Electronics (23.57%), Grocery (22.11%), Clothing (18.13%), Books (18.09%), Home Decor (18.10%)

**Top Brands:**
- Pepsi (10.03%), Coca-Cola (6.09%), Samsung (6.08%), Zara (6.08%), HarperCollins (6.07%)

**Most Popular Products:**
- Water (8.10%), Smartphone (6.11%), Non-Fiction (6.02%), Fiction (5.98%)

**Customer Feedback:**
- Excellent (33.36%), Good (31.49%), Average (20.75%), Bad (14.40%)

**Ratings Distribution:**
- Average Rating: 3.16/5.00
- Median Rating: 3.00/5.00

### 7. Order Fulfillment

**Shipping Methods:**
- Same-Day (34.49%), Express (33.89%), Standard (31.62%)

**Payment Methods:**
- Credit Card (29.84%), Debit Card (25.44%), Cash (24.44%), PayPal (20.28%)

**Order Status:**
- Delivered (43.19%), Shipped (21.53%), Processing (18.94%), Pending (16.34%)

### 8. Correlation Analysis

**Key Finding:** No strong correlations (|r| > 0.7) found between numerical variables

**Moderate Correlations:**
- Total_Amount ↔ Total_Purchases (r = 0.65)
- Total_Amount ↔ Amount (r = 0.67)
- Age ↔ Ratings (r = 0.17) - slight positive correlation

### 9. Temporal Patterns

**Monthly Distribution:**
- Peak: April (13.68%), January (12.34%)
- Low: June (6.17%), February (6.23%)

**Yearly Split:**
- 2023: 83% of transactions
- 2024: 17% of transactions

---

## 🎯 Key Findings Summary

1. **Data Quality is High:** 97.32% clean records with minimal missing data
2. **Date Parsing Success:** Custom parser handled mixed formats (MM-DD-YY, M/D/YYYY) effectively
3. **No Data Rejection:** All records preserved; issues flagged for review
4. **Balanced Product Mix:** Even distribution across 5 product categories
5. **Geographic Diversity:** Customers from 5 countries, 54 states, 130 cities
6. **Customer Base:** Majority male (62%), medium income (43%), regular segment (48%)
7. **High Customer Satisfaction:** 65% positive feedback (Excellent/Good)
8. **Fast Fulfillment:** 68% orders use express/same-day shipping

---

## ⚠️ Recommendations

1. **Handle Missing Values:** Investigate and impute/remove 8,099 rows (2.68%) with missing data
2. **Review Duplicates:** Examine 8 duplicate transactions for data entry errors
3. **Validate Year Outliers:** Check 49,808 records flagged as outliers in Year column
4. **Address Time Parsing:** Time column parsed with current date - consider separate date/time handling
5. **Monitor Product Performance:** Water products dominate - investigate supply chain capacity

---

## 📁 Output Files

- **problematic_rows.csv** - 8,105 rows with data quality issues and detailed error descriptions

---

## 🛠️ Methodology

- **Tools:** Python, Pandas, NumPy, Matplotlib, Seaborn
- **Date Parsing:** Custom robust parser with 18+ format support
- **Outlier Detection:** IQR method (3×IQR threshold)
- **Missing Value Strategy:** Flagged for review, not removed
- **Analysis Sections:** 11 comprehensive sections covering all aspects

