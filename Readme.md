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

# ETL Pipeline - Data Cleaning & Standardization

## 📋 Overview

Following the EDA, we built a production-ready ETL pipeline to process the retail transactional dataset, automatically clean and standardize all data, and separate clean records from problematic ones that require manual intervention.

**Pipeline Status:** Completed  
**Implementation Date:** November 15, 2025  
**Processing Time:** ~2-3 minutes for 302K records

---

## 🔄 ETL Pipeline Architecture

### **Extract Phase**
- **Source:** `/kaggle/input/retail-transactional-dataset/retail_data.csv`
- **Records Loaded:** 302,010 transactions
- **Columns:** 30 features

### **Transform Phase - 8 Data Cleaning Steps**

#### **Step 1: Date Standardization**
- **Purpose:** Convert all date formats to ISO 8601 standard (YYYY-MM-DD)
- **Formats Handled:** `9/18/2023`, `05-08-23`, `01-10-24`, `12/31/2023`, `MM-DD-YY`, `M/D/YYYY`, etc.
- **Method:** Custom robust parser with 18+ format patterns
- **Two-digit year handling:** Years >2050 automatically adjusted to 1900s
- **Invalid dates:** Flagged and moved to bad CSV

#### **Step 2: Time Standardization**
- **Purpose:** Standardize all time values to HH:MM:SS format
- **Method:** Convert to Python time objects
- **Invalid times:** Flagged and moved to bad CSV

#### **Step 3: Text Cleaning**
- **Operations:**
  - Strip leading/trailing whitespace
  - Remove extra internal spaces
  - Standardize phone numbers (digits only, min 10 digits)
  - Convert empty strings to NULL
- **Columns Affected:** Name, Email, Address, City, State, Country, Product fields

#### **Step 4: Numeric Validation**
- **Rules:**
  - Remove negative values (invalid for Price, Amount, Quantity)
  - Validate numeric data types
  - Convert invalid numeric strings to NULL
- **Columns Affected:** Transaction_ID, Customer_ID, Phone, Zipcode, Age, Amount, Total_Amount, etc.

#### **Step 5: Missing Value Detection**
- **Rule:** ANY row with at least one missing value is flagged
- **Strictness:** Zero tolerance for incomplete records in good CSV
- **Expected Impact:** ~8,099 rows flagged (2.68%)

#### **Step 6: Duplicate Removal**
- **Rule:** Exact duplicate rows flagged (first occurrence kept)
- **Expected Impact:** 8 rows flagged

#### **Step 7: Business Logic Validation**
- **Rules:**
  - Zero values in Price → Invalid
  - Zero values in Amount → Invalid
  - Zero values in Quantity → Invalid
  - Zero values in Total_Amount → Invalid
- **Rationale:** Zero prices/amounts indicate incomplete transactions

#### **Step 8: Record Separation**
- **Logic:** If ANY validation fails, entire row → bad CSV
- **Good CSV:** 100% clean, complete, standardized data
- **Bad CSV:** Records with detailed error descriptions

### **Load Phase**

**Output Files:**

1. **`/kaggle/working/good_data.csv`**
   - Fully cleaned and standardized records
   - No missing values
   - No duplicates
   - All dates in YYYY-MM-DD format
   - All times in HH:MM:SS format
   - Valid business rules
   - Ready for analysis/modeling

2. **`/kaggle/working/bad_data.csv`**
   - Records requiring manual review
   - Includes `Validation_Errors` column with detailed issue descriptions
   - Original data preserved for investigation

---

## 📊 Expected Results

Based on EDA findings:

| Metric | Value |
|--------|-------|
| **Input Records** | 302,010 |
| **Good Records** | ~293,900 (97.3%) |
| **Bad Records** | ~8,100 (2.7%) |
| **Data Quality Score** | 97.3% |

### **Issue Breakdown (Bad CSV)**

| Issue Type | Estimated Count |
|------------|----------------|
| Missing Values | ~8,099 rows |
| Invalid Dates | ~350 rows |
| Invalid Times | ~350 rows |
| Duplicates | 8 rows |
| Zero Values | Unknown |
| Negative Values | Unknown |

*Note: Rows may have multiple issues*

---

## ✅ Data Quality Guarantees

### **Good CSV Guarantees**
- ✓ Zero missing values
- ✓ Zero duplicates
- ✓ All dates in standard format (YYYY-MM-DD)
- ✓ All times in standard format (HH:MM:SS)
- ✓ All numeric fields validated
- ✓ No negative values in financial/quantity fields
- ✓ No zero values in critical business fields
- ✓ Text fields cleaned and normalized

### **Bad CSV Features**
- ✓ Original data preserved
- ✓ Detailed error descriptions per row
- ✓ Multiple issues per row documented
- ✓ Ready for manual review/correction

---

## 📝 Transformation Logic Examples

### **Date Transformation**
Input: "05-08-23", "01-10-24", "9/18/2023"
Output: "2023-05-08", "2024-01-10", "2023-09-18"
Format: YYYY-MM-DD (ISO 8601)

### **Time Transformation**
Input: "8:42:04", "22:03:55", "4:06:29"
Output: time(8, 42, 4), time(22, 3, 55), time(4, 6, 29)
Format: HH:MM:SS (Python time objects)

### **Phone Number Standardization**
Input: "(123) 456-7890", "123-456-7890", "1234567890"
Output: "1234567890", "1234567890", "1234567890"
Format: Digits only, minimum 10 digits

### **Text Cleaning**
Input: " John Doe ", " ", ""
Output: "John Doe", NaN, NaN
Logic: Strip whitespace, collapse multiple spaces, empty → NULL


---

## 🔍 Error Handling

### **Error Categories**

**Type 1: Invalid Format** - Data cannot be parsed/converted
- Example: Date "abc123", Phone "invalid"
- Action: Row → bad CSV with "Invalid [column]" error

**Type 2: Missing Data** - Required field is empty
- Example: NULL in any column
- Action: Row → bad CSV with "Missing values" error

**Type 3: Business Rule Violation** - Data fails validation
- Example: Price = 0, Amount = -100
- Action: Row → bad CSV with "Zero [column]" or "Negative [column]" error

**Type 4: Duplicate** - Exact duplicate record
- Example: Transaction already exists
- Action: Row → bad CSV with "Duplicate" error

---

## 🎯 Use Cases for Cleaned Data

### **Good CSV - Recommended Uses**
1. Machine learning model training
2. Statistical analysis and reporting
3. Data warehousing and BI dashboards
4. Customer segmentation analysis
5. Sales forecasting models
6. Product recommendation systems

### **Bad CSV - Recommended Actions**
1. Manual data correction
2. Contact data providers for clarification
3. Identify systemic data entry errors
4. Improve upstream data capture processes
5. Train data entry staff on common errors

## 🛠️ Methodology

- **Tools:** Python, Pandas, NumPy, Matplotlib, Seaborn
- **Date Parsing:** Custom robust parser with 18+ format support
- **Outlier Detection:** IQR method (3×IQR threshold)
- **Missing Value Strategy:** Flagged for review, not removed
- **Analysis Sections:** 11 comprehensive sections covering all aspects

---
## Project Documentation

A brief documentation about what we did in the pipeline and how we strategize and planed for the entire project. All the discussion and planned deliverables done in spring 1, are there present in project_documentation.pdf