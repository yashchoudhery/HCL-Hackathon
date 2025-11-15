import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

def parse_date_robust(date_str):
    if pd.isna(date_str) or str(date_str).strip() == '':
        return pd.NaT
    
    date_formats = [
        '%m-%d-%y',
        '%d-%m-%y',
        '%m/%d/%y',
        '%d/%m/%y',
        '%m-%d-%Y',
        '%d-%m-%Y',
        '%m/%d/%Y',
        '%d/%m/%Y',
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%d.%m.%Y',
        '%d.%m.%y',
        '%Y%m%d',
        '%m%d%Y',
        '%b %d, %Y',
        '%B %d, %Y',
        '%d %b %Y',
        '%d %B %Y',
    ]
    
    for fmt in date_formats:
        try:
            parsed = pd.to_datetime(date_str, format=fmt)
            if fmt.endswith('%y') and parsed.year > 2050:
                parsed = parsed.replace(year=parsed.year - 100)
            return parsed
        except:
            continue
    
    try:
        return pd.to_datetime(date_str, infer_datetime_format=True)
    except:
        return pd.NaT

print("COMPREHENSIVE EXPLORATORY DATA ANALYSIS")
print("Retail Transactional Dataset")
print(f"Analysis Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

df = pd.read_csv('/kaggle/input/retail-transactional-dataset/retail_data.csv')

print("SECTION 1: DATASET OVERVIEW")
print(f"\nRows: {df.shape[0]:,}")
print(f"Columns: {df.shape[1]}")
print(f"Memory Usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
print(f"Duplicate Rows: {df.duplicated().sum():,} ({df.duplicated().sum()/len(df)*100:.2f}%)")

print("SECTION 2: COLUMN INFORMATION")

column_info = pd.DataFrame({
    'Column': df.columns,
    'Data Type': df.dtypes.values,
    'Non-Null': [df[col].count() for col in df.columns],
    'Null Count': [df[col].isnull().sum() for col in df.columns],
    'Null %': [f"{(df[col].isnull().sum()/len(df)*100):.2f}%" for col in df.columns],
    'Unique': [df[col].nunique() for col in df.columns],
})
print("\n" + column_info.to_string(index=False))

print("SECTION 3: DATA PREVIEW")
print("\nFirst 5 Rows:")
print(df.head())
print("\nLast 5 Rows:")
print(df.tail())
print("\nRandom 5 Rows:")
print(df.sample(min(5, len(df))))

print("SECTION 4: DATE/TIME STANDARDIZATION")

date_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in ['date', 'time', 'timestamp'])]

if date_cols:
    for col in date_cols:
        print(f"\nProcessing: {col}")
        print("-"*80)
        
        original_sample = df[col].head(10)
        print(f"Original Format Sample:\n{original_sample}")
        
        df[col + '_parsed'] = df[col].apply(parse_date_robust)
        
        parsed_count = df[col + '_parsed'].notna().sum()
        unparsed_count = df[col + '_parsed'].isna().sum() - df[col].isna().sum()
        
        print(f"\nSuccessfully Parsed: {parsed_count:,} ({parsed_count/len(df)*100:.2f}%)")
        print(f"Failed to Parse: {unparsed_count:,} ({unparsed_count/len(df)*100:.2f}%)")
        
        if unparsed_count > 0:
            print(f"\nUnparseable Date Samples:")
            unparsed_mask = df[col + '_parsed'].isna() & df[col].notna()
            print(df.loc[unparsed_mask, col].head(10))
        
        print(f"\nStandardized Format Sample:")
        print(df[col + '_parsed'].head(10))
        
        if df[col + '_parsed'].notna().any():
            print(f"\nDate Range:")
            print(f"  Earliest: {df[col + '_parsed'].min()}")
            print(f"  Latest: {df[col + '_parsed'].max()}")
            print(f"  Span: {(df[col + '_parsed'].max() - df[col + '_parsed'].min()).days} days")
            
            current_time = pd.Timestamp.now()
            future_dates = df[col + '_parsed'] > current_time
            if future_dates.any():
                print(f"\n⚠️  Future Dates Detected: {future_dates.sum():,} records")
                print(f"Sample Future Dates:")
                print(df.loc[future_dates, [col, col + '_parsed']].head())
else:
    print("\nNo date/time columns detected")

print("SECTION 5: MISSING VALUES ANALYSIS")

missing_data = pd.DataFrame({
    'Column': df.columns,
    'Missing': df.isnull().sum(),
    'Missing %': (df.isnull().sum() / len(df) * 100).round(2),
})
missing_data = missing_data[missing_data['Missing'] > 0].sort_values('Missing', ascending=False)

if len(missing_data) > 0:
    print(f"\nColumns with Missing Values: {len(missing_data)}")
    print(missing_data.to_string(index=False))
    print(f"\nTotal Missing Cells: {df.isnull().sum().sum():,}")
    print(f"Overall Missing %: {(df.isnull().sum().sum() / (df.shape[0] * df.shape[1]) * 100):.2f}%")
else:
    print("\n✓ No missing values found")

print("SECTION 6: STATISTICAL SUMMARY")

numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

if numeric_cols:
    print("\nNumerical Columns:")
    print(df[numeric_cols].describe().T)
    
    print("\nAdvanced Statistics:")
    advanced_stats = pd.DataFrame({
        'Column': numeric_cols,
        'Median': [df[col].median() for col in numeric_cols],
        'Variance': [df[col].var() for col in numeric_cols],
        'Skewness': [df[col].skew() for col in numeric_cols],
        'Kurtosis': [df[col].kurtosis() for col in numeric_cols],
        'IQR': [df[col].quantile(0.75) - df[col].quantile(0.25) for col in numeric_cols]
    })
    print(advanced_stats.to_string(index=False))

categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
categorical_cols = [col for col in categorical_cols if '_parsed' not in col]

if categorical_cols:
    print("Categorical Columns:")
    cat_summary = pd.DataFrame({
        'Column': categorical_cols,
        'Unique': [df[col].nunique() for col in categorical_cols],
        'Most Frequent': [df[col].mode()[0] if len(df[col].mode()) > 0 else 'N/A' for col in categorical_cols],
        'Frequency': [df[col].value_counts().iloc[0] if len(df[col]) > 0 else 0 for col in categorical_cols],
        'Frequency %': [f"{(df[col].value_counts().iloc[0]/len(df)*100):.2f}%" if len(df[col]) > 0 else "0%" for col in categorical_cols]
    })
    print(cat_summary.to_string(index=False))

print("SECTION 7: DATA QUALITY ISSUES")

problematic_rows = pd.DataFrame(index=df.index)
problematic_rows['issues'] = ''
issue_summary = {}

missing_rows = df.isnull().any(axis=1)
if missing_rows.any():
    count = missing_rows.sum()
    issue_summary['Missing Values'] = count
    problematic_rows.loc[missing_rows, 'issues'] += 'Missing values; '
    print(f"\nRows with Missing Values: {count:,} ({count/len(df)*100:.2f}%)")

duplicates = df.duplicated(keep=False)
if duplicates.any():
    count = duplicates.sum()
    issue_summary['Duplicates'] = count
    problematic_rows.loc[duplicates, 'issues'] += 'Duplicate; '
    print(f"Duplicate Rows: {count:,} ({count/len(df)*100:.2f}%)")

for col in numeric_cols:
    negative_vals = df[col] < 0
    if negative_vals.any():
        count = negative_vals.sum()
        if 'Negative Values' not in issue_summary:
            issue_summary['Negative Values'] = 0
        issue_summary['Negative Values'] += count
        problematic_rows.loc[negative_vals, 'issues'] += f'Negative {col}; '
        print(f"Negative Values in '{col}': {count:,} ({count/len(df)*100:.2f}%)")

critical_cols = [col for col in numeric_cols if any(kw in col.lower() for kw in ['price', 'amount', 'quantity', 'total', 'cost'])]
for col in critical_cols:
    zero_vals = df[col] == 0
    if zero_vals.any():
        count = zero_vals.sum()
        if 'Zero Values' not in issue_summary:
            issue_summary['Zero Values'] = 0
        issue_summary['Zero Values'] += count
        problematic_rows.loc[zero_vals, 'issues'] += f'Zero {col}; '
        print(f"Zero Values in '{col}': {count:,} ({count/len(df)*100:.2f}%)")

print("Outlier Detection (IQR Method, 3*IQR threshold):")
for col in numeric_cols:
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    outliers = ((df[col] < (Q1 - 3 * IQR)) | (df[col] > (Q3 + 3 * IQR)))
    if outliers.any():
        count = outliers.sum()
        print(f"  {col}: {count:,} outliers ({count/len(df)*100:.2f}%)")

for col in categorical_cols:
    empty_strings = df[col].astype(str).str.strip() == ''
    if empty_strings.any():
        count = empty_strings.sum()
        if 'Empty Strings' not in issue_summary:
            issue_summary['Empty Strings'] = 0
        issue_summary['Empty Strings'] += count
        problematic_rows.loc[empty_strings, 'issues'] += f'Empty {col}; '
        print(f"Empty Strings in '{col}': {count:,}")

print("DATA QUALITY SUMMARY")
total_problematic = (problematic_rows['issues'] != '').sum()
print(f"\nProblematic Rows: {total_problematic:,} ({total_problematic/len(df)*100:.2f}%)")
print(f"Clean Rows: {len(df) - total_problematic:,} ({(len(df)-total_problematic)/len(df)*100:.2f}%)")

if issue_summary:
    print("\nIssue Breakdown:")
    for issue, count in sorted(issue_summary.items(), key=lambda x: x[1], reverse=True):
        print(f"  {issue}: {count:,}")

if total_problematic > 0:
    print("Sample Problematic Rows (First 10):")
    problematic_df = df[problematic_rows['issues'] != ''].copy()
    problematic_df['Data_Quality_Issues'] = problematic_rows.loc[problematic_rows['issues'] != '', 'issues']
    print(problematic_df.head(10))
    
    problematic_df.to_csv('/kaggle/working/problematic_rows.csv', index=False)
    print(f"\n✓ Saved to: /kaggle/working/problematic_rows.csv")

print("SECTION 8: UNIQUE VALUES & CARDINALITY")

unique_analysis = pd.DataFrame({
    'Column': df.columns,
    'Unique': [df[col].nunique() for col in df.columns],
    'Unique %': [f"{(df[col].nunique()/len(df)*100):.2f}%" for col in df.columns],
    'Cardinality': ['High' if df[col].nunique()/len(df) > 0.5 else 'Medium' if df[col].nunique()/len(df) > 0.05 else 'Low' for col in df.columns]
})
print("\n" + unique_analysis.sort_values('Unique', ascending=False).to_string(index=False))

print("Value Distributions (Low Cardinality Columns):")
for col in categorical_cols:
        print(f"\n{col} ({df[col].nunique()} unique values):")
        value_counts = df[col].value_counts()
        value_pcts = df[col].value_counts(normalize=True) * 100
        for val, count in value_counts.head(10).items():
            print(f"  {str(val):<30} : {count:>8,} ({value_pcts[val]:>6.2f}%)")
        if df[col].nunique() > 10:
            print(f"  ... and {df[col].nunique() - 10} more values")

if len(numeric_cols) > 1:
    print("SECTION 9: CORRELATION ANALYSIS")
    
    corr_matrix = df[numeric_cols].corr()
    print("\nCorrelation Matrix:")
    print(corr_matrix)
    
    print("\nHighly Correlated Pairs (|r| > 0.7):")
    high_corr = []
    for i in range(len(corr_matrix.columns)):
        for j in range(i+1, len(corr_matrix.columns)):
            corr_val = corr_matrix.iloc[i, j]
            if abs(corr_val) > 0.7:
                high_corr.append((corr_matrix.columns[i], corr_matrix.columns[j], corr_val))
    
    if high_corr:
        for col1, col2, corr in sorted(high_corr, key=lambda x: abs(x[2]), reverse=True):
            print(f"  {col1} ↔ {col2}: {corr:.3f}")
    else:
        print("  None found")

print("SECTION 10: DISTRIBUTION CHARACTERISTICS")

for col in numeric_cols:
    print(f"\n{col}:")
    print(f"  Range: [{df[col].min():.2f}, {df[col].max():.2f}]")
    print(f"  Mean: {df[col].mean():.2f}, Median: {df[col].median():.2f}")
    print(f"  Std Dev: {df[col].std():.2f}")
    print(f"  Skewness: {df[col].skew():.2f} ({'Right-skewed' if df[col].skew() > 0 else 'Left-skewed' if df[col].skew() < 0 else 'Symmetric'})")
    print(f"  Kurtosis: {df[col].kurtosis():.2f} ({'Heavy-tailed' if df[col].kurtosis() > 0 else 'Light-tailed'})")

print("SECTION 11: FINAL SUMMARY & RECOMMENDATIONS")

print(f"\nDataset: {len(df):,} rows × {len(df.columns)} columns")
print(f"Numerical Columns: {len(numeric_cols)}")
print(f"Categorical Columns: {len(categorical_cols)}")
print(f"Memory: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

quality_score = ((len(df) - total_problematic) / len(df)) * 100
print(f"\nData Quality Score: {quality_score:.2f}%")
print(f"Clean Rows: {len(df) - total_problematic:,}")
print(f"Problematic Rows: {total_problematic:,}")

print("\nRecommendations:")
if missing_rows.any():
    print(f"  1. Handle {missing_rows.sum():,} rows with missing values")
if duplicates.any():
    print(f"  2. Review {duplicates.sum():,} duplicate rows")
if 'Negative Values' in issue_summary:
    print(f"  3. Investigate {issue_summary['Negative Values']} negative values")
if 'Zero Values' in issue_summary:
    print(f"  4. Review {issue_summary['Zero Values']} zero values")
if not issue_summary:
    print("Data quality is excellent!")

print("EDA COMPLETE")
print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")