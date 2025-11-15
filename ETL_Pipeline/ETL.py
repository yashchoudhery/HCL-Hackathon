import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def parse_date_robust(date_str):
    if pd.isna(date_str) or str(date_str).strip() == '':
        return pd.NaT
    
    date_formats = [
        '%m-%d-%y', '%d-%m-%y', '%m/%d/%y', '%d/%m/%y',
        '%m-%d-%Y', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y',
        '%Y-%m-%d', '%Y/%m/%d', '%d.%m.%Y', '%d.%m.%y',
        '%Y%m%d', '%m%d%Y', '%b %d, %Y', '%B %d, %Y',
        '%d %b %Y', '%d %B %Y',
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

def clean_text(text):
    if pd.isna(text):
        return text
    text = str(text).strip()
    text = ' '.join(text.split())
    return text if text else np.nan

def standardize_phone(phone):
    if pd.isna(phone):
        return phone
    phone = str(phone).strip()
    phone = ''.join(filter(str.isdigit, phone))
    return phone if len(phone) >= 10 else np.nan

def clean_numeric(val):
    if pd.isna(val):
        return val
    try:
        cleaned = float(val)
        return cleaned if cleaned >= 0 else np.nan
    except:
        return np.nan

class RetailETL:
    def __init__(self, input_path):
        self.input_path = input_path
        self.df = None
        self.good_df = None
        self.bad_df = None
        
    def extract(self):
        print("="*80)
        print("ETL PIPELINE - RETAIL TRANSACTIONAL DATA")
        print("="*80)
        print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        print("STEP 1: EXTRACT")
        print("-"*80)
        self.df = pd.read_csv(self.input_path)
        print(f"Loaded {len(self.df):,} records with {len(self.df.columns)} columns")
        return self
    
    def transform(self):
        print("\nSTEP 2: TRANSFORM & CLEAN")
        print("-"*80)
        
        df_clean = self.df.copy()
        validation_flags = pd.DataFrame(index=df_clean.index)
        validation_flags['errors'] = ''
        validation_flags['is_valid'] = True
        
        print("\n[1/8] Standardizing Date Columns")
        date_cols = [col for col in df_clean.columns if 'date' in col.lower()]
        for col in date_cols:
            df_clean[col] = df_clean[col].apply(parse_date_robust)
            invalid_dates = df_clean[col].isna() & self.df[col].notna()
            if invalid_dates.any():
                validation_flags.loc[invalid_dates, 'errors'] += f'Invalid {col}; '
                validation_flags.loc[invalid_dates, 'is_valid'] = False
                print(f"  - {col}: {invalid_dates.sum()} invalid dates flagged")
        
        print("\n[2/8] Standardizing Time Columns")
        time_cols = [col for col in df_clean.columns if 'time' in col.lower() and 'date' not in col.lower()]
        for col in time_cols:
            df_clean[col] = pd.to_datetime(df_clean[col], format='%H:%M:%S', errors='coerce').dt.time
            invalid_times = df_clean[col].isna() & self.df[col].notna()
            if invalid_times.any():
                validation_flags.loc[invalid_times, 'errors'] += f'Invalid {col}; '
                validation_flags.loc[invalid_times, 'is_valid'] = False
                print(f"  - {col}: {invalid_times.sum()} invalid times flagged")
        
        print("\n[3/8] Cleaning Text Columns")
        text_cols = df_clean.select_dtypes(include=['object']).columns
        text_cols = [col for col in text_cols if col not in date_cols and col not in time_cols]
        for col in text_cols:
            if col.lower() == 'phone':
                df_clean[col] = df_clean[col].apply(standardize_phone)
            else:
                df_clean[col] = df_clean[col].apply(clean_text)
        print(f"  - Cleaned {len(text_cols)} text columns")
        
        print("\n[4/8] Validating Numeric Columns")
        numeric_cols = df_clean.select_dtypes(include=[np.number]).columns.tolist()
        for col in numeric_cols:
            original_vals = df_clean[col].copy()
            df_clean[col] = df_clean[col].apply(clean_numeric)
            invalid_numeric = df_clean[col].isna() & original_vals.notna()
            if invalid_numeric.any():
                validation_flags.loc[invalid_numeric, 'errors'] += f'Invalid {col}; '
                validation_flags.loc[invalid_numeric, 'is_valid'] = False
                print(f"  - {col}: {invalid_numeric.sum()} invalid values flagged")
        
        print("\n[5/8] Detecting Missing Values")
        missing_rows = df_clean.isnull().any(axis=1)
        if missing_rows.any():
            validation_flags.loc[missing_rows, 'errors'] += 'Missing values; '
            validation_flags.loc[missing_rows, 'is_valid'] = False
            print(f"  - {missing_rows.sum()} rows with missing values flagged")
        
        print("\n[6/8] Detecting Duplicates")
        duplicates = df_clean.duplicated(keep='first')
        if duplicates.any():
            validation_flags.loc[duplicates, 'errors'] += 'Duplicate; '
            validation_flags.loc[duplicates, 'is_valid'] = False
            print(f"  - {duplicates.sum()} duplicate rows flagged")
        
        print("\n[7/8] Validating Business Rules")
        critical_cols = [col for col in numeric_cols if any(kw in col.lower() for kw in ['price', 'amount', 'quantity', 'total'])]
        for col in critical_cols:
            zero_vals = df_clean[col] == 0
            if zero_vals.any():
                validation_flags.loc[zero_vals, 'errors'] += f'Zero {col}; '
                validation_flags.loc[zero_vals, 'is_valid'] = False
                print(f"  - {col}: {zero_vals.sum()} zero values flagged")
        
        print("\n[8/8] Separating Good and Bad Records")
        good_mask = validation_flags['is_valid']
        bad_mask = ~good_mask
        
        self.good_df = df_clean[good_mask].copy()
        self.bad_df = df_clean[bad_mask].copy()
        self.bad_df['Validation_Errors'] = validation_flags.loc[bad_mask, 'errors']
        
        print(f"  - Good records: {len(self.good_df):,} ({len(self.good_df)/len(self.df)*100:.2f}%)")
        print(f"  - Bad records: {len(self.bad_df):,} ({len(self.bad_df)/len(self.df)*100:.2f}%)")
        
        return self
    
    def load(self, good_path='/kaggle/working/good_data.csv', bad_path='/kaggle/working/bad_data.csv'):
        print("\nSTEP 3: LOAD")
        print("-"*80)
        
        self.good_df.to_csv(good_path, index=False)
        print(f"✓ Good data saved: {good_path}")
        print(f"  Records: {len(self.good_df):,}")
        print(f"  Columns: {len(self.good_df.columns)}")
        
        if len(self.bad_df) > 0:
            self.bad_df.to_csv(bad_path, index=False)
            print(f"\n✓ Bad data saved: {bad_path}")
            print(f"  Records: {len(self.bad_df):,}")
            print(f"  Columns: {len(self.bad_df.columns)}")
        else:
            print("\n✓ No bad records to save")
        
        return self
    
    def run(self, good_path='/kaggle/working/good_data.csv', bad_path='/kaggle/working/bad_data.csv'):
        self.extract()
        self.transform()
        self.load(good_path, bad_path)
        
        print("\n" + "="*80)
        print("ETL PIPELINE COMPLETED")
        print("="*80)
        print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"\nData Quality: {len(self.good_df)/len(self.df)*100:.2f}%")
        print("="*80)
        
        return self

etl = RetailETL('/kaggle/input/retail-transactional-dataset/retail_data.csv')
etl.run()

print("\n" + "="*80)
print("GOOD DATA SAMPLE (First 5 rows)")
print("="*80)
print(etl.good_df.head())

if len(etl.bad_df) > 0:
    print("\n" + "="*80)
    print("BAD DATA SAMPLE (First 5 rows)")
    print("="*80)
    print(etl.bad_df[['Validation_Errors']].head())
