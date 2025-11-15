import pandas as pd
from sqlalchemy import create_engine

file_path = '/kaggle/input/clean-retail-data/good_data.csv'  # Replace with the actual path
df = pd.read_csv(file_path)

engine = create_engine('sqlite:///retail_database.db', echo=True) 

customer_columns = ['Customer_ID', 'Name', 'Email', 'Phone', 'Address', 'City', 'State', 'Zipcode', 'Country', 'Age', 'Gender', 'Income', 'Customer_Segment']
customer_data = df[customer_columns].drop_duplicates()

customer_data.to_sql('customer_master', con=engine, if_exists='replace', index=False)

product_columns = ['Product_Category', 'Product_Brand', 'Product_Type']

df['Product_ID'] = df['Product_Category'] + "_" + df['Product_Brand']
product_data = df[['Product_ID'] + product_columns].drop_duplicates()

product_data.to_sql('product_master', con=engine, if_exists='replace', index=False)

sales_columns = ['Transaction_ID', 'Customer_ID', 'Total_Amount', 'Product_Category', 'Shipping_Method', 'Payment_Method', 'Order_Status', 'Ratings', 'Feedback']
sales_data = df[sales_columns]

sales_data.to_sql('sales_transactions', con=engine, if_exists='replace', index=False)

customer_analytics = df.groupby('Customer_ID').agg(
    total_spend=('Total_Amount', 'sum'),
    avg_rating=('Ratings', 'mean'),
    total_purchases=('Total_Purchases', 'sum')
).reset_index()

customer_analytics.to_sql('customer_analytics', con=engine, if_exists='replace', index=False)

df['Loyalty_Points'] = df['Total_Amount'] * 0.1
loyalty_columns = ['Transaction_ID', 'Customer_ID', 'Loyalty_Points']
loyalty_data = df[loyalty_columns]

loyalty_data.to_sql('loyalty_transactions', con=engine, if_exists='replace', index=False)