import pandas as pd
from sqlalchemy import create_engine

# Step 1: Load the dataset
file_path = '/kaggle/input/clean-retail-data/good_data.csv'  # Replace with the actual path
df = pd.read_csv(file_path)

# Step 2: Create a connection to the SQLite database 
engine = create_engine('sqlite:///retail_database.db', echo=True) 

# Step 3: Split the Data and Insert into Respective Tables

# 3.1: customer_master table
customer_columns = ['Customer_ID', 'Name', 'Email', 'Phone', 'Address', 'City', 'State', 'Zipcode', 'Country', 'Age', 'Gender', 'Income', 'Customer_Segment']
customer_data = df[customer_columns].drop_duplicates()

# Insert into customer_master table
customer_data.to_sql('customer_master', con=engine, if_exists='replace', index=False)

# 3.2: product_master table
product_columns = ['Product_Category', 'Product_Brand', 'Product_Type']
# Generate a Product_ID based on the combination of Product Category and Product Brand if not already present
df['Product_ID'] = df['Product_Category'] + "_" + df['Product_Brand']
product_data = df[['Product_ID'] + product_columns].drop_duplicates()

# Insert into product_master table
product_data.to_sql('product_master', con=engine, if_exists='replace', index=False)

# 3.3: sales_transactions table
sales_columns = ['Transaction_ID', 'Customer_ID', 'Total_Amount', 'Product_Category', 'Shipping_Method', 'Payment_Method', 'Order_Status', 'Ratings', 'Feedback']
sales_data = df[sales_columns]

# Insert into sales_transactions table
sales_data.to_sql('sales_transactions', con=engine, if_exists='replace', index=False)

# 3.4: customer_analytics table (e.g., total spend, average rating)
customer_analytics = df.groupby('Customer_ID').agg(
    total_spend=('Total_Amount', 'sum'),
    avg_rating=('Ratings', 'mean'),
    total_purchases=('Total_Purchases', 'sum')
).reset_index()

# Insert into customer_analytics table
customer_analytics.to_sql('customer_analytics', con=engine, if_exists='replace', index=False)

# 3.5: loyalty_transactions table (loyalty points based on total amount spent)
df['Loyalty_Points'] = df['Total_Amount'] * 0.1  # Assuming 10% of total amount is the loyalty points
loyalty_columns = ['Transaction_ID', 'Customer_ID', 'Loyalty_Points']
loyalty_data = df[loyalty_columns]

# Insert into loyalty_transactions table
loyalty_data.to_sql('loyalty_transactions', con=engine, if_exists='replace', index=False)