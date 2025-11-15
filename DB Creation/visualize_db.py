import sqlite3

# Path to the SQLite database file in Kaggle output directory
db_file_path = '/kaggle/working/retail_database.db'

# Connect to the SQLite database
conn = sqlite3.connect(db_file_path)

# Create a cursor object to interact with the database
cursor = conn.cursor()

# 1. Fetch first 5 records from the 'customer_master' table
cursor.execute("SELECT * FROM customer_master LIMIT 5;")
customer_master_data = cursor.fetchall()
print("Customer Master Data:")
for row in customer_master_data:
    print(row)

# 2. Fetch first 5 records from the 'product_master' table
cursor.execute("SELECT * FROM product_master LIMIT 5;")
product_master_data = cursor.fetchall()
print("\nProduct Master Data:")
for row in product_master_data:
    print(row)

# 3. Fetch first 5 records from the 'sales_transactions' table
cursor.execute("SELECT * FROM sales_transactions LIMIT 5;")
sales_transactions_data = cursor.fetchall()
print("\nSales Transactions Data:")
for row in sales_transactions_data:
    print(row)

# 4. Fetch first 5 records from the 'customer_analytics' table
cursor.execute("SELECT * FROM customer_analytics LIMIT 5;")
customer_analytics_data = cursor.fetchall()
print("\nCustomer Analytics Data:")
for row in customer_analytics_data:
    print(row)

# 5. Fetch first 5 records from the 'loyalty_transactions' table
cursor.execute("SELECT * FROM loyalty_transactions LIMIT 5;")
loyalty_transactions_data = cursor.fetchall()
print("\nLoyalty Transactions Data:")
for row in loyalty_transactions_data:
    print(row)

# Close the connection when done
conn.close()
