import sqlite3

db_file_path = '/kaggle/working/retail_database.db'

conn = sqlite3.connect(db_file_path)

cursor = conn.cursor()

cursor.execute("SELECT * FROM customer_master LIMIT 5;")
customer_master_data = cursor.fetchall()
print("Customer Master Data:")
for row in customer_master_data:
    print(row)

cursor.execute("SELECT * FROM product_master LIMIT 5;")
product_master_data = cursor.fetchall()
print("\nProduct Master Data:")
for row in product_master_data:
    print(row)

cursor.execute("SELECT * FROM sales_transactions LIMIT 5;")
sales_transactions_data = cursor.fetchall()
print("\nSales Transactions Data:")
for row in sales_transactions_data:
    print(row)

cursor.execute("SELECT * FROM customer_analytics LIMIT 5;")
customer_analytics_data = cursor.fetchall()
print("\nCustomer Analytics Data:")
for row in customer_analytics_data:
    print(row)

cursor.execute("SELECT * FROM loyalty_transactions LIMIT 5;")
loyalty_transactions_data = cursor.fetchall()
print("\nLoyalty Transactions Data:")
for row in loyalty_transactions_data:
    print(row)

conn.close()
