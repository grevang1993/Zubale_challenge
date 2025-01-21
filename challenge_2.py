import csv
# from collections import defaultdict, Counter
import os
import requests
import pandas as pd
import json

# Database configuration
with open('freecurrency_cred.json', 'r') as f:
    freecurrency_auth = json.load(f)

# Chanllenge 2.1:
def fetch_conversion_rate(api_key, from_currency, to_currency):
    url = f"https://api.freecurrencyapi.com/v1/latest?apikey={api_key}&currencies={to_currency}&base_currency={from_currency}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['data'][to_currency]
    else:
        raise Exception(f"Failed to fetch conversion rate: {response.status_code} {response.text}")

def consolidate_csv_data_with_conversion(products_file, orders_file, output_file, conversion_rate):
    # Read products data into a dictionary
    products = {}
    with open(products_file, 'r') as pf:
        reader = csv.DictReader(pf)
        for row in reader:
            products[row['id']] = {
                'name': row['name'],
                'price': float(row['price'])
            }

    # Process orders and write consolidated data to the output file
    with open(orders_file, 'r') as of, open(output_file, 'w', newline='') as out:
        order_reader = csv.DictReader(of)
        fieldnames = ['order_created_date', 'order_id', 'product_name', 'quantity', 'total_price_br', 'total_price_us']
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()

        for row in order_reader:
            product_id = row['product_id']
            if product_id in products:
                product_name = products[product_id]['name']
                price_brl = products[product_id]['price']
                quantity = int(row['quantity'])
                total_price_br = price_brl * quantity
                total_price_us = total_price_br * conversion_rate

                writer.writerow({
                    'order_created_date': row['created_date'],
                    'order_id': row['id'],
                    'product_name': product_name,
                    'quantity': quantity,
                    'total_price_br': round(total_price_br, 2),
                    'total_price_us': round(total_price_us, 2)
                })

# Chanllenge 2.2:
def analyze_orders(products_file, orders_file, output_file):

    # Create dataframes
    products_df = pd.read_csv(products_file)
    orders_df = pd.read_csv(orders_file)

    # Merge the DataFrames on product_id
    merged_df = orders_df.merge(products_df, left_on='product_id', right_on='id', how='left')

    # Calculate the total sales value for each order
    merged_df['total_sales'] = merged_df['quantity'] * merged_df['price']

    # Find the date with the highest number of orders
    orders_by_date = merged_df.groupby('created_date').size()
    highest_orders_date = orders_by_date.idxmax()
    highest_orders_count = orders_by_date.max()

    # Find the most sold product and its total sales value
    product_sales = merged_df.groupby('name').agg({'quantity': 'sum', 'total_sales': 'sum'})
    most_sold_product = product_sales['quantity'].idxmax()
    most_sold_quantity = product_sales['quantity'].max()
    most_sold_total_sales = product_sales.loc[most_sold_product, 'total_sales']

    # Find the top 3 most demanded categories
    category_demand = merged_df.groupby('category')['quantity'].sum().sort_values(ascending=False)
    top_3_categories = category_demand.head(3)

    # Prepare the results for saving
    results = {
        'Metric': [
            'Date with Highest Orders',
            'Most Sold Product',
            'Most Sold Product Total Sales',
            'Top 3 Categories'
        ],
        'Value': [
            f"{highest_orders_date} ({highest_orders_count} orders)",
            f"{most_sold_product} ({most_sold_quantity} units)",
            f"${most_sold_total_sales:.2f}",
            ', '.join([f"{cat} ({top_3_categories[cat]} units)" for cat in top_3_categories.index])
        ]
    }

    # Save the results to a new CSV file
    results_df = pd.DataFrame(results)
    results_df.to_csv(output_file, index=False)

     

def main():
    # Define file paths
    products_file = 'products.csv'
    orders_file = 'orders.csv'
    output_file = 'fixed_order_full_information.csv'
    output_file_2 = 'kpi_product_orders.csv'

    # Replace with your Free Currency API key
    api_key = freecurrency_auth['api_key']

    try:
        # Fetch conversion rate from BRL to USD
        conversion_rate = fetch_conversion_rate(api_key, 'BRL', 'USD')
        print(f"Conversion rate (BRL to USD): {conversion_rate}")

        # Run the consolidation with conversion
        consolidate_csv_data_with_conversion(products_file, orders_file, output_file, conversion_rate)

        # Verify output file creation
        if os.path.exists(output_file):
            print(f"The file '{output_file}' has been created successfully.")
        else:
            print("Error: The output file could not be created.")


        # Run the analysis
        analyze_orders(products_file, orders_file, output_file_2)

        # Verify output file creation
        if os.path.exists(output_file_2):
            print(f"The file '{output_file_2}' has been created successfully.")
        else:
            print("Error: The output file could not be created.")

    except Exception as e:
        print(f"An error occurred: {e}")



if __name__ == "__main__":
    main()