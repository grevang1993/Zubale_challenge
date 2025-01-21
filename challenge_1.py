import csv
import os

def consolidate_csv_data(products_file, orders_file, output_file):
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
        fieldnames = ['order_created_date', 'order_id', 'product_name', 'quantity', 'total_price']
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()

        for row in order_reader:
            product_id = row['product_id']
            if product_id in products:
                product_name = products[product_id]['name']
                price = products[product_id]['price']
                quantity = int(row['quantity'])
                total_price = price * quantity

                writer.writerow({
                    'order_created_date': row['created_date'],
                    'order_id': row['id'],
                    'product_name': product_name,
                    'quantity': quantity,
                    'total_price': round(total_price, 2)
                })

def main():
    # Define file paths
    products_file = 'products.csv'
    orders_file = 'orders.csv'
    output_file = 'order_full_information.csv'

    # Run the consolidation
    consolidate_csv_data(products_file, orders_file, output_file)

    # Verify output file creation
    if os.path.exists(output_file):
        print(f"The file '{output_file}' has been created successfully.")
    else:
        print("Error: The output file could not be created.")


if __name__ == "__main__":
    main()