import csv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
from collections import defaultdict, Counter
import pandas as pd
from time import time
import  json

# Database configuration
with open('database_cred.json', 'r') as f:
    postgreSql_auth = json.load(f)

def create_and_populate_tables(db_config, products_file, orders_file):

    try:
        engine= create_engine(f"{db_config['dialect']}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")

        # Create tables
        try:
            with engine.connect() as conn:  # Use a context manager for connection handling
                
                sql_query = """CREATE TABLE IF NOT EXISTS products (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        category VARCHAR(255) NOT NULL,
                        price NUMERIC(10, 2) NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS products_category_name ON public.products (category,name);
                    """
               
                sql_query2 = """CREATE TABLE IF NOT EXISTS orders (
                        id SERIAL PRIMARY KEY,
                        product_id INTEGER NOT NULL REFERENCES products(id),
                        quantity INTEGER NOT NULL,
                        created_date DATE NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS orders_created_date_idx ON public.orders (created_date,product_id,quantity);
                    """
                 
                stmt = text(sql_query)
                conn.execute(stmt)

                stmt2= text(sql_query2)
                conn.execute(stmt2)

                conn.commit()
            print("Tables 'products' and 'orders' created (or already existed).")
        except Exception as e:
            print(f"Error creating table: {e}")
        finally:
            engine.dispose()

        # Create dataframes and datatype changes
        products_df = pd.read_csv(products_file,index_col="id")        
        orders_df = pd.read_csv(orders_file,index_col="id")        

        # Populate tables  
        products_df.to_sql(name='products', con=engine, if_exists='append')
        orders_df.to_sql(name='orders', con=engine, if_exists='append')       
        

        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

def analyze_data(db_config, output_file):
    
    try:
        # Connect to PostgreSQL
        engine= create_engine(f"{db_config['dialect']}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")

        with engine.connect() as conn:
            # Query: Date with the highest number of orders
            sql_query ="""
            SELECT created_date, COUNT(*) AS order_count
            FROM orders
            GROUP BY created_date
            ORDER BY order_count DESC
            LIMIT 1;
            """
            stmt = text(sql_query)
            result = conn.execute(stmt)
            highest_orders_date, highest_orders_count = result.fetchone()

            # Query: Most demanded product
            sql_query ="""
            SELECT p.name, SUM(o.quantity) AS total_quantity
            FROM orders o
            JOIN products p ON o.product_id = p.id
            GROUP BY p.id, p.name
            ORDER BY total_quantity DESC
            LIMIT 1;
            """
            stmt = text(sql_query)
            result = conn.execute(stmt)
            most_demanded_product, most_demanded_quantity = result.fetchone()

            # Query: Top 3 most demanded categories
            sql_query ="""
            SELECT p.category, SUM(o.quantity) AS total_quantity
            FROM orders o
            JOIN products p ON o.product_id = p.id
            GROUP BY p.category
            ORDER BY total_quantity DESC
            LIMIT 3;
            """
            stmt = text(sql_query)
            result = conn.execute(stmt)
            top_categories = result.fetchall()

        # Write results to the output file
        with open(output_file, 'w', newline='') as out:
            fieldnames = ['metric', 'value']
            writer = csv.DictWriter(out, fieldnames=fieldnames)
            writer.writeheader()

            writer.writerow({'metric': 'Highest Orders Date', 'value': highest_orders_date})
            writer.writerow({'metric': 'Highest Orders Count', 'value': highest_orders_count})
            writer.writerow({'metric': 'Most Demanded Product', 'value': most_demanded_product})
            writer.writerow({'metric': 'Most Demanded Quantity', 'value': most_demanded_quantity})

            for i, (category, quantity) in enumerate(top_categories, start=1):
                writer.writerow({'metric': f'Top Category {i}', 'value': category})
                writer.writerow({'metric': f'Top Category {i} Quantity', 'value': quantity})

        print("Analysis completed and results saved.")

    except Exception as e:
        print(f"An error occurred during analysis: {e}")
    finally:
        if conn:
            conn.close()

def main():

    # File paths
    products_file = 'products.csv'
    orders_file = 'orders.csv'
    output_file = 'kpi_product_orders_by_SQL.csv'

    # Run the process
    create_and_populate_tables(postgreSql_auth, products_file, orders_file)
    analyze_data(postgreSql_auth, output_file)


if __name__ == "__main__":
    main()
