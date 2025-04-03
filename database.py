###### Create a Database and populate it with csv tables #####

# Import Libraries
import psycopg2
import pandas as pd
import numpy as np
import csv
import os

# Define functions
## Define a function for Creating a Database in a Server
def create_database(db_url, db_name):
    """ Create isolation level and connect to database """
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    conn = psycopg2.connect(db_url)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    """ Create a cursor for executing queries """
    cursor = conn.cursor()
    
    """ Create a database """
    query = f"CREATE DATABASE {db_name}"
    try:
        cursor.execute(query)
        print("Database Successfully Created")
    except Exception as e:
        print(f"Database Creation Unsuccessful with: {e}")

    """ Commit changes and Close connection """
    conn.commit()
    conn.close()

## Define a Function for Preparing data tables into a list of lists
def get_lists(file):
    with open(file, 'r') as opened_file:
        next(opened_file)
        read_file = csv.reader(opened_file)
        data_file = list(read_file)
    return data_file

## Define a function for Inserting data into database tables
def insert_data(db_url, table_name, query, values):
    """ Create a connection to the database and a cursor object """
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    """ Clean the table for any existing datapoints """
    truncate_query = f"TRUNCATE TABLE {table_name}"
    cursor.execute(truncate_query)

    """ Insert data into the table """
    try:
        cursor.executemany(query, values)
        print(f"Inserting data in {table_name} table is Successful!\n")
    except Exception as e:
        print(f"Insert Unsuccessful with {e}.")

    """ Commit Changes and Close connection """
    conn.commit()
    conn.close()

# Create a Database in the Postgresql Server
## Assign the database credential to a string "db_url"
password = os.environ.get('PASSWORD')
username = os.environ.get('USERNAME')
db_url = f"dbname=chinook user={username} port=5432 host=localhost password={password}"  

## Create a new database in our server "bike_store" - use a function defined above
create_database(db_url, "bike_store")

# Create tables in the new database
## Create a connection to the newly created database
db_url = f"dbname=bike_store user={username} port=5432 host=localhost password={password}"
conn = psycopg2.connect(db_url)
cursor = conn.cursor()

## 1. categories table
category = """
    CREATE TABLE IF NOT EXISTS categories(
        category_id integer PRIMARY KEY,
        category_name varchar(20) UNIQUE NOT NULL
        );
"""
cursor.execute(category)

## 2. order table
orders = """
    CREATE TABLE IF NOT EXISTS orders(
        order_id integer PRIMARY KEY,
        customer_id integer NOT NULL,
        order_status integer NOT NULL,
        order_date varchar(20) NOT NULL,
        required_date varchar(20) NOT NULL,
        shipped_date varchar(20),
        store_id integer NOT NULL,
        staff_id integer NOT NULL
        );
"""
cursor.execute(orders)

## 3. brands table
brands = """
    CREATE TABLE IF NOT EXISTS brands(
        brand_id integer PRIMARY KEY,
        brand_name varchar(15) UNIQUE NOT NULL
        );
"""
cursor.execute(brands)

## 4. customers table
customers = """
    CREATE TABLE IF NOT EXISTS customers(
        customer_id integer PRIMARY KEY,
        first_name varchar(15) NOT NULL,
        last_name varchar(15) NOT NULL,
        phone varchar(15),
        email varchar(40) NOT NULL,
        street varchar(30) UNIQUE NOT NULL,
        city varchar(25) NOT NULL,
        state varchar(2) NOT NULL,
        zip_code integer NOT NULL
        );
"""
cursor.execute(customers)

## 5. order_items table
order_items = """
    CREATE TABLE IF NOT EXISTS order_items(
        order_id integer NOT NULL,
        item_id integer NOT NULL,
        product_id integer NOT NULL,
        quantity integer NOT NULL,
        list_price numeric NOT NULL,
        discount numeric NOT NULL
        );
"""
cursor.execute(order_items)

## 6. products table
products = """
    CREATE TABLE IF NOT EXISTS products(
        product_id integer PRIMARY KEY,
        product_name varchar(60) NOT NULL,
        brand_id integer NOT NULL,
        category_id integer NOT NULL,
        model_year integer NOT NULL,
        list_year numeric NOT NULL
        );
"""
cursor.execute(products)

## 7. staffs table
staffs = """
    CREATE TABLE IF NOT EXISTS staffs(
        staff_id integer PRIMARY KEY,
        first_name varchar(10) UNIQUE NOT NULL,
        last_name varchar(10) UNIQUE NOT NULL,
        email varchar(30) UNIQUE NOT NULL,
        phone varchar(15) UNIQUE NOT NULL,
        active boolean NOT NULL,
        store_id integer NOT NULL,
        manager_id varchar(10)
        );
"""
cursor.execute(staffs)

## 8. stocks table
stocks = """
    CREATE TABLE IF NOT EXISTS stocks(
        store_id integer NOT NULL,
        product_id integer NOT NULL,
        quantity integer NOT NULL
        );
"""
cursor.execute(stocks)

## 9. stores table
stores = """
    CREATE TABLE IF NOT EXISTS stores(
        store_id integer PRIMARY KEY,
        store_name varchar(20) UNIQUE NOT NULL,
        phone varchar(15) UNIQUE NOT NULL,
        email varchar(20) UNIQUE NOT NULL,
        street varchar(20) UNIQUE NOT NULL,
        city varchar(10) UNIQUE NOT NULL,
        state varchar(2) UNIQUE NOT NULL,
        zip_code integer UNIQUE NOT NULL
        );
"""
cursor.execute(stores)

## Commit Changes and Close connection
conn.commit()
conn.close()

# Insert values into tables
## Insert brands.csv into brands table in the database
brands_csv = "data/brands.csv"
brands_values = get_lists(brands_csv)
query = "INSERT INTO brands VALUES(%s, %s)"

insert_data(db_url, "brands", query, brands_values)

## Insert categories.csv into categories table in the database
categories_csv = "data/categories.csv"
categories_values = get_lists(categories_csv)
query = "INSERT INTO categories VALUES(%s, %s)"

insert_data(db_url, "categories", query, categories_values)

## Insert customers.csv into customers table in the database
customers_csv = "data/customers.csv"
customers_values = get_lists(customers_csv)
query = "INSERT INTO customers VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"

insert_data(db_url, "customers", query, customers_values)

## Insert order.csv into orders table in the database
orders_csv = "data/orders.csv"
orders_values = get_lists(orders_csv)
query = "INSERT INTO orders VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"

insert_data(db_url, "orders", query, orders_values)

## Insert staffs.csv into staffs table in the database
staffs_csv = "data/staffs.csv"
staffs_values = get_lists(staffs_csv)
query = "INSERT INTO staffs VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"

insert_data(db_url, "staffs", query, staffs_values)

## Insert stores.csv into stores table in the database
stores_csv = "data/stores.csv"
stores_values = get_lists(stores_csv)
query = "INSERT INTO stores VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"

insert_data(db_url, "stores", query, stores_values)

## Insert order_items.csv into order_items table in the database
order_items_csv = "data/order_items.csv"
order_items_values = get_lists(order_items_csv)
query = "INSERT INTO order_items VALUES(%s, %s, %s, %s, %s, %s)"

insert_data(db_url, "order_items", query, order_items_values)

## Insert products.csv into products table in the database
products_csv = "data/products.csv"
products_values = get_lists(products_csv)
query = "INSERT INTO products VALUES(%s, %s, %s, %s, %s, %s)"

insert_data(db_url, "products", query, products_values)

## Insert stocks.csv into stocks table in the database
stocks_csv = "data/stocks.csv"
stocks_values = get_lists(stocks_csv)
query = "INSERT INTO stocks VALUES(%s, %s, %s)"

insert_data(db_url, "stocks", query, stocks_values)

################################################################