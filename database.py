###### Create a Database and populate it with csv tables #####

# Import Libraries
import psycopg2
import pandas as pd
import numpy as np
import csv

# Define functions
## Define a function for Creating a Database in a Server
def create_database(db_url, db_name):
    ''' Create isolation level and connect to database '''
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    conn = psycopg2.connect(db_url)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    ''' Create a cursor for executing querries '''
    cursor = conn.cursor()
    
    ''' Create a database '''
    query = f"CREATE DATABASE {db_name}"
    try:
        cursor.execute(query)
        print("Database Successfully Created")
    except Exception as e:
        print(f"Database Creation Unsuccessful with: {e}")

    ''' Commit changes and Close connection '''
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
    ''' Create a connection to the database and a cursor object '''
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    ''' Clean the table for any existing datapoints '''
    truncate_query = f"TRUNCATE TABLE {table_name}"
    cursor.execute(truncate_query)

    ''' Insert data into the table '''
    try:
        cursor.executemany(query, values)
        print("Insert Successfull")
    except Exception as e:
        f"Insert Unsuccessfull with {e}."

    ''' Commit Changes and Close connection '''
    conn.commit()
    conn.close()

# Creat a Database in the PosgreSQL Server  
## Assign the database credential to a string "db_url"
db_url = "dbname=chinook user=postgres port=5435 host=localhost password=022027"  

## Create a new database in our server "bike_store" - use a function defined above
create_database(db_url, "bike_store")

# Create tables in the new database
## Create a connection to the newly created database
db_url = "dbname=bike_store user=postgres port=5432 host=localhost password=022027"
conn = psycopg2.connect(db_url)
cursor = conn.cursor()

## 1. categories table
category = '''
            CREATE TABLE IF NOT EXISTS categories(
            category_id integer PRIMARY KEY,
            category_name varchar(50) UNIQUE NOT NULL
            );
           '''
cursor.execute(category)

## 2. order table
orders = '''
          CREATE TABLE IF NOT EXISTS orders(
          order_id integer PRIMARY KEY,
          customer_id integer NOT NULL,
          order_status integer NOT NULL,
          order_date date NOT NULL,
          required_date date NOT NULL,
          shipped_date date,
          store_id integer NOT NULL,
          staff_id integer NOT NULL
          );
         '''
cursor.execute(orders)

## 3. brands table
brands = '''
          CREATE TABLE IF NOT EXISTS brands(
          brand_id integer PRIMARY KEY,
          brand_name varchar(50) UNIQUE NOT NULL
          );
         '''
cursor.execute(brands)

## 4. customers table
customers = '''
            CREATE TABLE IF NOT EXISTS customers(
            customer_id integer PRIMARY KEY,
            first_name varchar(50) NOT NULL,
            last_name varchar(50) NOT NULL,
            phone varchar(50),
            email varchar(50) NOT NULL,
            street varchar(30) UNIQUE NOT NULL,
            city varchar(50) NOT NULL,
            state varchar(50) NOT NULL,
            zip_code integer NOT NULL
            );
            '''
cursor.execute(customers)

## 5. order_items table
order_items = '''
                CREATE TABLE IF NOT EXISTS order_items(
                order_id integer NOT NULL,
                item_id integer NOT NULL,
                product_id integer NOT NULL,
                quantity integer NOT NULL,
                list_price numeric NOT NULL,
                discount numeric NOT NULL
                );
              '''
cursor.execute(order_items)

## 6. products table
products = '''
            CREATE TABLE IF NOT EXISTS products(
            product_id integer PRIMARY KEY,
            product_name varchar(50) NOT NULL,
            brand_id integer NOT NULL,
            category_id integer NOT NULL,
            model_year integer NOT NULL,
            list_year integer NOT NULL
            );
           '''
cursor.execute(products)

## 7. staffs table
staffs = '''
          CREATE TABLE IF NOT EXISTS staffs(
          staff_id integer PRIMARY KEY,
          first_name varchar(50) UNIQUE NOT NULL,
          last_name varchar(50) UNIQUE NOT NULL,
          email varchar(50) UNIQUE NOT NULL,
          phone varchar(50) UNIQUE NOT NULL,
          active boolean NOT NULL,
          store_id integer NOT NULL,
          manager_id integer
          );
          '''
cursor.execute(staffs)

## 8. stocks table
stocks = '''
          CREATE TABLE IF NOT EXISTS stocks(
          store_id integer NOT NULL,
          product_id integer NOT NULL,
          quantity integer NOT NULL
          );
         '''
cursor.execute(stocks)

## 9. stores table
stores = '''
          CREATE TABLE IF NOT EXISTS stores(
          store_id integer PRIMARY KEY,
          store_name varchar(50) UNIQUE NOT NULL,
          phone varchar(50) UNIQUE NOT NULL,
          email varchar(100) UNIQUE NOT NULL,
          street varchar(50) UNIQUE NOT NULL,
          city varchar(50) UNIQUE NOT NULL,
          state varchar(10) UNIQUE NOT NULL,
          zip_code integer UNIQUE NOT NULL
          );
         '''
cursor.execute(stores)

## Commit Changes and Close connection
conn.commit()
conn.close()
