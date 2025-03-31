#***** Create a Database and populate it with csv tables *****#
# Import Libraries
import psycopg2
import pandas as pd
import numpy as np

# Create a Connection to an existing database in a PostgreSQL server
db_url = "dbname=chinook user=postgres port=5432 host=localhost password=022027"  

conn = psycopg2.connect(db_url)
cursor = conn.cursor()
# Create a new database in our server "bike_store"
query = "CREATE DATABASE bike_store"
cursor.execute(query)
conn.commit()

# Close the initial connection
conn.close()

# Create a connection to the newly created database
db_url = "dbname=bike_store user=postgres port=5432 host=localhost password=022027"
conn = psycopg2.connect(db_url)
cursor = conn.cursor()

# Create tables in the new database
## 1. categories table
category = '''
            CREATE TABLE IF NOT EXISTS categories(
            category_id integer PRIMARY KEY,
            category_name varchar(50))
           '''
cursor.execute(category)

## 2. order table
orders = '''
          CREATE TABLE IF NOT EXISTS orders(
          order_id integer PRIMARY KEY,
          customer_id integer,
          order_status integer,
          order_date date,
          required_date date,
          shipped_date date,
          store_id integer,
          staff_id integer)
         '''
cursor.execute(orders)

## 3. brands table
brands = '''
          CREATE TABLE IF NOT EXISTS customers(
          brand_id integer PRIMARY KEY,
          brand_name varchar(50))
         '''
cursor.execute(brands)

## 4. customers table
customers = '''
            CREATE TABLE IF NOT EXISTS customers(
            customer_id integer PRIMARY KEY,
            first_name varchar(50),
            last_name varchar(50),
            phone varchar(50),
            email varchar(50),
            street varchar(30),
            city varchar(50),
            state varchar(50),
            zip_code integer)
            '''
cursor.execute(customers)

## 5. order_items table
order_items = '''
                CREATE TABLE IF NOT EXISTS order_items(
                order_id integer,
                item_id integer,
                product_id integer,
                quantity integer,
                list_price numeric,
                discount numeric)
              '''
cursor.execute(order_items)

## 6. products table
products = '''
            CREATE TABLE IF NOT EXISTS products(
            product_id integer PRIMARY KEY,
            product_name varchar(50),
            brand_id integer,
            category_id integer,
            model_year integer,
            list_year integer)
           '''
cursor.execute(products)

## 7. staffs table
staffs = '''
          CREATE TABLE IF NOT EXISTS staffs(
          staff_id integer PRIMARY KEY,
          first_name varchar(50),
          last_name varchar(50),
          email varchar(50),
          phone varchar(50),
          active boolean,
          store_id integer,
          manager_id integer)
         '''
cursor.execute(staffs)

## 8. stocks table
stocks = '''
          CREATE TABLE IF NOT EXISTS stocks(
          stock_id integer PRIMARY KEY,
          product_id integer,
          quantity integer)
         '''
cursor.execute(stocks)

## 9. stores table
stores = '''
          CREATE TABLE IF NOT EXISTS stores(
          store_id integer PRIMARY KEY,
          store_name varchar(50),
          phone varchar(50),
          email varchar(100),
          street varchar(50),
          city varchar(50),
          state varchar(10),
          zip_code integer)
         '''
cursor.execute(stores)
