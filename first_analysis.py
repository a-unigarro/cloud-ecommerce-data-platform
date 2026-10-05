import pandas as pd


df= pd.read_csv('data/raw/olist/olist_orders_dataset.csv')
#print(df.shape)
#print(df.dtypes)
#print(df.head())



df= pd.read_csv('data/raw/olist/olist_customers_dataset.csv')
#print(df.shape)
#print(df.dtypes)
#print(df.head())


df= pd.read_csv('data/raw/olist/olist_order_items_dataset.csv')
#print(df.shape)
#print(df.dtypes)
#print(df.head())


df= pd.read_csv('data/raw/olist/olist_order_payments_dataset.csv')
#print(df.shape)
#print(df.dtypes)
#print(df.head())


df= pd.read_csv('data/raw/olist/olist_geolocation_dataset.csv')
print(df.shape)
print(df.dtypes)
print(df.head())

