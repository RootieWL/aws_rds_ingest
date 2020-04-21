import boto3
import psycopg2
rds = boto3.client('rds', region_name='ap-southeast-1')

try:
    dbs = rds.describe_db_instances()
    for db in dbs['DBInstances']:
        print(("%s@%s:%s %s") % (
            db['MasterUsername'],
            db['Endpoint']['Address'],
            db['Endpoint']['Port'],
            db['DBInstanceStatus']))
except Exception as error:
    print(error)

connection = psycopg2.connect(
                database="postgres",
                user="XXXX",
                password="XXXX",
                host="XXXX",
                port='5432'
            )

cursor = connection.cursor()

cursor.execute("""CREATE TABLE sector_daily (
                sn integer PRIMARY KEY,
                rank_A float,
                rank_B float,
                rank_C float,
                rank_D float,
                rank_E float,
                rank_F float,
                rank_G float,
                rank_H float,
                rank_I float,
                rank_J float,
                sector_name varchar(30),
                date date)""")

cursor.execute("""CREATE TABLE stock_hist (
                sn integer PRIMARY KEY,
                date date,
                daily_price_open float,
                daily_price_high float,
                daily_price_low float,
                daily_price_close float,
                daily_price_volume float,
                stock_symbol varchar(10))""")

cursor.execute("""CREATE TABLE index_hist (
                sn integer PRIMARY KEY,
                date date,
                daily_price_open float,
                daily_price_high float,
                daily_price_low float,
                daily_price_close float,
                daily_price_volume float,
                index_symbol varchar(10))""")

with open('sector_daily.csv', 'r') as row:
    next(row) 
    cursor.copy_from(row, 'sector_daily', sep=',')

with open('stock_hist.csv', 'r') as row:
    next(row) 
    cursor.copy_from(row, 'stock_hist', sep=',')

with open('index_hist.csv', 'r') as row:
    next(row) 
    cursor.copy_from(row, 'index_hist', sep=',')

connection.commit()
print('Tables Created')