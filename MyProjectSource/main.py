import logging
import pandas as pd
#from tasks import Tasks 
import psycopg2 as psy
import sqlalchemy

import json
#import models.sample as models
#from database.dw_session import dwEngine
#from database.oltp_session import oltpEngine
# Python program to read
# json file


f = open('C:/Users/bniet/github-classroom/DSSA-Stockton-University/dssa_dw_final_project_nietzolb/.config/settings.json')
# returns JSON object as
# a dictionary
config = json.load(f)
path=config['sqltools.connections'][0]
print(path)
dbname=((path['database']))
usrname=((path['username']))
pwd=((path['password']))
conport=path['port']
conhost=path['server']
# Closing file
f.close()

dbconpar='dssa_dw_final_project_nietzolb/.config/settings.json'

def setup(path):
    conn = psy.connect("dbname=dbname, user=usrname, password=pwd")
    cur = conn.cursor()
    return cur

setup(path)

def extract():
    def schema():
        return
    def tables():
        return
    def fields():
        return
    def relationships():
        return
def transform():
    return
def load():
    return    
def teardown():
    cur.close()
    conn.close()  
    
    return        
    

if __name__ == "__main__":
    main()
    
    

"""

task1=tasks.Task(create_cursor)
task1.run(path=DbConfig, section='postgresql')


logging.basicConfig(level=logging.INFO, filename = "debug.logs")
logger = logging.getLogger(__name__)


def create_cursor(path:str, section:str) -> Cursor:
    client = PostgresClient()
    conn = client.connect_from_config(path, section, autocommit=True)
    cursor = conn.cursor()
    return cursor


def setup(engine, schema_name):
    # Connect to the database, this is done by importing the engine above

    # Define the schema, equiv of CREATE SCHEMA IF NOT EXIST...
    if not engine.dialect.has_schema(engine, schema_name):
        engine.execute(CreateSchema(schema_name))

    # Create Tables, make sure all models are defined and imported see the sample.py in the app/models/ directory 
    models.Base.metadata.create_all(bind=engine, checkfirst= True)

def extract(table_name, engine):
    # Extract the tables using pandas and the SQLAlchemy engine object
    df = pd.read_sql_table(table_name, con=engine.connect())
    return df

def cust_transform(df):
    transf_df = df[['id','name', 'address', 'zip code', 'city', 'country']]
    transf_df.rename(columns= {'zip code':'zip'}, inplace=True)
    return transf_df

def pay_transform(df):
    transf_df = df[['payment_id','customer_id', 'amount', 'payment_date']]
    return transf_df

def get_year_from_col(df, column):
    df['year'] = df[column].dt.year
    return df

def get_month_from_col(df, column):
    df['month'] = df[column].dt.month
    return df

def get_sum_aggregations(df, by):
    df = df.groupby(by=by).sum()
    return df

def monthly_rev_base(df_A, df_B, join:str, l_on, r_on):
    # Building the base fact table
    df = df_A.merge(right=df_B, how=join, left_on=l_on, right_on=r_on)
    df = get_year_from_col(df, column='payment_date')
    df = get_month_from_col(df, column='payment_date')
    df = df[['id', 'year', 'month', 'amount']]

    # Running the aggregations
    tranf_df = get_sum_aggregations(df, by=['id', 'year', 'month'])
    tranf_df = tranf_df.reset_index()
    tranf_df.rename(columns= {
        'amount':'total_sales',
        'id':'customer_id'
        }, inplace=True)
    tranf_df['id'] = tranf_df.index
    
    return tranf_df
    
def load(df, target, schema, engine, use_index=False):


    df.to_sql(
        name=target,
        con=engine,
        schema=schema,
        if_exists='append', 
        index=use_index,
        method='multi'
    )

def teardown(engine):
    # Close the Connection
    engine.dispose()

def main() -> None:
    # Setup the upstream schema and database connections
    setup(dwEngine, schema_name='prof')

    # Extract Data from OLTP    
    cust = extract(table_name='customer_list', engine=oltpEngine)
    pay = extract(table_name='payment', engine=oltpEngine)

    # Transform Data for to match target schema
    transf_cust = cust_transform(cust)
    transf_pay = pay_transform(pay)

    monthly_revenue = monthly_rev_base(
        df_A=transf_pay,
        df_B=transf_cust,
        join='left',
        l_on='customer_id',
        r_on='id')

    # Load Table Objects to Data Warehouse
    # Load the customer dimension table
    load(
        df=transf_cust, 
        target='dim_customer', 
        schema='prof', 
        engine=dwEngine)
    # Load the payments dimension table
    load(
        df=transf_pay, 
        target='dim_payments', 
        schema='prof',
        engine=dwEngine)
    # Load the monthly_revenue fact table
    load(
        df=monthly_revenue, 
        target='monthly_revenue', 
        schema='prof', 
        engine=dwEngine)

    # Close any open connections
    teardown(oltpEngine)
    teardown(dwEngine)
"""
    
