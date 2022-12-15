import logging
import pandas as pd
import pandas.io.sql as sqlio
#from tasks import Tasks 
import psycopg2 as psy
import sqlalchemy
import pypika
import json
import queue


# open json file containing Database connection information
f = open('C:/Users/bniet/github-classroom/DSSA-Stockton-University/dssa_dw_final_project_nietzolb/.config/settings.json')
# returns JSON object as a dictionary
config = json.load(f)
# Closing file
f.close()

def setup(config):
    path=config['sqltools.connections'][0]
    conn = psy.connect(dbname=path.get('database'), user=path.get('username'), password=path.get('password'), host=path.get('server'), port=path.get('port'))
    return conn
#establish connection and cursor
dvdcon=setup(config)
dvdcur=dvdcon.cursor()    

# Full customer table from dvd database in a pandas dataframe
# Build the Customer dimension table for the data warehouse
cust_data_pd=sqlio.read_sql_query("Select customer_id,first_name,last_name,email from Customer",dvdcon) 
cust_data_pd['name'] = cust_data_pd['first_name'].map(str) + cust_data_pd['last_name'].map(str)
cust_data_pd.rename(columns = {'customer_id':'sk_customer'}, inplace = True)
dim_cust=cust_data_pd[['sk_customer','name', 'email']]

# Build the Date dimension table for the data warehouse
rent_data_pd=sqlio.read_sql_query("Select rental_date from Rental",dvdcon) 
rent_data_pd['year']=rent_data_pd['rental_date'].dt.year
rent_data_pd['quarter']=rent_data_pd['rental_date'].dt.quarter
rent_data_pd['month']=rent_data_pd['rental_date'].dt.month
rent_data_pd['day']=rent_data_pd['rental_date'].dt.day
dim_date=rent_data_pd
dim_date.rename(columns = {'rental_date':'sk_date'}, inplace = True)


# Build the Film dimension table for the data warehouse
film_data_pd=sqlio.read_sql_query("Select film_id, rating, length, rental_duration, release_year, title from Film",dvdcon) 
film_data_pd.rename(columns = {'film_id':'sk_film'}, inplace = True)
film_data_pd.rename(columns = {'rating':'rating_code'}, inplace = True)
film_data_pd.rename(columns = {'length':'film_duration'}, inplace = True)
film_data_pd.rename(columns = {'rental_duration':'rental_duration'}, inplace = True)
film_data_pd['language']=sqlio.read_sql_query("Select name from Language",dvdcon) 
dim_film=film_data_pd

# Build the Staff dimension table for the data warehouse
staff_data_pd=sqlio.read_sql_query("Select staff_id,first_name,last_name,email from Staff",dvdcon) 
staff_data_pd['name'] = staff_data_pd['first_name'].map(str) + staff_data_pd['last_name'].map(str)
staff_data_pd.rename(columns = {'staff_id':'sk_staff'}, inplace = True)
dim_staff=staff_data_pd[['sk_staff','name', 'email']]

# Build the Staff dimension table for the data warehouse
store_data_pd=sqlio.read_sql_query("Select store_id from Store",dvdcon)
store_data_pd['name']= staff_data_pd['name']
store_data_pd['address']=sqlio.read_sql_query("Select address from Address",dvdcon)
store_data_pd['state']=sqlio.read_sql_query("Select district from Address",dvdcon)
store_data_pd['city']=sqlio.read_sql_query("Select city from City",dvdcon)
store_data_pd['country']=sqlio.read_sql_query("Select country from Country",dvdcon)
dim_store=store_data_pd


# Build the FACT_RENTAL fact table for the data warehouse
FACT_RENTAL=[]
FACT_RENTAL['sk_customer'] = dim_cust['sk_customer']
FACT_RENTAL['sk_date'] = dim_date['sk_date']
FACT_RENTAL['sk_store'] = dim_store['sk_store']
FACT_RENTAL['sk_film'] = dim_film['sk_film']
FACT_RENTAL['sk_staff'] = dim_staff['sk_staff']




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
    dvdcur.close()
    dvdcon.close()  
    
    return        




if __name__ == "__main__":
    main()
    
