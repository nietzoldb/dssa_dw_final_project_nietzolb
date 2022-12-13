import logging
import pandas as pd
import psycopg2 as psy
import sqlalchemy
import pypika
import json





def main():
    establish_workflow = Pipeline(
        steps=[
            Task(create_cursor,
                kwargs={'path': DB_CONFIG, 'section': SECTION},
                depends_on=None,
                name='create_cursor'),
            Task(create_schema,
                kwargs={'schema_name': DW._name},
                depends_on=['create_cursor'],
                name='create_schema'),
            Task(create_table,
                kwargs={'table_name': DW.dimCustomer, 'primary_key': 'sk_customer', 'definition': DIM_CUSTOMER},
                depends_on=['create_schema'],
                name='create_dim_customer'),
            Task(create_table,
                kwargs={'table_name': DW.dimStore, 'primary_key': 'sk_store', 'definition': DIM_STORE},
                depends_on=['create_schema'],
                name='create_dim_store'),
            Task(create_table,
                kwargs={'table_name': DW.dimFilm, 'primary_key': 'sk_film', 'definition': DIM_FILM},
                depends_on=['create_schema'],
                name='create_dim_film'),
            Task(create_table,
                kwargs={'table_name': DW.dimStaff, 'primary_key': 'sk_staff', 'definition': DIM_STAFF},
                depends_on=['create_schema'],
                name='create_dim_staff'),
            Task(create_table,
                kwargs={'table_name': DW.dimDate, 'primary_key': 'sk_date', 'definition': DIM_DATE},
                depends_on=['create_schema'],
                name='create_dim_date')             
            Task(create_table,
                kwargs={'table_name': DW.factRental, 'definition': FACT_RENTAL,
                        'foriegn_keys':['sk_customer', 'sk_store', 'sk_date', 'sk_staff', 'sk_film']
                        },
                depends_on=['create_schema'],
                name='create_fact_rentals') 
        ],
        type= 'default'
        )
    