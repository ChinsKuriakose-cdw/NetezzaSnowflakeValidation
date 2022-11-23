# code to validate data migration from netezza to snowflake using snowflake Stored procedures and Netezza queries
# uses snowflake connector and netezza connector libraries 

import json
import nzpy
import pandas as pd
import snowflake.connector as sf


class Netezza:
    def __init__(self, db_name, schema_name, table_name):
        self.conn = None
        self.db_name = db_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.full_table_name = f"{self.db_name}.{self.schema_name}.{self.table_name}"
        self.column_info_df = None
        # self.connect_netezza()
        self.get_column_dtype_info()
        self.validate_columns()

    def connect_netezza(self):
        """
            :description: Used to create a connection object to interact with netezza database.
                          Needs to be updated with parameterized username, password, hostname, port and database
        """
        self.conn = nzpy.connect(user="admin", password="password", host='localhost', port=5480, database="db1", securityLevel=1, logLevel=0)
        
    def get_column_dtype_info(self):
        query = f"""select 
                ATTNAME, FORMAT_TYPE 
            from {self.db_name}.{self.schema_name}._v_relation_column 
            where NAME = '{self.table_name}' 
                and DATABASE = '{self.db_name}' 
                and OWNER = '{self.schema_name}' 
            order by ATTNUM;"""
        # self.column_info_df = pd.read_sql(query, conn) # use this line once the connection to netezza is created.
        self.column_info_df = pd.read_csv('src/netezza_col_dtype_sample.csv', sep=',', header='infer') # remove this once connection to netezza is established
        print(self.column_info_df)
    
    def int_col_checks(self, col):
        query = f"""select AVG({col}) as AVG, MIN({col}) as MIN, MAX({col}) as MAX, SUM({col}) as SUM, SUM(case when {col} is null then 1 else 0 end) as NULL_SUM, COUNT({col}) as COUNT
        from {self.full_table_name}"""
        # df = pd.read_sql(query, self.conn) # use this line once the connection to netezza is created
        df = pd.read_csv('src/int_col_check_sample.csv') # remove this once connection to netezza is established
        return df['AVG'][0], df['MIN'][0], df['MAX'][0], df['SUM'][0], df['NULL_SUM'][0], df['COUNT'][0]

    def varchar_col_checks(self, col):
        query = f"""select 0 as AVG, MIN({col}) as MIN, MAX({col}) as MAX, 0 as SUM, SUM(case when {col} is null then 1 else 0 end) as NULL_SUM, COUNT({col}) as COUNT
        from {self.full_table_name}"""
        # df = pd.read_sql(query, self.conn) # use this line once the connection to netezza is created
        df = pd.read_csv('src/varchar_col_check_sample.csv') # remove this once connection to netezza is established
        return df['AVG'][0], df['MIN'][0], df['MAX'][0], df['SUM'][0], df['NULL_SUM'][0], df['COUNT'][0]

    def func_selector(self, col):
        # for col in self.column_info_df['ATTNAME'].values:
        dtype = self.column_info_df.loc[self.column_info_df['ATTNAME'] == col, 'FORMAT_TYPE'].iloc[0].split('(')[0].upper()
        if dtype in ('NUMERIC','REAL','DOUBLE PRECISION','INTEGER','BYTEINT','SMALLINT','BIGINT'):
            return self.int_col_checks(col)
        elif 'CHAR' in dtype:
            return self.varchar_col_checks(col)
        else:
            return 0, 0, 0, 0, 0, 0

    def validate_columns(self):
        self.column_info_df['AVG'], \
            self.column_info_df['MIN'], \
                self.column_info_df['MAX'], \
                    self.column_info_df['SUM'], \
                        self.column_info_df['NULL_SUM'], \
                            self.column_info_df['COUNT'] = zip(*self.column_info_df['ATTNAME'].apply(self.func_selector))


class Snowflake:

    sf_validation_sp = 'COMMON.ADMIN.GENERIC_VALIDATION_SP'

    def __init__(self, db_name, schema_name, table_name) -> None:
        self.db_name = db_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.full_table_name = f"{self.db_name}.{self.schema_name}.{self.table_name}"
        # self.connect_snowflake()
        self.get_validation_json()

    def connect_snowflake(self):
        self.conn = sf.connect(
                user='suser',
                password='pass@123',
                account='abc123.us-east-2',
                warehouse='demo_wh',
                database='demo',
                schema='public'
                )
        self.cur = self.conn.cursor()

    def get_validation_json(self):
        query = f"""call {Snowflake.sf_validation_sp}('{self.full_table_name}')"""
        self.cur.execute(query)
        self.val_json = json.loads(self.cur.fetchone()[0])
        

def data_validation(netezza, snowflake):
    # return true|false based on validation
    pass
        

if __name__ == '__main__':
    netezza = Netezza('DB', 'SCHEMA', 'TABLE')
    snowflake = Snowflake('DB', 'SCHEMA', 'TABLE')
    assert data_validation(netezza, snowflake)
    print(netezza.column_info_df)
