# code to validate data migration from netezza to snowflake using snowflake Stored procedures and Netezza queries
# uses snowflake connector and netezza connector libraries

import nzpy
import pandas as pd


class Netezza:
    def __init__(self, db_name, schema_name, table_name):
        self.conn = None
        self.db_name = db_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.full_table_name = f"{self.db_name}.{self.schema_name}.{self.table_name}"
        self.column_info_df = None
        self.get_column_dtype_info()
        self.validate_columns()


    def connect_netezza(self):
        self.conn = nzpy.connect(user="admin", password="password", host='localhost', port=5480, database="db1", securityLevel=1, logLevel=0)
        
    def get_column_dtype_info(self):
        query = f"""select 
                ATTNAME, FORMAT_TYPE 
            from {self.db_name}.{self.schema_name}._v_relation_column 
            where NAME = '{self.table_name}' 
                and DATABASE = '{self.db_name}' 
                and OWNER = '{self.schema_name}' 
            order by ATTNUM;"""
        # self.column_info_df = pd.read_sql(query, conn)
        self.column_info_df = pd.read_csv('src/netezza_col_dtype_sample.csv', sep=',', header='infer')
    
    def int_col_checks(self, col):
        query = f"""select AVG({col}) as AVG, MIN({col}) as MIN, MAX({col}) as MAX, SUM({col}) as SUM, SUM(case when {col} is null then 1 else 0 end) as NULL_SUM, COUNT({col}) as COUNT
        from {self.full_table_name}"""
        # df = pd.read_sql(query, self.conn)
        df = pd.read_csv('src/int_col_check_sample.csv')
        print(df)
        return df['AVG'][0], df['MIN'][0], df['MAX'][0], df['SUM'][0], df['NULL_SUM'][0], df['COUNT'][0]

    def varchar_col_checks(self, col):
        query = f"""select 0 as AVG, MIN({col}) as MIN, MAX({col}) as MAX, 0 as SUM, SUM(case when {col} is null then 1 else 0 end) as NULL_SUM, COUNT({col}) as COUNT
        from {self.full_table_name}"""
        # df = pd.read_sql(query, self.conn)
        df = pd.read_csv('src/varchar_col_check_sample.csv')
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

        

if __name__ == '__main__':
    netezza = Netezza('EDW_SS_PROD', 'ETL_DW_PROD', 'ADD_TYPE_DIM')
    # print(netezza.column_info_df['ATTNAME'].values, type(netezza.column_info_df['ATTNAME'].values))
    # print(netezza.column_info_df.loc[netezza.column_info_df['ATTNAME'] == 'ADD_TYPE_SID', 'FORMAT_TYPE'].iloc[0])
    print(netezza.column_info_df)
