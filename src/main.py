################################################ Migration Validation Script ###################################################################
# code to validate data migration from netezza to snowflake using snowflake Stored procedures and Netezza queries
# uses snowflake connector and netezza connector libraries
# author: Chins Kuriakose
# Created on: 25/11/2022
# Last updated on: 25/11/2022

import json
import nzpy
import pandas as pd
import numpy as np
import snowflake.connector as sf


class CountValidationError(Exception):
    """
        :descrption: Exception raised when the snowflake data count and netezza data count mismatches after the migration task.
    """
    def __init__(self, sf_count, net_count) -> None:
        """
            :description: constructor function to update the error message in the exception
            :param sf_count: Count of records in snowflake db after migration.
            :param net_count: Count of records in netezza db which was migrated to snowflake.
            :usage: raise CountVaildationError(<sf_count>, <net_count>)
            :output: Counts mismatched.
                     Snowflake count : <sf_count>
                     Netezza count : <net_count>
        """
        self.message = f"Counts mismatched.\nSnowflake count : {sf_count}\nNetezza count : {net_count}"
        super().__init__(self.message)


class DataValidationError(Exception):
    """
        :descrption: Exception raised when the Data validation between snowflake and netezza databases fail after the migration task.
    """
    def __init__(self, report: dict) -> None:
        """
            :description: constructor function to update the error message in the exception
            :param report: Dictionary containing info regarding mismatched validation data
            :usage: raise DataVaildationError(<report>)
            :output: Data Validation Failed.
                     Report:
                     {
                        "ADD_TYPE": {
                            "MAX_LENGTH": {
                            "SF": 22,
                            "Netezza": 20.0
                            }
                        },
                        "ETL_UPDATE_DATE": {
                            "MIN": {
                                "SF": "2015-05-28 15:53:00",
                                "Netezza": "2015-05-28 15:53:31"
                            }
                        }
                     }
        """
        self.message = f"Data Validation Failed.\nReport:\n{json.dumps(report, indent=4)}"
        super().__init__(self.message)


class Netezza:
    def __init__(self, db_name, schema_name, table_name, date_col=None, start_date=None, end_date=None):
        self.conn = None
        self.db_name = db_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.full_table_name = f"{self.db_name}.{self.schema_name}.{self.table_name}"
        self.date_col = date_col
        self.start_date = start_date
        self.end_date = end_date
        self.where_clause = ''
        self.val_df = None
        # self.connect_netezza()
        self.get_column_dtype_info()
        self.get_table_count()
        self.validate_columns()

    def connect_netezza(self):
        """
            :description: Used to create a connection object to interact with netezza database.
                          Needs to be updated with parameterized username, password, hostname, port and database
        """
        self.conn = nzpy.connect(user="admin", password="password", host='localhost',
                                 port=5480, database="db1", securityLevel=1, logLevel=0)

    def get_table_count(self):
        
        self.where_clause = ""

        if self.date_col:
            if self.start_date and self.end_date:
                self.where_clause = f"""\nwhere {self.date_col} > '{self.start_date}'
                    and {self.date_col} < '{self.end_date}'"""
            elif self.start_date:
                self.where_clause = f"""\nwhere {self.date_col} > '{self.start_date}'"""
            elif self.end_date:
                self.where_clause = f"""\nwhere {self.date_col} < '{self.end_date}'"""

        query = f"""select count(*)
        from {self.full_table_name}{self.where_clause}"""

        # self.table_count = pd.read_sql(query, conn) # use this line once the connection to netezza is created.
        # remove this once connection to netezza is established
        self.table_count = 27

    def get_column_dtype_info(self):
        query = f"""select 
                ATTNAME, FORMAT_TYPE 
            from {self.db_name}.{self.schema_name}._v_relation_column 
            where NAME = '{self.table_name}' 
                and DATABASE = '{self.db_name}' 
                and OWNER = '{self.schema_name}' 
            order by ATTNUM;"""
        # self.val_df = pd.read_sql(query, conn) # use this line once the connection to netezza is created.
        # remove this once connection to netezza is established
        self.val_df = pd.read_csv(
            'src/netezza_col_dtype_sample.csv', sep=',', header='infer')

    def int_col_checks(self, col):
        query = f"""select AVG({col}) as AVG, 
            MIN({col}) as MIN, 
            MAX({col}) as MAX, 
            SUM({col}) as SUM
        from {self.full_table_name}{self.where_clause}"""
        # df = pd.read_sql(query, self.conn) # use this line once the connection to netezza is created
        # remove this once connection to netezza is established
        df = pd.read_csv('src/int_col_check_sample.csv')
        return df['AVG'][0], \
            df['MIN'][0], \
            df['MAX'][0], \
            df['SUM'][0], \
            np.NaN

    def varchar_col_checks(self, col):
        query = f"""select 
            max(length({col})) as MAX_LENGTH
        from {self.full_table_name}{self.where_clause}"""
        # df = pd.read_sql(query, self.conn) # use this line once the connection to netezza is created
        # remove this once connection to netezza is established
        df = pd.read_csv('src/varchar_col_check_sample.csv')
        return np.NaN, \
            np.NaN, \
            np.NaN, \
            np.NaN, \
            df['MAX_LENGTH'][0]

    def datetime_col_checks(self, col):
        query = f"""select 
            min({col}) as MIN,
            max({col}) as MAX
        from {self.full_table_name}{self.where_clause}"""
        # df = pd.read_sql(query, self.conn) # use this line once the connection to netezza is created
        # remove this once connection to netezza is established
        df = pd.read_csv('src/date_col_check_sample.csv')
        return np.NaN, \
            df['MIN'][0], \
            df['MAX'][0], \
            np.NaN, \
            np.NaN

    def func_selector(self, col):
        # for col in self.val_df['ATTNAME'].values:
        dtype = self.val_df.loc[self.val_df['ATTNAME'] ==
                                col, 'FORMAT_TYPE'].iloc[0].split('(')[0].upper()
        if dtype in ('NUMERIC', 'REAL', 'DOUBLE PRECISION', 'INTEGER', 'BYTEINT', 'SMALLINT', 'BIGINT'):
            return self.int_col_checks(col)
        elif 'CHAR' in dtype:
            return self.varchar_col_checks(col)
        elif dtype in ['DATE', 'INTERVAL'] or 'TIME' in dtype:
            return self.datetime_col_checks(col)
        else:
            return np.NaN, np.NaN, np.NaN, np.NaN, np.NaN

    def validate_columns(self):
        self.val_df['AVG'], \
            self.val_df['MIN'], \
            self.val_df['MAX'], \
            self.val_df['SUM'], \
            self.val_df['MAX_LENGTH'] = zip(
                *self.val_df['ATTNAME'].apply(self.func_selector))
        # print(self.val_df)


class Snowflake:

    sf_validation_sp = 'COMMON.ADMIN.GENERIC_VALIDATION_SP'

    def __init__(self, db_name, schema_name, table_name) -> None:
        self.db_name = db_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.full_table_name = f"{self.db_name}.{self.schema_name}.{self.table_name}"
        self.table_count = None
        self.val_json = dict()
        # self.connect_snowflake()
        self.get_validation_json()
        self.get_table_count()

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
        query = f"""call {Snowflake.sf_validation_sp}('{self.db_name}', '{self.schema_name}', '{self.table_name}')"""
        # self.cur.execute(query)
        # self.val_json = json.loads(self.cur.fetchone()[0])
        with open("src/sf_val_json_sample.json") as f:
            self.val_json = json.load(f)

        # {col1 :{datatype: number, avg: 1, min : 2, max : 3}, clm_nm2 :varchar , lenth : ...}
    
    def get_table_count(self):
        self.table_count = self.val_json.pop('COUNT')



def count_validation(netezza: Netezza, snowflake:Snowflake):
    if netezza.table_count != snowflake.table_count:
        raise CountValidationError(snowflake.table_count, netezza.table_count)


def data_validation(netezza:Netezza, snowflake:Snowflake):

    report = dict()
    for col in netezza.val_df['ATTNAME'].values:
        # for key in snowflake.val_json.keys():
            params = list(snowflake.val_json[col].keys())
            params.remove('DATATYPE')
            for param in params:
                if snowflake.val_json[col][param] != netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, param].iloc[0]:
                    if col not in report.keys():
                        report[col] = dict()
                    if param not in report[col].keys():
                        report[col][param] = dict()
                    report[col][param]['SF'] = snowflake.val_json[col][param]
                    report[col][param]['Netezza'] = netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, param].iloc[0]

    if report:
        raise DataValidationError(report)


if __name__ == '__main__':
    netezza = Netezza('DB', 'SCHEMA', 'TABLE')

    snowflake = Snowflake('DB', 'SCHEMA', 'TABLE')
    count_validation(netezza, snowflake)
    data_validation(netezza, snowflake)
