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
        self.val_df = None
        # self.connect_netezza()
        self.get_column_dtype_info()
        self.validate_columns()

    def connect_netezza(self):
        """
            :description: Used to create a connection object to interact with netezza database.
                          Needs to be updated with parameterized username, password, hostname, port and database
        """
        self.conn = nzpy.connect(user="admin", password="password", host='localhost',
                                 port=5480, database="db1", securityLevel=1, logLevel=0)

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
            SUM({col}) as SUM, 
            SUM(case when {col} is null then 1 else 0 end) as NULL_SUM, 
            COUNT({col}) as COUNT
        from {self.full_table_name}"""
        # df = pd.read_sql(query, self.conn) # use this line once the connection to netezza is created
        # remove this once connection to netezza is established
        df = pd.read_csv('src/int_col_check_sample.csv')
        return df['AVG'][0], \
            df['MIN'][0], \
            df['MAX'][0], \
            df['SUM'][0], \
            df['NULL_SUM'][0], \
            df['COUNT'][0]

    def varchar_col_checks(self, col):
        query = f"""select 0 as AVG, MIN({col}) as MIN, MAX({col}) as MAX, 0 as SUM, SUM(case when {col} is null then 1 else 0 end) as NULL_SUM, COUNT({col}) as COUNT
        from {self.full_table_name}"""
        # df = pd.read_sql(query, self.conn) # use this line once the connection to netezza is created
        # remove this once connection to netezza is established
        df = pd.read_csv('src/varchar_col_check_sample.csv')
        return df['AVG'][0], \
            df['MIN'][0], \
            df['MAX'][0], \
            df['SUM'][0], \
            df['NULL_SUM'][0], \
            df['COUNT'][0]

    def func_selector(self, col):
        # for col in self.val_df['ATTNAME'].values:
        dtype = self.val_df.loc[self.val_df['ATTNAME'] ==
                                col, 'FORMAT_TYPE'].iloc[0].split('(')[0].upper()
        if dtype in ('NUMERIC', 'REAL', 'DOUBLE PRECISION', 'INTEGER', 'BYTEINT', 'SMALLINT', 'BIGINT'):
            return self.int_col_checks(col)
        elif 'CHAR' in dtype:
            return self.varchar_col_checks(col)
        else:
            return 0, 0, 0, 0, 0, 0

    def validate_columns(self):
        self.val_df['AVG'], \
            self.val_df['MIN'], \
            self.val_df['MAX'], \
            self.val_df['SUM'], \
            self.val_df['NULL_SUM'], \
            self.val_df['COUNT'] = zip(
                *self.val_df['ATTNAME'].apply(self.func_selector))
        # print(self.val_df)


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
        query = f"""call {Snowflake.sf_validation_sp}('{self.db_name}', '{self.schema_name}', '{self.table_name}')"""
        # self.cur.execute(query)
        # self.val_json = json.loads(self.cur.fetchone()[0])
        with open("src/sf_val_json_sample.json") as f:
            self.val_json = json.load(f)

        # {col1 :{datatype: number, avg: 1, min : 2, max : 3}, clm_nm2 :varchar , lenth : ...}


def data_validation(netezza, snowflake):

    validation_df = pd.DataFrame()

    for col in snowflake.val_json.keys():
        s_avg = snowflake.val_json[col]['AVG'] if 'AVG' in snowflake.val_json[col].keys(
        ) else 0
        s_min = snowflake.val_json[col]['MIN'] if 'MIN' in snowflake.val_json[col].keys(
        ) else 0
        s_max = snowflake.val_json[col]['MAX'] if 'MAX' in snowflake.val_json[col].keys(
        ) else 0
        s_sum = snowflake.val_json[col]['SUM'] if 'SUM' in snowflake.val_json[col].keys(
        ) else 0
        s_nullsum = snowflake.val_json[col]['NULL_SUM'] if 'NULL_SUM' in snowflake.val_json[col].keys(
        ) else 0
        s_count = snowflake.val_json[col]['COUNT'] if 'COUNT' in snowflake.val_json[col].keys(
        ) else 0



        # print(col, 'AVG', netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, 'AVG'].iloc[0])
        n_avg = netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, 'AVG'].iloc[0]

        # print(col, 'MIN', netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, 'MIN'].iloc[0])
        n_min = netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, 'MIN'].iloc[0]

        # print(col, 'MAX', netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, 'MAX'].iloc[0])
        n_max = netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, 'MAX'].iloc[0]

        # print(col, 'SUM', netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, 'SUM'].iloc[0])
        n_sum = netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, 'SUM'].iloc[0]

        # print(col, 'NULL_SUM', netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, 'NULL_SUM'].iloc[0])
        n_nullsum = netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, 'NULL_SUM'].iloc[0]

        # print(col, 'COUNT', netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, 'COUNT'].iloc[0])  
        n_count = netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, 'COUNT'].iloc[0]      
        
        if not (s_avg == n_avg and s_min == n_min and s_max == n_max and s_sum == n_sum and s_nullsum == n_nullsum and s_count == n_count):
            if validation_df.empty:
                validation_df = pd.DataFrame(
                    {
                        'COLUMN_NM': [col],
                        'DATATYPE': [snowflake.val_json[col]['DATATYPE']],
                        'SF_AVG': [s_avg],
                        'Net_AVG': [n_avg],
                        'SF_MIN': [s_min],
                        'Net_MIN': [n_min],
                        'SF_MAX': [s_max],
                        'Net_MAX': [n_max],
                        'SF_SUM': [s_sum],
                        'Net_SUM': [n_sum],
                        'SF_NULLSUM': [s_nullsum],
                        'Net_NULLSUM': [n_nullsum],
                        'SF_COUNT': [s_count],
                        'Net_COUNT': [n_count]
                    }
                )
            else:
                validation_df = pd.concat([
                    validation_df,
                    pd.DataFrame(
                        {
                            'COLUMN_NM': [col],
                            'DATATYPE': [snowflake.val_json[col]['DATATYPE']],
                            'SF_AVG': [s_avg],
                            'Net_AVG': [n_avg],
                            'SF_MIN': [s_min],
                            'Net_MIN': [n_min],
                            'SF_MAX': [s_max],
                            'Net_MAX': [n_max],
                            'SF_SUM': [s_sum],
                            'Net_SUM': [n_sum],
                            'SF_NULLSUM': [s_nullsum],
                            'Net_NULLSUM': [n_nullsum],
                            'SF_COUNT': [s_count],
                            'Net_COUNT': [n_count]
                        }
                    )
                ])

    if validation_df.empty:
        return True, None
    else:
        return False, validation_df
    # return true|false based on validation


if __name__ == '__main__':
    netezza = Netezza('DB', 'SCHEMA', 'TABLE')

    snowflake = Snowflake('DB', 'SCHEMA', 'TABLE')
    validation_status, validation_df = data_validation(netezza, snowflake)
    if validation_status:
        print("Validation successfull")
    else:
        print("Validation Failed")
        print("Here is the report : ")
        print("\n", validation_df.to_string())
