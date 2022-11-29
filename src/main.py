################################## Netezza to Snowflake Migration Validation Script ####################################
# Description : Code to validate data migration from netezza to snowflake using snowflake Stored procedures and Netezza queries
#               Uses snowflake connector and netezza connector libraries
# Author: Chins Kuriakose
# Created on: 25/11/2022
# Last updated on: 25/11/2022
# Usage: python main.py [-h] [--date_column DATE_COLUMN] [--start_date START_DATE] [--end_date END_DATE] snowflake_table_name netezza_table_name

import sys
import json
import boto3
import nzpy
import argparse
import pandas as pd
import numpy as np
import snowflake.connector as sf


ssm_client = boto3.client('ssm')


class CountValidationError(Exception):
    """
        Exception raised when the snowflake data count and netezza data count mismatches after the migration task.
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
        Exception raised when the Data validation between snowflake and netezza databases fail after the migration task.
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
                            "MAX_STR_LENGTH": {
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

    """
    Class to represent table in Netezza database.

    ...

    Attributes
    ----------
    netezza_date_col: str
        Load date column for Netezza tables. Used by default if no particular date column is mentioned in the input.

    conn: nzpy connection object
        A connection object to interact with Netezza

    db_name: str
        Name of the database in Netezza for the table to be validated.

    schema_name: str
        Name of the schema in Netezza for the table to be validated.

    table_name: str
        Name of the table for which validation has to be done.

    full_table_name: str
        Complete name of the table --> DB.SCHEMA.TABLENAME

    date_col: str
        Name of the column which contains the date for which the validation has to be done.

    start_date: str
        Start date/timestamp from which the data has to be queried for validation.

    end_date: str
        End date/timestamp up to which the data has to be queried for validation.

    where_clause: str
        Based on the date parameter, the query to get data from the netezza table may use this where clause.

    val_df: pandas.Dataframe
        Dataframe which would contain the column name, data type and other validation parameters from Netezza.


    Methods
    -------
    connect_netezza()
        Creates connection object with credentials from parameter store.

    set_where_clause()
        Update the where_clause attribute based on the timestamp constraints as per the input.

    get_table_count()
        Gets count of records from the Netezza table using SQL query.

    get_column_dtype_info()
        Queries _v_relation_column table to get column details of the table from Netezza.

    int_col_checks(col)
        Returns average, minimum, maximum, sum for the specified (input) column using SQL query from the Netezza table.

    varchar_col_checks(col)
        Returns max character length in the specified (input) column from the table in Netezza using SQL query.

    datetime_col_checks(col)
        Returns minimum and maximum value for the specified (input) column from the table in Netezza using SQL query.

    func_selector(col)
        Invokes the right validation function based on the column datatype.

    validate_columns()
        Updates the validation dataframe using pandas apply function with func_selector function.

    """

    netezza_date_col = 'ETL_LOAD_DATE'

    def __init__(self, db_name, schema_name, table_name, date_col=netezza_date_col, start_date=None, end_date=None):
        """
        :description: Constructor to create Netezza object.
        :param db_name: Name of the database in which the table resides.
        :param schema_name: Name of the schema in which the table resides.
        :param table_name: Name of the table.
        :param date_col: Name of the date column that is used to query the table for data validation.
        :param start_date: Start date from which the data in the table has to be queried.
        :param end_date: End date up to which the data in the table has to be queried.
        """
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
        self.set_where_clause()
        self.get_column_dtype_info()
        self.get_table_count()
        self.validate_columns()

    def connect_netezza(self):
        """
            :description: Creates connection object with parameterized username, password, hostname, port and database
                          from parameter store.
        """

        self.conn = nzpy.connect(user="admin", password="password", host='localhost',
                                 port=5480, database="db1", securityLevel=1, logLevel=0)

    def set_where_clause(self):
        """
        :description: Update the where_clause attribute based on the timestamp constraints as per the input.
        """

        if self.start_date and self.end_date:
            self.where_clause = f"""\nwhere {self.date_col} > '{self.start_date}'
                        and {self.date_col} < '{self.end_date}'"""
        elif self.start_date:
            self.where_clause = f"""\nwhere {self.date_col} > '{self.start_date}'"""
        elif self.end_date:
            self.where_clause = f"""\nwhere {self.date_col} < '{self.end_date}'"""

    def get_table_count(self):
        """
        :description: Gets count of records from the Netezza table using SQL query.
        """

        query = f"""select count(*)
        from {self.full_table_name}{self.where_clause}"""

        # self.table_count = pd.read_sql(query, conn) # use this line once the connection to netezza is created.
        # remove this once connection to netezza is established
        self.table_count = 27

    def get_column_dtype_info(self):
        """
        :description: Queries _v_relation_column table to get column details of the table from Netezza.
        """

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
        """
        :description: Returns average, minimum, maximum, sum for the specified (input) column
                      using SQL query from the Netezza table
        :param col: Name of the column from which the validation details have to be queried from.
        :return: <average:float>, <minimum:float>, <maximum:float>, <sum:float>, np.NaN
        """

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
            np.NaN, \
            np.NaN, \
            np.NaN

    def varchar_col_checks(self, col):
        """
        :description: Returns max character length in the specified (input) column
                      from the table in Netezza using SQL query.
        :param col: Name of the column from which the validation details have to be queried from.
        :return: np.NaN, np.NaN, np.NaN, np.NaN, <max_length:int>
        """

        query = f"""select 
            max(length({col})) as MAX_STR_LENGTH
        from {self.full_table_name}{self.where_clause}"""
        # df = pd.read_sql(query, self.conn) # use this line once the connection to netezza is created
        # remove this once connection to netezza is established
        df = pd.read_csv('src/varchar_col_check_sample.csv')
        return np.NaN, \
            np.NaN, \
            np.NaN, \
            np.NaN, \
            np.NaN, \
            np.NaN, \
            df['MAX_STR_LENGTH'][0]

    def datetime_col_checks(self, col):
        """
        :description: Returns minimum and maximum value for the specified (input) column
                      from the table in Netezza using SQL query.
        :param col: Name of the column from which the validation details have to be queried from.
        :return: np.NaN, <minimum:float>, <maximum:float>, np.NaN, np.NaN
        """

        query = f"""select 
            min({col}) as MIN_DATE,
            max({col}) as MAX_DATE
        from {self.full_table_name}{self.where_clause}"""
        # df = pd.read_sql(query, self.conn) # use this line once the connection to netezza is created
        # remove this once connection to netezza is established
        df = pd.read_csv('src/date_col_check_sample.csv')
        return np.NaN, \
            np.NaN, \
            np.NaN, \
            np.NaN, \
            df['MIN_DATE'][0], \
            df['MAX_DATE'][0], \
            np.NaN

    def func_selector(self, col):
        """
        :description: Invokes the right validation function based on the column datatype.
        :param col: Name of the column from which the validation details have to be queried from.
        :return: Output of function called in the if block.
                 Pattern: <average:float>, <minimum:float>, <maximum:float>, <sum:float>, <max_length:int>
        """

        dtype = self.val_df.loc[self.val_df['ATTNAME'] ==
                                col, 'FORMAT_TYPE'].iloc[0].split('(')[0].upper()
        if dtype in ('NUMERIC', 'REAL', 'DOUBLE PRECISION', 'INTEGER', 'BYTEINT', 'SMALLINT', 'BIGINT'):
            return self.int_col_checks(col)
        elif 'CHAR' in dtype:
            return self.varchar_col_checks(col)
        elif dtype in ['DATE', 'INTERVAL'] or 'TIME' in dtype:
            return self.datetime_col_checks(col)
        else:
            return np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN

    def validate_columns(self):
        """
        :description: Updates the validation dataframe using pandas apply function with func_selector function.
        """

        self.val_df['AVG'], \
            self.val_df['MIN'], \
            self.val_df['MAX'], \
            self.val_df['SUM'], \
            self.val_df['MIN_DATE'], \
            self.val_df['MAX_DATE'], \
            self.val_df['MAX_STR_LENGTH'] = zip(
            *self.val_df['ATTNAME'].apply(self.func_selector))


class Snowflake:
    """
    Class to represent table in Snowflake database

    ...
    Attributes
    ----------
    sf_validation_sp: str
        Name of the generic validation SP in snowflake which creates a validation json containing the calculated metrics
        from the Snowflake table

    db_name: str
        Name of the database in Snowflake for the table to be validated.

    schema_name: str
        Name of the schema in Snowflake for the table to be validated.

    table_name: str
        Name of the table for which validation has to be done.

    full_table_name: str
        Complete name of the table --> DB.SCHEMA.TABLENAME

    val_json: str
        JSON data returned from the snowflake stored procedure for the particular table.

    table_count: str
        Count of records in the Snowflake table retrieved from the validation json


    Methods
    -------
    connect_snowflake()
        Creates connection object with credentials from parameter store.

    get_validation_json()
        Executes the generic snowflake SP and retreives the validation json.

    get_table_count()
        Retrieves record count from Snowflake validation json.
    """

    sf_validation_sp = 'COMMON.ADMIN.GENERIC_VALIDATION_SP'
    snowflake_date_col = 'ETL_LOAD_TYPE'

    def __init__(self, db_name, schema_name, table_name, date_col=snowflake_date_col, start_date=None, end_date=None) -> None:
        """
        :description: Constructor to create Snowflake class objects.
        :param db_name: Name of the database in Snowflake for the table to be validated.
        :param schema_name: Name of the schema in Snowflake for the table to be validated.
        :param table_name: Name of the table for which validation has to be done.
        """
        self.db_name = db_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.full_table_name = f"{self.db_name}.{self.schema_name}.{self.table_name}"
        self.val_json = dict()
        self.date_col = date_col
        self.start_date = start_date
        self.end_date = end_date
        self.table_count = None
        # self.connect_snowflake()
        self.get_validation_json()
        self.get_table_count()

    def connect_snowflake(self):
        """
        :description: Creates connection object with credentials from parameter store.
        """
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
        """
        :description: Executes the generic snowflake SP and retreives the validation json.
        """
        query = f"""call {Snowflake.sf_validation_sp}('{self.db_name}', '{self.schema_name}', '{self.table_name}')"""
        # self.cur.execute(query)
        # self.val_json = json.loads(self.cur.fetchone()[0])
        with open("src/sf_val_json_sample.json") as f:
            self.val_json = json.load(f)

        # {col1 :{datatype: number, avg: 1, min : 2, max : 3}, clm_nm2 :varchar , lenth : ...}

    def get_table_count(self):
        """
        :description: Retrieves record count from Snowflake validation json.
        """
        self.table_count = self.val_json.pop('COUNT')


def count_validation(netezza: Netezza, snowflake: Snowflake):
    """
    :description: Validates the count of records in netezza and snowflake.
                  Raises CountValidationError exception if counts mismatched.
    :param netezza: Object of class Netezza.
    :param snowflake: Object of class Snowflake.
    """

    if netezza.table_count != snowflake.table_count:
        raise CountValidationError(snowflake.table_count, netezza.table_count)


def data_validation(netezza: Netezza, snowflake: Snowflake):
    """
    :description: Validates the data received from snowflake after executing the generic stored procedure with
                  the data from Netezza which is queried in the functions in Netezza class.
                  If there are any validation errors, will raise DataValidationError Exception.
    :param netezza: Object of class Netezza.
    :param snowflake: Object of class Snowflake.
    """

    report = dict()
    for col in netezza.val_df['ATTNAME'].values:
        params = list(snowflake.val_json[col].keys())

        # remove 'Z' in snowflake timestamps
        if 'MAX_DATE' in params and snowflake.val_json[col]['MAX_DATE'].endswith('Z'):
            snowflake.val_json[col]['MAX_DATE'] = snowflake.val_json[col]['MAX_DATE'][:-2]
        if 'MIN_DATE' in params and snowflake.val_json[col]['MIN_DATE'].endswith('Z'):
            snowflake.val_json[col]['MIN_DATE'] = snowflake.val_json[col]['MIN_DATE'][:-2]

        params.remove('DATA_TYPE')
        for param in params:
            if snowflake.val_json[col][param] != netezza.val_df.loc[netezza.val_df['ATTNAME'] == col, param].iloc[0]:
                if col not in report.keys():
                    report[col] = dict()
                if param not in report[col].keys():
                    report[col][param] = dict()
                report[col][param]['SF'] = snowflake.val_json[col][param]
                report[col][param]['Netezza'] = netezza.val_df.loc[netezza.val_df['ATTNAME']
                                                                   == col, param].iloc[0]

    if report:
        raise DataValidationError(report)

if __name__ == '__main__':
    # reading input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("snowflake_table_name", help="Full name of the table in Snowflake. Format: DB.SCHEMA.TABLE.")
    parser.add_argument("netezza_table_name", help="Full name of the table in Netezza. Format: DB.SCHEMA.TABLE.")
    parser.add_argument("--date_column", help="Column in the table which can be used to query for the required data validation.")
    parser.add_argument("--start_date", help="Start date from which the data is migrated, hence the date in the table from which the data has to be queried.")
    parser.add_argument("--end_date", help="End date up to which the data is migrated, hence the date in the table up to which the data has to be queried.")
    args = parser.parse_args()

    sf_db, sf_schema, sf_table = args.snowflake_table_name.split('.')
    net_db, net_schema, net_table = args.netezza_table_name.split('.')
    date_col = None
    start_date = None
    end_date = None

    if args.date_column:
        date_col = args.date_column
    if args.start_date:
        start_date = args.start_date
    if args.end_date:
        end_date = args.end_date

    netezza = Netezza(sf_db, sf_schema, sf_table, date_col, start_date, end_date)
    snowflake = Snowflake(net_db, net_schema, net_table, date_col, start_date, end_date)

    count_validation(netezza, snowflake)
    data_validation(netezza, snowflake)
