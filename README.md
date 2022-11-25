### Dependencies ###

```bash
pip install snowflake-connector-python
pip install nzpy
pip install pandas

```




Sample dataframe created by querying the Netezza _v_relation_column view for a particular table

```
           ATTNAME            FORMAT_TYPE
0     ADD_TYPE_SID                INTEGER
1         ADD_TYPE  CHARACTER VARYING(20)
2    ADD_TYPE_DESC  CHARACTER VARYING(50)
3    ETL_LOAD_DATE              TIMESTAMP
4  ETL_UPDATE_DATE              TIMESTAMP
5      ETL_USER_ID  CHARACTER VARYING(20)
6          CHANNEL  CHARACTER VARYING(50)
7    CHANNEL_GROUP  CHARACTER VARYING(20)
```

Sample dataframe created after running validation queries in Netezza:

```
           ATTNAME            FORMAT_TYPE           AVG                  MIN                  MAX           SUM  MAX_LENGTH
0     ADD_TYPE_SID                INTEGER  4.452741e+07                    0            300180001  1.202240e+09         NaN
1         ADD_TYPE  CHARACTER VARYING(20)           NaN                  NaN                  NaN           NaN        20.0
2    ADD_TYPE_DESC  CHARACTER VARYING(50)           NaN                  NaN                  NaN           NaN        20.0
3    ETL_LOAD_DATE              TIMESTAMP           NaN  2015-05-28 15:53:31  2022-06-07 06:19:30           NaN         NaN
4  ETL_UPDATE_DATE              TIMESTAMP           NaN  2015-05-28 15:53:31  2022-06-07 06:19:30           NaN         NaN
5      ETL_USER_ID  CHARACTER VARYING(20)           NaN                  NaN                  NaN           NaN        20.0
6          CHANNEL  CHARACTER VARYING(50)           NaN                  NaN                  NaN           NaN        20.0
7    CHANNEL_GROUP  CHARACTER VARYING(20)           NaN                  NaN                  NaN           NaN        20.0
```
