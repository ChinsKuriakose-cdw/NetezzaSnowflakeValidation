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

Sample dataframe created after running validation queries:

```
           ATTNAME            FORMAT_TYPE           AVG  MIN        MAX         SUM  NULL_SUM  COUNT
0     ADD_TYPE_SID                INTEGER  4.452741e+07    0  300180001  1202240056         0     27
1         ADD_TYPE  CHARACTER VARYING(20)  0.000000e+00  APP        XFR           0         0     27
2    ADD_TYPE_DESC  CHARACTER VARYING(50)  0.000000e+00  APP        XFR           0         0     27
3    ETL_LOAD_DATE              TIMESTAMP  0.000000e+00    0          0           0         0      0
4  ETL_UPDATE_DATE              TIMESTAMP  0.000000e+00    0          0           0         0      0
5      ETL_USER_ID  CHARACTER VARYING(20)  0.000000e+00  APP        XFR           0         0     27
6          CHANNEL  CHARACTER VARYING(50)  0.000000e+00  APP        XFR           0         0     27
7    CHANNEL_GROUP  CHARACTER VARYING(20)  0.000000e+00  APP        XFR           0         0     27
```
