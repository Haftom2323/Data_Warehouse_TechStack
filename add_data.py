import os
import pandas as pd
import string
import re
import mysql.connector as mysql
from mysql.connector import Error

def DBConnect(dbName=None):
    conn = mysql.connect(host='localhost', user='root', password='123456',
                         database=dbName, buffered=True)
    cur = conn.cursor()
    return conn, cur

def emojiDB(dbName: str) -> None:
    conn, cur = DBConnect(dbName)
    dbQuery = f"ALTER DATABASE {dbName} CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;"
    cur.execute(dbQuery)
    conn.commit()

def createDB(dbName: str) -> None:
    conn, cur = DBConnect()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {dbName};")
    conn.commit()
    cur.close()

def createTables(dbName: str) -> None:
    conn, cur = DBConnect(dbName)
    sqlFile = 'schema.sql'
    fd = open(sqlFile, 'r')
    readSqlFile = fd.read()
    fd.close()

    sqlCommands = readSqlFile.split(';')

    for command in sqlCommands:
        try:
            res = cur.execute(command)
        except Exception as ex:
            print("Command skipped: ", command)
            print(ex)
    conn.commit()
    cur.close()

    return


def insert_to_sensor_table(dbName: str, df: pd.DataFrame, table_name: str) -> None:
        
    conn, cur = DBConnect(dbName)
    
    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} ( utc_time_id,source_ref, source_id, feed_id, primary_link_source_flag, \
                    samples, avg_speed, avg_flow,avg_occ, \
                    avg_freeflow_speed,avg_travel_time, \
                    high_quality_samples,samples_below_100pct_ff,samples_below_95pct_ff, \
                    samples_below_90pct_ff,samples_below_85pct_ff,samples_below_80pct_ff, \
                    samples_below_75pct_ff,samples_below_70pct_ff,samples_below_65pct_ff, \
                    samples_below_60pct_ff,samples_below_55pct_ff,samples_below_50pct_ff, \
                    samples_below_45pct_ff,samples_below_40pct_ff,samples_below_35pct_ff) \
                    VALUES(%s, "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s","%s");"""
        data = (row[0],row[1], row[2], row[3], (row[4]), (row[5]), row[6], row[7], row[8], row[9], row[10], row[11],
                row[12], row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23],row[24],row[25])

        try:
            cur.execute(sqlQuery, data)
            conn.commit()
            print("Data Inserted Successfully")
        except Exception as e:
            conn.rollback()
            print("Error: ", e)
    return

def db_execute_fetch(*args, many=False, tablename='', rdf=True, **kwargs) -> pd.DataFrame:
    connection, cursor1 = DBConnect(**kwargs)
    if many:
        cursor1.executemany(*args)
    else:
        cursor1.execute(*args)

    field_names = [i[0] for i in cursor1.description]

    
    res = cursor1.fetchall()

    
    nrow = cursor1.rowcount
    if tablename:
        print(f"{nrow} recrods fetched from {tablename} table")

    cursor1.close()
    connection.close()

    
    if rdf:
        return pd.DataFrame(res, columns=field_names)
    else:
        return res

if __name__ == "__main__":
    createDB(dbName='DWH')
    emojiDB(dbName='DWH')
    createTables(dbName='DWH')
    headers=["utc_time_id","source_ref", "source_id", "feed_id", "primary_link_source_flag","samples", "avg_speed", "avg_flow","avg_occ","avg_freeflow_speed","avg_travel_time","high_quality_samples","samples_below_100pct_ff","samples_below_95pct_ff","samples_below_90pct_ff","samples_below_85pct_ff","samples_below_80pct_ff","samples_below_75pct_ff","samples_below_70pct_ff","samples_below_65pct_ff","samples_below_60pct_ff","samples_below_55pct_ff","samples_below_50pct_ff","samples_below_45pct_ff","samples_below_40pct_ff","samples_below_35pct_ff","samples_below_30pct_ff","samples_below_25pct_ff","samples_below_20pct_ff","samples_below_15pct_ff","samples_below_10pct_ff","samples_below_5pct_ff"]
    headers=headers[0:26]
    data=pd.read_csv('.data/I80_davis.txt',header=0, names=headers,index_col=False)
    data=data[-500:]
    data['utc_time_id']=data['utc_time_id'].apply(lambda c:pd.Timestamp(c)) 
    insert_to_sensor_table(dbName='DWH', df=data, table_name='sensor')