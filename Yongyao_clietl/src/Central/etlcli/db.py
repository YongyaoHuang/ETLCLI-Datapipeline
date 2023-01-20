# -*- coding: utf-8 -*-
"""
Yongyao huang
2023/1/18

function to build ETL 
"""
import pandas as pd
import sqlite3
import time
import math


def sql2csv(sql,c,save_address,kwargs):
    """
    sql:The sql query to select the 
    c:the target database's cursor
    save_address:The csv file saving file path
    tag:Tag to distinguishing output files
    """
    c.execute(sql)
    print('Sucessfully execute the sql')
    
    # get the title
    des = c.description
    title = [each[0] for each in des]

    # get the query content
    result_list = []
    for each in c.fetchall():
        result_list.append(list(each))
    print(len(result_list))
    # save into dataframe
    df_dealed = pd.DataFrame(result_list, columns=title)
    
    for k,v in kwargs.items():
        if len(kwargs)>0:
            df_dealed.to_csv(save_address+' '+k+'='+v+'.csv', index = None)
            break
            
        else:
            df_dealed.to_csv(save_address+'.csv', index = None)
            break

    print('Sucessfully transfer to csv')
  
def df2db(df,n,target,c,conn):
    """
    this function is for transfer dataframe save to database
    df:dataframe
    n:how many qureys run at one time
    target:target database's name
    c:target database's cursor
    conn:target database's connection
    """
    
    column_list=df.columns.values
    line=''
    for a in column_list:
        line=line+a+','    
    line=line[:-1]

    count=0
    line2=''
    for index,row in df.iterrows():
        count+=1
        line2=line2+'('
        for e in row:
            if type(e)==int or type(e)== float:
                if math.isnan(e):
                    line2=line2+'null'+','
                else:    
                    line2=line2+str(e)+','
            else:
                e=e.strip().replace('\'','\'\'')
                line2=line2+'\''+str(e)+'\''+','
        line2=line2[:-1]+')'+',\n'
        
        if count==n:
            line2=line2[:-2]
            sql2='REPLACE INTO {}({})\nVALUES{};'.format(target,line,line2)
            line2=''
            count=0
            try:
                c.execute(sql2)
                conn.commit()
                print('Successfully upload latest values')
            except Exception as e:
                print('Database error when insert value:',e)
                print('In this sql:',sql2)
                conn.rollback()
        
def connect_db(db_address):
    """
    db_address:The file where the database to operate on is located
    """

    try:
        conn = sqlite3.connect(r'C:\Users\Administrator\Documents\Downloads\source.db')
        print('successful open database')
    except Exception as e:
        print('error:',e)
        print('unsucessfully open database, try two more')
        for i in range(2):
            time.sleep(10)
            conn = sqlite3.connect(r'C:\Users\Administrator\Documents\Downloads\source.db')
            print('successfully connected to database')
    
    c = conn.cursor()
    return(c,conn)

def create_table_with_df(c,conn,df,target):
    """
    This function is for using dataframe's columns values to create table.
    Because Sqlite3 can regcongize data type automactically, so here I didn't set data schema for general purpose.'
    df:data source(data frame)
    target:target database's name'
    c:target database's cursor
    conn:target database's connection
    
    # 
    """
    sql='CREATE TABLE '+target+'(\n'
    
    column_list=df.columns.values
    for h in column_list:       
        sql=sql+h+',\n'
    sql=sql+'primary key ({}));'.format(df.columns.values[0])
    
    try:
        c.execute(sql)
        conn.commit()
        print('Successfully create table {}'.format(target))
    except Exception as e:
        print('Database error when creating:',e)
        conn.rollback()
   
def conditional_select(sql,groupby,c,conn,target,kwargs):
    """
    This function is use for select query from database
    sql:The query send to database
    kwargs:the filter for select query
    group by: if the query is using group by? if true then using 'having'. if false , then use 'where' 
    c:target database's cursor
    conn:target database's connection
    target:target database's name
    """
    
    #if group by then use having
    if groupby:
        if len(kwargs)>0:
            sql=sql+' having '
            for k,v in kwargs.items():
                if k.lower()=='date':
                    tag=v
                    sql=sql+'date(datetime)='+'\''+v+'\''
                    return(sql)
                else:
                    tag=k+'='+v
                    sql=sql+tag
                    return(sql)
        else:
            return(sql)
        
    #else use where
    else:
        if len(kwargs)>0:
            sql=sql+' where '
            for k,v in kwargs.items():
                if k.lower()=='date':
                    tag=v
                    sql=sql+'date(invoice_date)='+'\''+v+'\''
                    return(sql)

                else:
                    tag=k+'='+v
                    sql=sql+tag
                    return(sql)
        else:
            return(sql)
    


def extract(db_address,sql,target,**kwargs):
    """
    db_address:The file where the database to operate on is located
    input: Path to the file with the input data in CSV format.
    database: The database file path where to put the given data.
    target: The target table of the data to be loaded.
    """
    #read sql
    sql = open(sql, 'r')
    sql = "".join(sql.readlines())
    
    #connect to the data base
    c,conn=connect_db(db_address)

    #see if have other conditions for select query
    if len(kwargs)==0:
        kwargs={}
    
    #preprocess the sql query
    sql=conditional_select(sql,False,c,conn,target,kwargs)
    
    #output query result and save to csv
    sql2csv(sql,c,target,kwargs)
    
    #close connection avoid using up resources
    conn.close()
    
def load(input_address,database,target):
    """
    input_address: Path to the file with the input data in CSV format.
    database: The database file path where to put the given data.
    target: The target table of the data to be loaded.
    """
    
    #connect database
    c,conn=connect_db(database)
    
    #read csv
    csv=pd.read_csv(input_address,header=0)
    
    #create table using dataframe's column values
    create_table_with_df(c,conn,csv,target)
        
    #save dataframe into databse
    df2db(csv,20,target,c,conn)
    
    #close the connections to avoid using up resources    
    conn.close()   

def transform(database,sql,target,**kwargs):
    """
    database: The file where the database to operate on is located.
    sql: The sql file to execute in order to perform the transformation task.
    params: The params for the sql file.
    target: The target table in the same db where to put the resulting data.
    """
    #connect database
    c,conn=connect_db(database)
    
    #read the sql query
    sql = open(sql, 'r')
    sql = "".join(sql.readlines())
    
    #preprocess sql query as demand
    sql=conditional_select(sql,True,c,conn,target,kwargs)
    
    #transfer output to dataframe
    df = pd.read_sql(sql,conn)

    create_table_with_df(c,conn,df,target)

    df2db(df,20,target,c,conn)
    
    
if __name__=="__main__":
    