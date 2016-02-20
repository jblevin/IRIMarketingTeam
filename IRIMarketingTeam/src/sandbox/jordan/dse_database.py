import sys
import time
import numpy
import math
import pandas as pd
from pandas import DataFrame
import mysql.connector


class DSEDatabase:


    def __init__(self, user, password, database='heavylifters', host='mandrake.arvixe.com'):
        '''
        database: the database to connect to
        host: the name of the host server
        user: the user name to use for the connection
        password: the password of the user for the database to connect to
        '''
        self._cnx = None
        self._database = database
        self._host = host
        self._user = user
        self._password = password
        
        self._connect()
        
        
    def __del__(self):
        print('DSEDatabase instance shutting down...')
        self._disconnect()
        self._cnx.close()

    
    def _connect(self):
        config = {
                    'user':self._user,
                    'password':self._password,
                    'host':self._host,
                    'database':self._database
                 }

        try:
            self._cnx = mysql.connector.connect(**config)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        except:
            print('Error connecting to the database with the following exception:\r\n{0}'.format(sys.exc_info()[0]))
            
            
    def _disconnect(self):
        try:
            self._cnx.close()
        except:
            print('Error disconnecting:\r\n{0}'.format(sys.exc_info()[0]))
            
        
    def _commit(self):
        try:
            self._cnx.commit()
        except:
            print(sys.exc_info()[0])
            
            
    def fetch_df_tbl(self, tbl, cols, limit=10):
        '''
        returns the results of a query on one table in the database wrapped in a pandas DataFrame
        tbl: the name of the table to query
        cols: list of column names to return
        limit: number of rows to return; default: 10
        
        '''
        
        df_mysql = None
        
        try:
            
            projections = ','.join(cols)
            lmt = ' LIMIT ' + str(limit) if limit > 0 else ''
            sql = 'SELECT {0} FROM {1}{2}'.format(projections, tbl, lmt)
            
            df_mysql = pd.read_sql(sql, con=self._cnx)
            
        except:
            print('Error connecting to the database with the following exception:\r\n{0}'.format(sys.exc_info()[0]))
            
        return df_mysql
    
    
    def fetch_df_sql(self, sql):
        '''
        returns the results of a given sql query wrapped in a pandas DataFrame
        sql: the sql query to run against the database
        
        '''
        
        df_mysql = None
        
        try:
            
            df_mysql = pd.read_sql(sql, con=self._cnx)
            
        except:
            print('Error connecting to the database with the following exception:\r\n{0}'.format(sys.exc_info()[0]))
            
        return df_mysql
        
        
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
        