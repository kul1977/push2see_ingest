import re
# import csv

import pandas as pd
import logging
import logging.handlers
from datetime import datetime
from tabulate import tabulate
import sys, getopt
import os

import time
import glob
import configparser
#from faker import Faker
import mariadb
import __main__

#----------------------------------------- Variable ------------------------------------------
EXEC_PARA  ='N/A'
INPUT_PATH = r'D:\Workspace\CPF-IT\bin\00_pre-input'
POST_PATH  = r'D:\Workspace\CPF-IT\bin\01_input'
EXEC_PARA='-f <Config file>'

#----------------------------------------- Logging ------------------------------------------
# logging.basicConfig()

logFormatter = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')

log = logging.getLogger()
# log.setLevel(logging.DEBUG)
log.setLevel(logging.INFO)


#----------------------------------------- Function ------------------------------------------
def _usage(argv) :
    print ('Invalid parameters:',argv[0],EXEC_PARA)
    sys.exit(2)
    

def _statistic(start_time) :
    """
    Given start time and calculate duration of processing time.

    :param start_time: current time
    :return: start time - current time
    
    :# doctest: +SKIP
    :# doctest: +ELLIPSIS

    >>> _statistic(time.time())
    0.0
    >>> _statistic(time.time()-1)
    1.0
    >>> _statistic(time.time()-12345)
    12345.0
    """

    Finish_time = time.time()
    return(Finish_time - start_time)

def _filename_output(filename) :
    """
    Given pattern filename, return filename after replace fomula.

    :param filename: filename pattern
    :return: filename after calculate on fomula

    >>> _filename_output("CUS_{YYYYMMDD_HHMMSS}.txt") # doctest: +ELLIPSIS
    'CUS_2..._....txt'
    >>> _filename_output("CUS_{YYYYMMDD_HHMM}.txt") # doctest: +ELLIPSIS 
    'CUS_2..._....txt'
    >>> _filename_output("CUS_{YYYYMMDD_HH}.txt") # doctest: +ELLIPSIS 
    'CUS_2..._....txt'
    >>> _filename_output("CUS_{YYYYMMDD}.txt") # doctest: +ELLIPSIS 
    'CUS_2....txt'
    >>> _filename_output("CUS_{ERROR}.txt")  # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: Error Unknow pattern filename ...
    """


    if "{YYYYMMDD_HHMMSS}" in filename :
        out_filename = filename.replace('{YYYYMMDD_HHMMSS}',datetime.today().strftime('%Y%m%d_%H%M%S'))
    elif "{YYYYMMDD_HHMM}" in filename :
        out_filename = filename.replace('{YYYYMMDD_HHMM}',datetime.today().strftime('%Y%m%d_%H%M'))
    elif "{YYYYMMDD_HH}" in filename :
        out_filename = filename.replace('{YYYYMMDD_HH}',datetime.today().strftime('%Y%m%d_%H'))
    elif "{YYYYMMDD}" in filename :
        out_filename = filename.replace('{YYYYMMDD}',datetime.today().strftime('%Y%m%d'))
    elif "{YYYYMM}" in filename :
        out_filename = filename.replace('{YYYYMM}',datetime.today().strftime('%Y%m'))
    else:
        raise ValueError("Error Unknow pattern filename {}".format(filename))
        print("Error Unknow pattern filename {}".format(filename))
        print("Supports only ")
        print(" 1. X...X_{YYYYMMDD_HHMMSS}")
        print(" 2. X...X_{YYYYMMDD_HHMM}")
        print(" 3. X...X_{YYYYMMDD_HH}")
        print(" 4. X...X_{YYYYMMDD}")
        print(" 5. X...X_{YYYYMM}")
        sys.exit(2)

    return(out_filename)

def _delimeter(config_delimeter) :
    """_summary_

    Args:
        config_delimeter (_type_): _description_

    Returns:
        _type_: _description_
    >>> _delimeter('tab')
    '\\t'
    >>> _delimeter('comma')
    ','
    >>> _delimeter('pipe')
    '|'
    >>> _delimeter('Unknow') # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: Error Unknow delimter ...
    """

    if config_delimeter in ("tab") :
        return "\t"
    elif config_delimeter in ("comma",',') :
        return ","
    elif config_delimeter in ("pipe",'|') :
        return "|"
    else:
        raise ValueError("Error Unknow delimter {}".format(config_delimeter))
        print("Error Unknow delimter {}".format(config_delimeter))
        sys.exit(2)

def _db_create_db(db_conn,sql_create_db) :
    """
    Args:
        :param db_conn: database connection
               sql_create_db: sql create database
        :returns: no
            
    >>> _db_create_db(_db_connect('Mariadb'),"CREATE DATABASE IF NOT EXISTS STG_TEST_1;")
    >>> _db_create_db(_db_connect('Mariadb'),"CREATE DATABASE IF NOT EXISTS STG_TEST_2;")
    >>> _db_create_db(_db_connect('Mariadb'),"CREATE DATABASE IF NOT EXISTS STG_DB;")
    >>> _db_create_db(_db_connect('Mariadb'),"DROP DATABASE IF EXISTS STG_TEST_1;")
    >>> _db_create_db(_db_connect('Mariadb'),"DROP DATABASE IF EXISTS STG_TEST_2;")
    >>> _db_create_db(_db_connect('Mariadb'),"DROP DATABASE IF EXISTS STG_DB;")
    """

    try:    
        db_conn.execute(sql_create_db)
    except mariadb.Error as e:
        log.error(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)                
    except:
        # log.error('usage: {} -f <config file>'.format(sys.argv[0]))
        # _usage(sys.argv)
        sys.exit(2)

def _db_create_tb(db_conn,sql_create_tb) :
    """
    Args:
        :param db_conn: database connection
               sql_create_db: sql create database
        :returns: no
            
    >>> _db_create_db(_db_connect('Mariadb'),"CREATE DATABASE IF NOT EXISTS STG_DB;")
    >>> _db_create_tb(_db_connect('Mariadb'),"CREATE TABLE IF NOT EXISTS STG_DB.TEST (test int);")
    >>> _db_create_tb(_db_connect('Mariadb'),"DROP TABLE IF EXISTS STG_DB.TEST;")
    """    
    try:    
        db_conn.execute(sql_create_tb)
    except mariadb.Error as e:
        log.error(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)                
    except:
        # log.error('usage: {} -f <config file>'.format(sys.argv[0]))
        # _usage(sys.argv)
        sys.exit(2)        

def _db_delete_tb(db_conn,sql_delete_tb) :
    """
    Args:
        :param db_conn: database connection
               sql_delete_tb: sql delete database
        :returns: no
            
    >>> _db_create_tb(_db_connect('Mariadb'),"CREATE TABLE IF NOT EXISTS STG_DB.TEST (test int);")
    >>> _db_delete_tb(_db_connect('Mariadb'),"TRUNCATE TABLE STG_DB.TEST;")
    >>> _db_create_tb(_db_connect('Mariadb'),"DROP TABLE IF EXISTS STG_DB.TEST;")
    """    

    try:    
        db_conn.execute(sql_delete_tb)
    except mariadb.Error as e:
        log.error(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)                
    except:
        # log.error('usage: {} -f <config file>'.format(sys.argv[0]))
        # _usage(sys.argv)
        sys.exit(2)                

def _db_ingest_file(db_conn,sql_bulk_load) :
    """
    Args:
        :param db_conn: database connection
               sql_bulk_load: sql bulk load
        :returns: number of record
            
    >>> _db_create_tb(_db_connect('Mariadb'),"CREATE TABLE IF NOT EXISTS STG_DB.TEST (test int);")
    >>> _db_ingest_file(_db_connect('Mariadb'),SQL_BLUK_LOAD)
    10
    >>> _db_create_db(_db_connect('Mariadb'),"DROP DATABASE IF EXISTS STG_DB;")
    """    

    try:            
        db_conn.execute(sql_bulk_load)
        log.info("{:0,.0f} record(s) on RowCount".format(db_conn.rowcount))

        return db_conn.rowcount
    except mariadb.Error as e:
        log.error(f"Error MariaDB Platform: {e}")
        sys.exit(1)                
    except:
        log.error('usage: {} -f <config file>'.format(sys.argv[0]))
        # _usage(sys.argv)
        sys.exit(2)         

def _db_connect(db_type) :

    """
    Args:
        :param db_type: String Type of Database
               stg_db: database
        :returns: database connection
            
    >>> _db_connect('Mariadb') # doctest: +ELLIPSIS
    <mariadb.connection.cursor object at ...
    >>> _db_connect('NONO') # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    SystemExit: 2
    """

    try:
        if db_type == "Mariadb":
            # Connect to MariaDB Platform
            conn = mariadb.connect(
                user="root",
                password="example",
                host="127.0.0.1",
                port=3306,
                autocommit=True,
                local_infile = 1                
                # database=stg_db
            )            
        else:
            log.error("No DB Supports")
            sys.exit(2)

        # opts, args = getopt.getopt(sys.argv[1:],"hvf:t:d:",["conf=","type=","test"])
    except mariadb.Error as e:
        log.error(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)        
    except:
        # log.error('usage: {} -f <config file>'.format(sys.argv[0]))
        # _usage(sys.argv)
        sys.exit(2)

    # return Cursor
    return conn.cursor()

#------------------------------------------ Argment ------------------------------------------
try:
    opts, args = getopt.getopt(sys.argv[1:],"hvf:t:d:",["conf=","type=","test"])
except getopt.GetoptError:
    # log.info('usage: {} -f <config file>'.format(sys.argv[0]))
    _usage(sys.argv)
    sys.exit(2)

# set initiai value
CONF = "N/A"
FILE_TYPE = "N/A"
DB_TYPE = "N/A"
MODE = "N/A"

INPUT_PATH     = "N/A"
INPUT_FILENAME = "N/A"

for opt, arg in opts:
    # log.info(opt)
    if opt == '-h':
        # log.info('usage: {} -f <config file>'.format(sys.argv[0]))
        _usage(sys.argv)
        sys.exit(2)
    elif opt in ("-f", "--conf"):
        CONF = arg
    elif opt in ("-t", "--type"):
        FILE_TYPE = arg
    elif opt in ("-d", "--database"):
        DB_TYPE = arg        
    elif opt in ("-v","--test"):
        MODE = "Unit Test"        
    else:
        _usage(sys.argv)

# check argment
if len(sys.argv) <= 1 :
    _usage(sys.argv)


#------------------------------------------- Main --------------------------------------------

log.info("__name__ : {}".format(__name__))



if __name__ == "__main__" and MODE == "Unit Test":
    log.info("Start Unit Test")
    import doctest
    
    # You do your tests:
    config = configparser.ConfigParser()
    config.read("conf\config.ini")
    SQL_BLUK_LOAD = config["Mariadb"]['SQL_BLUK_LOAD']

    SQL_BLUK_LOAD = SQL_BLUK_LOAD.replace("{STG_DATABASE}","STG_DB")
    SQL_BLUK_LOAD = SQL_BLUK_LOAD.replace("{STG_TABLE}","TEST")
    SQL_BLUK_LOAD = SQL_BLUK_LOAD.replace("{DELIMETER}","|")
    SQL_BLUK_LOAD = SQL_BLUK_LOAD.replace("{FULL_FILENAME}","D:\\\\Workspace\\\\push2see_ingest\\\\input\\\\TEST.txt")

    doctest.testmod()
    # doctest.testmod(verbose = True)
    log.info("Finish Unit Test")
else:
    config = configparser.ConfigParser()
    config.read(CONF)

    Start_time = time.time()

    if DB_TYPE == "N/A" :
        DB_TYPE =  "Mariadb"

    # check exists
    if FILE_TYPE not in config :
        raise KeyError("Error not found key : {} on config : {}".format(FILE_TYPE,CONF))

    # OUTPUT_PATH = config['Golbal']['OUTPUT_PATH']
    DEGUG_LEVEL = config['Golbal']['DEGUG_LEVEL']
    INPUT_PATH = config['Golbal']['INPUT_PATH']
    STG_DATABASE = config['Golbal']['STG_DATABASE']
    
    INPUT_FILENAME = config[FILE_TYPE]['INPUT_FILENAME']
    HEADER = True if "Yes" == config[FILE_TYPE]['HEADER'] else False
    TAILER = True if "Yes" == config[FILE_TYPE]['TAILER'] else False
    
    DB_TYPE = config[FILE_TYPE]['DB_TYPE']
    DELIMETER = _delimeter(config[FILE_TYPE]['DELIMETER'])


    SQL_CREATE_DB = config[DB_TYPE]['SQL_CREATE_DB']
    STG_TABLE = FILE_TYPE.upper()

    # number_of_record = int(config[FILE_TYPE]['NUM_OF_RECORD'])
    # arr_columns = config[FILE_TYPE]['COLUMNES'].split(',')

    # arr_header_columns = arr_columns.copy()

    # pre calculate speical columes


    
    # for i in range(len(arr_columns)):    
    #     if "->" in arr_columns[i]:
    #         colume_special = arr_columns[i].split("->")[1]
    #         arr_columns[i] = config['Special_Columns'][colume_special]
    #         arr_header_columns[i] = colume_special

    log.info("Start")
    log.info('-----------------------------------------------------------------')
    log.info('DEGUG_LEVEL : {}'.format(config['Golbal']['DEGUG_LEVEL']))
    log.info('STG_DATABASE : \'{}\''.format(STG_DATABASE))    
    log.info('STG_TABLE : \'{}\''.format(STG_TABLE))    
    
    log.info('INPUT_PATH : \'{}\''.format(INPUT_PATH))
    log.info('Type : \'{}\''.format(config[FILE_TYPE]['TYPE']))
    # log.info('NUM_OF_RECORD : {:0,.0f}'.format(int(config[FILE_TYPE]['NUM_OF_RECORD'])))
    # log.info('COLUMNES HEADER : {}'.format(arr_header_columns))
    # log.info('COLUMNES RECORD : {}'.format(arr_columns))        
    log.info('DB_TYPE : \'{}\''.format(DB_TYPE))
    log.info('DELIMETER : \'{}\''.format(DELIMETER))    
    log.info('INPUT_FILENAME : \'{}\''.format(INPUT_FILENAME))
    log.info('DB_TYPE : \'{}\''.format(DB_TYPE))    
    log.info('-----------------------------------------------------------------')

    try:
        db_cur = None

        db_cur = _db_connect(DB_TYPE)

        log.info('Check Database: {} and create if not found'.format(STG_DATABASE))
        SQL_CREATE_DB = SQL_CREATE_DB.replace("{STG_DATABASE}",STG_DATABASE)
        _db_create_db(db_cur,SQL_CREATE_DB)
        
        # read input file
        SQL_CREATE_TB = config[DB_TYPE]['SQL_CREATE_TB']
        SQL_TRUNCATE_DB = config[DB_TYPE]['SQL_TRUNCATE_DB']
        SQL_BLUK_LOAD = config[DB_TYPE]['SQL_BLUK_LOAD']

        # read file to dataFrame
        df = pd.read_csv(INPUT_PATH + "\\" + INPUT_FILENAME, sep = DELIMETER)

        # iterating the columns
        list_cols = ""
        for col in df.columns:
            # log.info('Column : \'{}\''.format(col))    
            # print("{} VARCHAR(255),".format(col) )
            list_cols = list_cols + "\t{} VARCHAR(255),".format(col) +"\n"

        # using negative indexing
        list_cols = list_cols[:-2]
        
        SQL_CREATE_TB = SQL_CREATE_TB.replace("{STG_DATABASE}",STG_DATABASE)
        SQL_CREATE_TB = SQL_CREATE_TB.replace("{STG_TABLE}",STG_TABLE)
        SQL_CREATE_TB = SQL_CREATE_TB.replace("{LIST_COLS}",list_cols)
        
        # print(SQL_CREATE_TB)
        log.info('Check Table: {}.{} and create if not found'.format(STG_DATABASE,FILE_TYPE.upper()))
        _db_create_tb(db_cur,SQL_CREATE_TB);        

        log.info('-----------------------------------------------------------------')



        log.info('Show simple data on file')
        log.info('-----------------------------------------------------------------')
        # print(df.head().to_string()) 
        print(tabulate(df.head(), headers='keys', tablefmt='psql'))
        df_rows = len(df.index)
        # log.info('-----------------------------------------------------------------')    


        # Bluk load
        FULL_FILENAME = INPUT_PATH + "\\" + INPUT_FILENAME
        FULL_FILENAME = FULL_FILENAME.replace("\\","\\\\")

        SQL_BLUK_LOAD = SQL_BLUK_LOAD.replace("{STG_DATABASE}",STG_DATABASE)
        SQL_BLUK_LOAD = SQL_BLUK_LOAD.replace("{STG_TABLE}",FILE_TYPE.upper())
        SQL_BLUK_LOAD = SQL_BLUK_LOAD.replace("{DELIMETER}",DELIMETER)
        SQL_BLUK_LOAD = SQL_BLUK_LOAD.replace("{FULL_FILENAME}",FULL_FILENAME)

        SQL_TRUNCATE_DB = SQL_TRUNCATE_DB.replace("{STG_DATABASE}",STG_DATABASE)
        SQL_TRUNCATE_DB = SQL_TRUNCATE_DB.replace("{STG_TABLE}",STG_TABLE)
        # SQL_BLUK_LOAD = SQL_BLUK_LOAD.replace("\\","\\\\")

        # print(SQL_BLUK_LOAD)
        log.info('-----------------------------------------------------------------')
        log.info('Loading file: {} into table: {}.{}'.format(INPUT_PATH + "\\" + INPUT_FILENAME,STG_DATABASE,STG_TABLE))
        _db_delete_tb(db_cur,SQL_TRUNCATE_DB)
        number_of_record = _db_ingest_file(db_cur,SQL_BLUK_LOAD)

        Duration = _statistic(Start_time)
        log.info('{:0,.0f} record(s) on dataFrame'.format(df_rows))
        log.info('{:0,.0f} record(s) on table'.format(number_of_record))

        iRet = 0                
        if df_rows != number_of_record :
            iRet = 1

        log.info('Verify: {}'.format(df_rows == number_of_record))            

        log.info("{:0,.2f} sec(s)".format(Duration))
        log.info("{:0,.2f} record(s) per sec".format(number_of_record/Duration))
        log.info('-----------------------------------------------------------------')        

        if iRet == 0:
            log.info("Done")
        else:
            log.info("False")

        sys.exit(iRet) 

    except Exception as inst:
        print('-----------------------------------------------------------------')
        print(type(inst))
        print(inst.args) 
        print(inst)
        print('-----------------------------------------------------------------')
        raise
