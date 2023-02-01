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

# import sqlalchemy
import mariadb

import cx_Oracle
cx_Oracle.init_oracle_client(lib_dir=r"D:\Workspace\push2see_ingest\db\oracle\instantclient_21_8")

import __main__

#----------------------------------------- Variable ------------------------------------------
EXEC_PARA  ='N/A'
EXEC_PARA='-f <Config file>'
conn = None
iRet = 0
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
        :cx_Oracle sql statement don't require ';' end of SQL Statement
        :returns: no
            
    >>> _db_create_db(_db_connect('Mariadb','127.0.0.1','root','example',3306),"CREATE DATABASE IF NOT EXISTS STG_TEST_1;")
    >>> _db_create_db(_db_connect('Mariadb','127.0.0.1','root','example',3306),"CREATE DATABASE IF NOT EXISTS STG_TEST_2;")
    >>> _db_create_db(_db_connect('Mariadb','127.0.0.1','root','example',3306),"CREATE DATABASE IF NOT EXISTS STG_DB;")
    >>> _db_create_db(_db_connect('Mariadb','127.0.0.1','root','example',3306),"DROP DATABASE IF EXISTS STG_TEST_1;")
    >>> _db_create_db(_db_connect('Mariadb','127.0.0.1','root','example',3306),"DROP DATABASE IF EXISTS STG_TEST_2;")
    >>> _db_create_db(_db_connect('Mariadb','127.0.0.1','root','example',3306),"DROP DATABASE IF EXISTS STG_DB;")

    >>> _db_create_db(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"CREATE USER STG_TEST_1 IDENTIFIED BY password")    
    >>> _db_create_db(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"DROP USER STG_TEST_1 CASCADE")
    >>> _db_create_db(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"DROP USER STG_DB CASCADE")
    """

    try:    
        db_conn.execute(sql_create_db)
    except mariadb.Error as e:
        log.error(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)                
    except cx_Oracle.Error as e:
        (error,) = e.args

        if error.code == 1920 or error.code == 1918:
            log.warning("Warning Oracle Code ORA-{0:05d} and Skiped".format(error.code))
        else:
            log.error(f"Error connecting to Oracle Platform: {e}")
            log.error("Error connecting to Oracle Platform: {}".format(error.code))
            sys.exit(1)                   
    except Exception as e:
        log.error('Other error {} {}'.format(type(e),e))
        # _usage(sys.argv)
        sys.exit(2)

def _db_create_tb(db_conn,sql_create_tb) :
    """
    Args:
        :param db_conn: database connection
               sql_create_db: sql create database
        :returns: no
            
    >>> _db_create_db(_db_connect('Mariadb','127.0.0.1','root','example',3306),"CREATE DATABASE IF NOT EXISTS STG_DB;")
    >>> _db_create_tb(_db_connect('Mariadb','127.0.0.1','root','example',3306),"CREATE TABLE IF NOT EXISTS STG_DB.TEST (test int);")
    >>> _db_create_tb(_db_connect('Mariadb','127.0.0.1','root','example',3306),"DROP TABLE IF EXISTS STG_DB.TEST;")

    >>> _db_create_db(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"CREATE USER STG_TEST_1 IDENTIFIED BY password")
    >>> _db_create_tb(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"GRANT UNLIMITED TABLESPACE TO STG_TEST_1")
    >>> _db_create_tb(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"CREATE TABLE STG_TEST_1.TEST(test int)")
    >>> _db_create_db(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"DROP USER STG_TEST_1 CASCADE")
    """    
    try:
        db_conn.execute(sql_create_tb)        
    except mariadb.Error as e:
        log.error(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)                
    except cx_Oracle.Error as e:
        (error,) = e.args

        if error.code == 955:            
            log.warning("Warning Oracle Code ORA-{0:05d} and Skiped".format(error.code))
        else:
            log.error(f"Error connecting to Oracle Platform: {e}")
            log.error("Error connecting to Oracle Platform: {}".format(error.code))
            sys.exit(1)                   

    except Exception as e:
        log.error('Other error {} {}'.format(type(e),e))
        # _usage(sys.argv)
        sys.exit(2)        

def _db_delete_tb(db_conn,sql_delete_tb) :
    """
    Args:
        :param db_conn: database connection
               sql_delete_tb: sql delete database
        :returns: no
            
    >>> _db_create_tb(_db_connect('Mariadb','127.0.0.1','root','example',3306),"CREATE TABLE IF NOT EXISTS STG_DB.TEST (test int);")
    >>> _db_delete_tb(_db_connect('Mariadb','127.0.0.1','root','example',3306),"TRUNCATE TABLE STG_DB.TEST;")
    >>> _db_create_tb(_db_connect('Mariadb','127.0.0.1','root','example',3306),"DROP TABLE IF EXISTS STG_DB.TEST;")

    >>> _db_create_db(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"CREATE USER STG_TEST_1 IDENTIFIED BY password")    
    >>> _db_create_db(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"GRANT UNLIMITED TABLESPACE TO STG_TEST_1")    
    >>> _db_create_tb(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"CREATE TABLE STG_TEST_1.TEST(test int)")
    >>> _db_delete_tb(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"TRUNCATE TABLE STG_TEST_1.TEST")
    >>> _db_create_db(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"DROP USER STG_TEST_1 CASCADE")
    """    

    try:    
        db_conn.execute(sql_delete_tb)
    except mariadb.Error as e:
        log.error(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)                
    except Exception as e:
        log.error('Other error {} {}'.format(type(e),e))
        # _usage(sys.argv)
        sys.exit(2)                

def _db_ingest_file(db_conn,sql_bulk_load,sql_delete_tailer) :
    """
    Args:
        :param db_conn: database connection
               sql_bulk_load: sql bulk load
        :returns: number of record
            
    >>> _db_create_tb(_db_connect('Mariadb','127.0.0.1','root','example',3306),"CREATE TABLE IF NOT EXISTS STG_DB.TEST (test int);")
    >>> _db_ingest_file(_db_connect('Mariadb','127.0.0.1','root','example',3306),SQL_BLUK_LOAD,None)
    10
    >>> _db_create_db(_db_connect('Mariadb','127.0.0.1','root','example',3306),"DROP DATABASE IF EXISTS STG_DB;")
    """    

    try:            
        db_conn.execute(sql_bulk_load)
        
        ingest_record = db_conn.rowcount
        delete_tailer = 0

        # delete tailer record
        if sql_delete_tailer != None:
            db_conn.execute(sql_delete_tailer)
            delete_tailer = db_conn.rowcount

            log.info("{:0,.0f} tailer record(s) deleted".format(delete_tailer))

        ingest_record = ingest_record - delete_tailer
        log.info("{:0,.0f} record(s) on RowCount".format(ingest_record))

        return ingest_record
    except mariadb.Error as e:
        log.error(f"Error MariaDB Platform: {e}")
        sys.exit(1)                
    except Exception as e:
        log.error('Other error {} {}'.format(type(e),e))
        # _usage(sys.argv)
        sys.exit(2)         

def _db_ingest_records(db_conn,sql_bulk_load,DataFrame) :
    """
    Args:
        :param db_conn: database connection
               sql_bulk_load: sql bulk load
        :returns: number of record
            
    >>> _db_create_db(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"CREATE USER STG_TEST_1 IDENTIFIED BY password")    
    >>> _db_create_db(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"GRANT UNLIMITED TABLESPACE TO STG_TEST_1")    
    >>> _db_create_tb(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"CREATE TABLE STG_TEST_1.TEST(test varchar(255))")
    >>> _db_delete_tb(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"TRUNCATE TABLE STG_TEST_1.TEST")
    >>> _db_ingest_records(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"INSERT INTO STG_TEST_1.TEST (test) VALUES (:0)",test_df)
    10
    >>> _db_create_db(_db_connect('Oracle','127.0.0.1:1521/xe','system','oracle',1521),"DROP USER STG_TEST_1 CASCADE")
    """    

    try:
        # Convert all columns to string
        # DataFrame = DataFrame.astype(str)

        # Creating a list of tupples from the dataframe values
        tpls = [tuple(x) for x in DataFrame.to_numpy()]
        
        db_conn.prepare(sql_bulk_load)
        db_conn.executemany(None,tpls)
        
        log.info("{:0,.0f} record(s) on RowCount".format(db_conn.rowcount))        
        return db_conn.rowcount
    except mariadb.Error as e:
        log.error(f"Error MariaDB Platform: {e}")
        sys.exit(1)
    except cx_Oracle.Error as e:
        log.error(f"Error Oracle Platform: {e}")
        sys.exit(1)
    except Exception as e:
        log.error('Other error {} {}'.format(type(e),e))
        # _usage(sys.argv)
        sys.exit(2)         


def _db_connect(db_type,host_db,user_db,pass_db,port_db) :

    """
    Args:
        :param db_type: String Type of Database
               stg_db: database
        :returns: database connection
            
    >>> _db_connect('Mariadb','127.0.0.1','root','example',3306) # doctest: +ELLIPSIS
    <mariadb.connection.cursor object at ...
    >>> _db_connect('NONO','127.0.0.1','root','example',3306) # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    SystemExit: 2

    >>> _db_connect('Oracle','127.0.0.1:1521/xe','system','oracle','1521') # doctest: +ELLIPSIS
    <cx_Oracle.Cursor on <cx_Oracle.Connection to ...
    """

    try:
        if db_type == "Mariadb":
            # Connect to MariaDB Platform
            conn = mariadb.connect(
                user=user_db,
                password=pass_db,
                host=host_db,
                port=int(port_db),
                autocommit=True,
                local_infile = 1
                # database=stg_db
            )        
        elif db_type == "Oracle":
            conn = cx_Oracle.connect(
                user=user_db,
                password=pass_db,
                dsn=host_db,
                encoding="UTF-8")

            conn.autocommit = True

            # log.info("Database version: {}".format(conn.version))
            # log.info("Client version: {}".format(cx_Oracle.clientversion()))

        else:
            log.error("No DB Supports")
            sys.exit(2)

        # opts, args = getopt.getopt(sys.argv[1:],"hvf:t:d:",["conf=","type=","test"])
    except mariadb.Error as e:
        log.error(f"Error connecting to MariaDB Platform: {e}")    
        sys.exit(1)        
    except cx_Oracle.Error as e:
        log.error(f"Error connecting to Oracle Platform: {e}")
        sys.exit(1)        
    except Exception as e:
        log.error('Other error {} {}'.format(type(e),e))
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

    # Create DataFrame
    INPUT_PATH = config['Golbal']['INPUT_PATH']
    test_df = pd.read_csv(INPUT_PATH + "\\TEST.txt",dtype=str)

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
        # conn   = None
        db_cur = None
        DB_HOST = config[DB_TYPE]['HOST_DB']
        DB_USER_DB = config[DB_TYPE]['USER_DB']
        DB_PASS_DB = config[DB_TYPE]['PASS_DB']
        DB_PORT_DB = config[DB_TYPE]['PORT_DB']

        db_cur = _db_connect(DB_TYPE,DB_HOST,DB_USER_DB,DB_PASS_DB,DB_PORT_DB)
        # db_cur = conn.cursor()

        log.info('Check Database: {} and create if not found'.format(STG_DATABASE))
        SQL_CREATE_DB = SQL_CREATE_DB.replace("{STG_DATABASE}",STG_DATABASE)
        _db_create_db(db_cur,SQL_CREATE_DB)
        
        # read input file
        SQL_CREATE_TB = config[DB_TYPE]['SQL_CREATE_TB']
        SQL_TRUNCATE_DB = config[DB_TYPE]['SQL_TRUNCATE_DB']
        SQL_BLUK_LOAD = config[DB_TYPE]['SQL_BLUK_LOAD']

        # read file to dataFrame
        df = pd.read_csv(INPUT_PATH + "\\" + INPUT_FILENAME, sep = DELIMETER, dtype=str)

        if TAILER :
            # get last row and first coluns
            TAILER_COLUMN = df.columns[0]
            TAILER_VALUE  = df[TAILER_COLUMN].values[-1:][0]

            SQL_DELETE_TAILER = config[DB_TYPE]['SQL_DELETE_TAILER']
            SQL_DELETE_TAILER = SQL_DELETE_TAILER.replace("{STG_DATABASE}",STG_DATABASE)
            SQL_DELETE_TAILER = SQL_DELETE_TAILER.replace("{STG_TABLE}",STG_TABLE)
            SQL_DELETE_TAILER = SQL_DELETE_TAILER.replace("{COL}",TAILER_COLUMN)
            SQL_DELETE_TAILER = SQL_DELETE_TAILER.replace("{VALUE}",TAILER_VALUE)

            log.info("Get Last row of columns {} = '{}'".format(TAILER_COLUMN, TAILER_VALUE))
            
            log.info('Remove last row because TAILER: {}'.format(TAILER))

            # remove tailer record
            df = df[:-1]

        else:
            SQL_DELETE_TAILER = None

        # iterating the columns
        list_cols_create_tb = ""
        list_cols_insert = ""
        list_values_insert = ""
        index = 0
        for col in df.columns:
            # log.info('Column : \'{}\''.format(col))    
            # print("{} VARCHAR(255),".format(col) )
            list_cols_create_tb = list_cols_create_tb + "\t{} VARCHAR(255),".format(col) +"\n"            
            list_cols_insert = list_cols_insert + "{},".format(col)
            list_values_insert = list_values_insert + ":{},".format(index)
            index = index + 1

        # using negative indexing
        list_cols_create_tb = list_cols_create_tb[:-2]
        list_cols_insert    = list_cols_insert[:-1]
        list_values_insert  = list_values_insert[:-1]
        
        SQL_CREATE_TB = SQL_CREATE_TB.replace("{STG_DATABASE}",STG_DATABASE)
        SQL_CREATE_TB = SQL_CREATE_TB.replace("{STG_TABLE}",STG_TABLE)
        SQL_CREATE_TB = SQL_CREATE_TB.replace("{LIST_COLS}",list_cols_create_tb)

        # Oracle require grant TABLESPACE
        if DB_TYPE == "Oracle":
            SQL_GRANT_TABLE_SPACE = config[DB_TYPE]['SQL_GRANT_TABLE_SPACE']
            SQL_GRANT_TABLE_SPACE = SQL_GRANT_TABLE_SPACE.replace("{STG_DATABASE}",STG_DATABASE)

            log.info('Grant Unlimited Table Space to User: {}'.format(STG_DATABASE))
            _db_create_tb(db_cur,SQL_GRANT_TABLE_SPACE)

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
        
        log.info('Delete records on table: {}.{}'.format(STG_DATABASE,STG_TABLE))
        _db_delete_tb(db_cur,SQL_TRUNCATE_DB)

        # ingest data to table
        log.info('Loading file: {} into table: {}.{}'.format(INPUT_PATH + "\\" + INPUT_FILENAME,STG_DATABASE,STG_TABLE))

        if DB_TYPE == "Mariadb":            
            number_of_record = _db_ingest_file(db_cur,SQL_BLUK_LOAD,SQL_DELETE_TAILER)
        elif DB_TYPE == "Oracle":
            SQL_BLUK_LOAD = SQL_BLUK_LOAD.replace("{COLS}",list_cols_insert)
            SQL_BLUK_LOAD = SQL_BLUK_LOAD.replace("{VALUES}",list_values_insert)
            number_of_record = _db_ingest_records(db_cur,SQL_BLUK_LOAD,df)            
        else:
            log.error("No DB Supports")
            sys.exit(2)            

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
        
    except Exception as inst:
        print('-----------------------------------------------------------------')
        print(type(inst))
        print(inst.args) 
        print(inst)
        print('-----------------------------------------------------------------')
        raise
    finally:
        if conn is not None:
            conn.close()
        
        sys.exit(iRet) 
