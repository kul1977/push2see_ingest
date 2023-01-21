import time
from datetime import datetime
from beautifultable import BeautifulTable
from tabulate import tabulate
import sys, getopt
import pandas as pd

import logging
import logging.handlers

#----------------------------------------- Variable ------------------------------------------
EXEC_PARA='-v'


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


#------------------------------------------ Argment ------------------------------------------
try:
    opts, args = getopt.getopt(sys.argv[1:],"hvf:t:",["conf=","type=","test"])
except getopt.GetoptError:
    # log.info('usage: {} -f <config file>'.format(sys.argv[0]))
    _usage(sys.argv)
    sys.exit(2)

# set initiai value
CONF = "N/A"
FILE_TYPE = "N/A"
MODE = "N/A"


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

def _show_df_table(data, arrayCols) :
    """_show_df_table Show data framework to table on terminal

    Args:
        :data   array / Series
        :arrayCols  array of columns name        


    Returns:    True / False

    >>> _show_df_table([1,2,3,4],['Values'])
    +----+----------+
    |    |   Values |
    |----+----------|
    |  0 |        1 |
    |  1 |        2 |
    |  2 |        3 |
    |  3 |        4 |
    +----+----------+
    True
    >>> _show_df_table('a',['Values']) # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    TypeError: exceptions must derive from BaseException
    """

    # Create the pandas DataFrame
    try:    
        df = pd.DataFrame(data, columns=arrayCols)                            
        print(tabulate(df, headers='keys', tablefmt='psql'))
        return True
    except:
        raise ("Error can't call tabulate")

# Support doctest
if __name__ == "__main__" :
    log.info("Start Unit Test")
    import doctest
    doctest.testmod()
    # doctest.testmod(verbose = True)-
    log.info("Finish Unit Test")
