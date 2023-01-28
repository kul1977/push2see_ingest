This is Example Project Data Ingest version 0.1.0 Push2See_IngesT
=====================================

General Information
-------------------
- Website: https://www.python.org
- Source code: https://github.com/python/cpython
- DocTest: https://docs.python.org/3/library/doctest.html#
- DockerDesktop: https://www.docker.com/products/docker-desktop/
- MariaDB: https://hub.docker.com/_/mariadb

For more complete instructions on contributing to CPython development,

Build Instructions
------------------

On Unix, Linux, macOS, and Windows::

    git clone https://github.com/kul1977/push2see_ingest.git

You can clone project from git to your notebook and should be install prerequisite.

Prerequisite
------------------
- Visual Studio Code 1.71.10 or Lasted version
- Python 3.7 or higher
- Python module pandas
- Python module doctest
- Docker Desktop 20.x.x or Lasted version
- Docker image Database Mariadb 10.9.4

5 Steps prepare database & execute data ingest by Python
------------------
1. Download script from Git
2. Manage Stack Database MariaDB
3. Setup Module Python
4. Execute Unit Test by doctest
5. ingest data into table STG_DB.CUSTOMER or STG_DB.VENDOR


Step 1 Download script from Git
------------------

    git clone https://github.com/kul1977/push2see_ingest.git


Step 2 Start Database
------------------

    docker-compose -f db\mariadb\docker-compose.yml up -d
    
Step 3 Setup Module Python
------------------

    python -m venv env
    env/Scripts/activate.bat (Window)
    env/Scripts/activate.sh (Linux,MacOS)
    
Step 4 Execute Unit Test by doctest
------------------
    
    python bin\ingest.py --test
    

Step 5 ingest data into table STG_DB.CUSTOMER or STG_DB.VENDOR
------------------

 table: STG_DB.CUSTOMER
 ------------------
    python bin\ingest.py -f conf\config.ini -t Customer
    
 table: STG_DB.VENDOR
 ------------------    
    python bin\ingest.py -f conf\config.ini -t Vendor
    

Detail steps on Medium : https://medium.com/@kul1977/%E0%B8%97%E0%B8%94%E0%B8%A5%E0%B8%AD%E0%B8%87%E0%B8%AA%E0%B8%A3%E0%B9%89%E0%B8%B2%E0%B8%87-framework-data-ingest-by-python-32baed6e783d

    
    
