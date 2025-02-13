import pandas as pd
import numpy as np
from pyspark.sql import functions as F

def dropDupeCols(df):
    newcols = []
    dupcols = []

    for i in range(len(df.columns)):
        if df.columns[i].lower() not in [x.lower() for x in newcols]:
            newcols.append(df.columns[i])
        else:
            dupcols.append(i)

    df = df.toDF(*[str(i) for i in range(len(df.columns))])
    for dupcol in dupcols:
        df = df.drop(str(dupcol))

    return df.toDF(*newcols)
#---------------------------------------------------------------------------------- 
def path_exists(dbutils, path):
    """Function returns Boolean, true if a DataBricks/dbfs file path exists or
    false if it does not. 
    Parameters
    ----------
    dbutils: dbutils object
        DataBricks notebook dbutils object
    path : str
        DataBricks dbfs file storage path
    Returns
    ----------
    Boolean
    """            
    try:
        path = to_dbfs_path(path)
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise e      
#----------------------------------------------------------------------------------                    
def list_file_paths(dbutils, dir_path, ext='csv', path_type='os'):
    """Function lists files of a given extension type within a 
    given DataBricks/dbfs file path. 
    Parameters
    ----------
    dbutils: dbutils object
        DataBricks notebook dbutils object
    dir_path : str
        DataBricks dbfs file storage path
    ext : str
        File extension type to search for
        Default, csv
    path_type str
        Type of file paths to return. 
        Allowed options:
            'dbfs' returns databricks file store paths
            'os' returns local os type paths
            Default, 'os'
    Returns
    ----------
    fps : list
        List of file paths
    """      
    try:
        dir_path = to_dbfs_path(dir_path)
        if not path_exists(dbutils, dir_path):
            print(f'Directory not found: {dir_path}')
            return []
        if path_type =='os':
            fps = [db_path_to_local(f.path) 
                    for f in dbutils.fs.ls(dir_path) 
                    if ((f.path).lower()).endswith(f'.{ext.lower()}')]
        elif path_type =='dbfs':
            fps = [f.path 
                    for f in dbutils.fs.ls(dir_path) 
                    if ((f.path).lower()).endswith(f'.{ext.lower()}')]
        print(f'Found {len(fps)} {ext} file(s) within {dir_path}')
        return fps
    except Exception as e:
        raise e
#---------------------------------------------------------------------------------- 
def list_sub_dirs(dbutils, dir_path, recursive=False, ignore=['.parquet']):
    """Function lists sub directories of a given 
    DataBricks/dbfs file path. 
    Parameters
    ----------
    dbutils: dbutils object
        DataBricks notebook dbutils object
    dir_path : str
        DataBricks dbfs file storage path
    recursive : Boolean
        Boolean value for recursively list sub directories
        Default, False 
    ignore : list
        List of file types that are actaully folders to ignore
        Default, ['.parquet']
    Returns
    ----------
    sub_dirs : list
        Sorted list of sub directories
    """
    sub_dirs = [p.path for p in dbutils.fs.ls(dir_path) 
                if p.isDir() and p.path != dir_path and
                not os.path.abspath(p.path).lower().endswith(tuple(ignore))]
    if recursive:
        for sd in sub_dirs:
            sub_dirs = sub_dirs + list_sub_dirs(dbutils, sd, recursive)
    return sorted(sub_dirs)
#---------------------------------------------------------------------------------- 
def create_dir(dbutils, out_dir):
    """Function creates a directory if it does not exist. 
    Parameters
    ----------
    out_dir: str
        DataBricks dbfs file storage path 
    Returns
    ----------
    out_dir : Boolean
        True if the directory was created or exists
    """ 
    out_dir = to_dbfs_path(out_dir)
    
    try: 
        if not path_exists(dbutils, out_dir):
            dbutils.fs.mkdirs(out_dir)
            print(f'Created directory: {out_dir}')
        else:
            print(f'Directory already exists: {out_dir}')
        return True
    except:
        return False
#---------------------------------------------------------------------------------- 
def grab_correct_fabric_bdc(bdc_date, state_code):
  # TODO Create Docstring
    custom_fabric = spark.sql(f"SELECT custom_fabric FROM ba_planning_tool.state_lookup_table WHERE state_geoid = '{state_code}'").collect()[0][0]
    if custom_fabric != None:
        print(f'selecting {custom_fabric} Availability for fabric')
        availability_date = custom_fabric
        if (custom_fabric == datetime(2022, 12, 31).date()):
            bdc_df = spark.sql(f"SELECT * FROM ba_planning_tool.authoritative_bdc WHERE state_geoid='{state_code}' AND expiration_date > '{bdc_date}' AND effective_date <='{bdc_date}'")
            fabric = spark.sql(f"SELECT * FROM ba_planning_tool.fabric_cost_table WHERE state_geoid='{state_code}'")
            return bdc_df, fabric, availability_date
        elif (custom_fabric == datetime(2023,6,30).date()):
            bdc_df = spark.sql(f"SELECT * FROM ba_planning_tool.v3_authoritative_bdc WHERE state_geoid='{state_code}' AND expiration_date > '{bdc_date}' AND effective_date <='{bdc_date}'")
            fabric = spark.sql(f"SELECT * FROM ba_planning_tool.v3_fabric_cost_table WHERE state_geoid='{state_code}'")
            return bdc_df, fabric, availability_date
        elif (custom_fabric == datetime(2023,12,31).date()):
            bdc_df = spark.sql(f"SELECT * FROM ba_planning_tool.v4_authoritative_bdc WHERE state_geoid='{state_code}' AND expiration_date > '{bdc_date}' AND effective_date <='{bdc_date}'")
            fabric = spark.sql(f"SELECT * FROM ba_planning_tool.v4_fabric_cost_table WHERE state_geoid='{state_code}'")
            return bdc_df, fabric, availability_date
        else:
            raise Exception('Incorrect fabric selection; check format of custom fabric input in state lookup table')
    else:
        if (bdc_date >= datetime(2023, 11, 14).date()) & (bdc_date < datetime(2024, 5, 14).date()):
            print('selecting v3 fabric auth bdc')
            availability_date = datetime(2023, 6, 30).date() ##needed for later point in certain scripts

            bdc_df = spark.sql(
                    f"SELECT * FROM ba_planning_tool.v3_authoritative_bdc WHERE state_geoid='{state_code}' AND expiration_date > '{bdc_date}' AND effective_date <='{bdc_date}'"
                )
            fabric = spark.sql(f"SELECT * FROM ba_planning_tool.v3_fabric_cost_table WHERE state_geoid='{state_code}'")
            return bdc_df, fabric, availability_date
        elif bdc_date < datetime(2023, 11, 14).date():
            print('selecting v2 fabric auth bdc')
            availability_date = datetime(2022, 12, 31).date()

            bdc_df = spark.sql(
                    f"SELECT * FROM ba_planning_tool.authoritative_bdc WHERE state_geoid='{state_code}' AND expiration_date > '{bdc_date}' AND effective_date <='{bdc_date}'"
                )
            fabric = spark.sql(f"SELECT * FROM ba_planning_tool.fabric_cost_table WHERE state_geoid='{state_code}'")
            return bdc_df, fabric, availability_date
        elif (bdc_date >= datetime(2024, 5, 14).date()) & (bdc_date < datetime(2024, 11, 13).date()):
            print('selecting v4 fabric auth bdc')
            availability_date = datetime(2023, 12, 31).date()

            bdc_df = spark.sql(
                    f"SELECT * FROM ba_planning_tool.v4_authoritative_bdc WHERE state_geoid='{state_code}' AND expiration_date > '{bdc_date}' AND effective_date <='{bdc_date}'"
                )
            fabric = spark.sql(f"SELECT * FROM ba_planning_tool.v4_fabric_cost_table WHERE state_geoid='{state_code}'")

            return bdc_df, fabric, availability_date
        elif bdc_date >= datetime(2024, 11, 13).date():
            print('selecting v5 fabric auth bdc')
            availability_date = datetime(2024, 6, 30).date()

            bdc_df = spark.sql(
                    f"SELECT * FROM ba_planning_tool.v5_authoritative_bdc WHERE state_geoid='{state_code}' AND expiration_date > '{bdc_date}' AND effective_date <='{bdc_date}'"
                )
            fabric = spark.sql(f"SELECT * FROM ba_planning_tool.v5_fabric_no_cost WHERE state_geoid='{state_code}'")

            return bdc_df, fabric, availability_date
        else:
            raise Exception("Incorrect fabric selection date; check input date format")
