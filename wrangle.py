import pandas as pd
import numpy as np

import env
import os

def check_file_exists(filename, query, url):
    '''
    Args:
        filename (str): The name of the database
        query (str, optional): The SQL query to execute, 'SELECT * FROM...'
        url (env function): The  function in env.py file that connects to the SQL database.
        
        MUST have your own env.py file to replicate. Formatted as:
    
    def get_db_url(db, 
                   user=user, 
                   password=password, 
                   host=host):
        return (f'mysql+pymysql://{user}:{password}@{host}/{db}')
        
        
    As the name implies, this here is to see if the file(csv) we are calling/using exists AND what to do 
    if it doesn't exist. 
    If it doesn't exist, it will read the query using the url (env info) and makes it into a csv file!
    '''
    
    if os.path.exists(filename):
        print('this file exists, reading csv')
        df = pd.read_csv(filename, index_col=0)
    else:
        print('this file doesnt exist, read from sql, and export to csv')
        df = pd.read_sql(query, url)
        df.to_csv(filename)
        
    return df


def get_telco_data():
    '''
    Retrieves the telco dataframe. The MySQL query will return all columns from the customers table,
    with the three additional columns because of the joins using their ids.
    The check file function will assure the telco file exists and what to do if it doesn't.
    '''
    url = env.get_db_url('telco_churn')
    query = '''
    select *
    from customers
        join contract_types
            using (contract_type_id)
        join internet_service_types
            using (internet_service_type_id)
        join payment_types
            using (payment_type_id)
    '''
    
    filename = 'telco_churn.csv'

    #call the check_file_exists fuction 
    df = check_file_exists(filename, query, url)
    return df



# Read data from the student_grades table in the school_sample database on our mySQL server. 
def get_connection(db, user=env.user, host=env.host, password=env.password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

def get_student_data():
    filename = "student_grades.csv"

    if os.path.isfile(filename):

        return pd.read_csv(filename)
    else:
        # Create the url
        url = get_connection('school_sample')

        # Read the SQL query into a dataframe
        df = pd.read_sql('SELECT * FROM student_grades', url)

        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename)

        # Return the dataframe to the calling code
        return df
### This function is an "all-in-one" version of the "check_file_exists" function and the "get_db_data" function. Just another way it could be done! ###
def wrangle_grades():
    '''
    Read student_grades into a pandas DataFrame from mySQL,
    drop student_id column, replace whitespaces with NaN values,
    drop any rows with Null values, convert all columns to int64,
    return cleaned student grades DataFrame.
    '''

    # Acquire data

    grades = get_student_data()

    # Replace white space values with NaN values.
    grades = grades.replace(r'^\s*$', np.nan, regex=True)

    # Drop all rows with NaN values.
    df = grades.dropna()

    # Convert all columns to int64 data types.
    df = df.astype('int')

    return df
### connected with the two functions above it, was done in the book/LESSON, kept in here as an example ###



