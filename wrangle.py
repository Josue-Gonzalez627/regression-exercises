import pandas as pd
import numpy as np

import env
import os
from sklearn.model_selection import train_test_split

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


def splitting_data(df, seed=123): ### No longer needs the col key argument because we only stratify on Classification!
    '''
    Just like the splitting Titanic function but it can be used for any df now!
    must provide the df and column. Does not clean it though
    '''

    #first split
    train, validate_test = train_test_split(df,
                     train_size=0.6,
                     random_state=seed
                     #stratify=df[col]
                    )
    
    #second split
    validate, test = train_test_split(validate_test,
                                     train_size=0.5,
                                      random_state=seed
                                      #stratify=validate_test[col]
                        
                                     )
    print(f'train ----> {train.shape} 60%')
    print(f'validate -> {validate.shape}  20%')
    print(f'test -----> {test.shape}  20%')
    return train, validate, test



##### Read data from the student_grades table in the school_sample database on our mySQL server. #####
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
##### This function is an "all-in-one" version of the "check_file_exists" function and the "get_db_data" function. Just another way it could be done! ###
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
##### connected with the two functions above it, was done in the book/LESSON, kept in here as an example #####


def wrangle_exams():
    '''
    read csv from url into df, clean df, and return the prepared df
    '''
    
    # Read csv file into pandas DataFrame.
    file = "https://gist.githubusercontent.com/ryanorsinger/\
    14c8f919920e111f53c6d2c3a3af7e70/raw/07f6e8004fa171638d6d599cfbf0513f6f60b9e8/student_grades.csv"
    df = pd.read_csv(file)
    
    #replace blank space with null value
    df.exam3 = df.exam3.replace(' ', np.nan)
    
    #drop all nulls
    df = df.dropna()
    
    #change datatype to exam1 and exam3 to integers
    df.exam1 = df.exam1.astype(int)
    df.exam3 = df.exam3.astype(int)
    
    return df



def get_zillow_data(): 
    '''
    This function acquires zillow.csv if it is available.
    If not, it'll make the MYSQL connection and use the query below to 
    read it in the datafram.
    It'll also write/create the csv for next time.
    '''
    filename = 'zillow.csv'
    
    if os.path.isfile(filename):
        return pd.read_csv(filename, index_col=0)
    
    else:
        # Create the url
        url = env.get_db_url('zillow')
        
        sql_query = '''
        Select bathroomcnt,
        bedroomcnt,
        calculatedfinishedsquarefeet, 
        fips,
        taxamount,
        taxvaluedollarcnt,
        yearbuilt
        FROM properties_2017
        WHERE propertylandusetypeid = '261'
        '''
    # Read the SQL query into a df
    df = pd.read_sql(sql_query, url)
    
    # Write that df to disk for later. Called 'caching' the data for later.
    df.to_csv(filename)
    
    # Return the dataframe to the calling code
    return df

def get_zillow_sample_data(): 
    '''
    This function acquires zillow_sample.csv if it is available.
    If not, it'll make the MYSQL connection and use the query below to 
    read it in the datafram.
    It'll also write/create the csv for next time.
    '''
    filename = 'zillow_sample.csv'
    
    if os.path.isfile(filename):
        return pd.read_csv(filename, index_col=0)
    
    else:
        # Create the url
        url = env.get_db_url('zillow')
        
        sql_query = '''
        Select bathroomcnt,
        bedroomcnt,
        calculatedfinishedsquarefeet, 
        fips,
        taxamount,
        taxvaluedollarcnt,
        yearbuilt
        FROM properties_2017
        WHERE propertylandusetypeid = '261'
        LIMIT 10000
        '''
    # Read the SQL query into a df
    df_sample = pd.read_sql(sql_query, url)
    
    # Write that df to disk for later. Called 'caching' the data for later.
    df_sample.to_csv(filename)
    
    # Return the dataframe to the calling code
    return df_sample

def prep_zillow(df):
    '''
    This function; 
    - takes in a df
    - renames the columns
    - drops all null values
    - changes datatypes for appropriate columns
    - renames fips to actual county names.
    - returns a clean df
    '''
    df = df.rename(columns={'bedroomcnt': 'bedrooms',
                        'bathroomcnt': 'bathrooms',
                        'calculatedfinishedsquarefeet': 'area',
                        'taxamount': 'tax_amount',
                        'taxvaluedollarcnt': 'tax_value',
                        'yearbuilt': 'year_built',
                        'fips': 'county'})
    
    df = df.dropna()
    
    make_ints = ['bedrooms', 'area', 'tax_value', 'year_built']
    
    for col in make_ints:
        df[col] = df[col].astype(int)
        
    df.county = df.county.map({6037:'LA', 6059:'Orange', 6111:'Ventura'})
    
    return df

def prep_zillow_sample(df_sample):
    '''
    This function; 
    - takes in a df_sample
    - renames the columns
    - drops all null values
    - changes datatypes for appropriate columns
    - renames fips to actual county names.
    - returns a clean df
    '''
    df_sample = df_sample.rename(columns={'bedroomcnt': 'bedrooms',
                        'bathroomcnt': 'bathrooms',
                        'calculatedfinishedsquarefeet': 'area',
                        'taxamount': 'tax_amount',
                        'taxvaluedollarcnt': 'tax_value',
                        'yearbuilt': 'year_built',
                        'fips': 'county'})
    
    df_sample = df_sample.dropna()
    
    make_ints = ['bedrooms', 'area', 'tax_value', 'year_built']
    
    for col in make_ints:
        df_sample[col] = df_sample[col].astype(int)
        
    df_sample.county = df_sample.county.map({6037:'LA', 6059:'Orange', 6111:'Ventura'})
    
    return df_sample

def wrangle_zillow():
    '''
    This function;
    - uses the get_zillo_data() function to acquire the Zillow data
    - uses the prep_zillo(df) function to prepare the Zillow data
    - return the clean df
    '''
    df = prep_zillow(get_zillow_data())
    return df

def wrangle_zillow_sample():
    '''
    This function;
    - uses the get_zillow_sample_data() function to acquire the Zillow data
    - uses the prep_zillo(df) function to prepare the Zillow data
    - return the clean df
    '''
    df_sample = prep_zillow_sample(get_zillow_sample_data())
    return df_sample