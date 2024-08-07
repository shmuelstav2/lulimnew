# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import pandas as pd
import openpyxl
from sqlalchemy import text
import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine
import urllib
import pandas as pd
import os
import pandas as pd
import numpy as np
#import schedule
import pandas as pd
#import xlsxwriter
import time
import pandas as pd
import pymongo
from pymongo import MongoClient
from pymongo import MongoClient
from datetime import datetime, timedelta

def read_config(filename='config.txt'):
    script_path = os.path.abspath(__file__)

    # Construct the absolute path to config.txt
    config_path = os.path.join(os.path.dirname(script_path), filename)
    config = {}
    try:
        with open(filename, 'r') as file:
            for line in file:
                key, value = line.strip().split('=')
                config[key.strip()] = value.strip().strip("'\"")  # Remove quotes
    except Exception as e:
        print(f"Error reading configuration file: {e}")

    return config

# Get configuration values
config = read_config()

# Access individual parameters
excel_prod = config.get('excel_prod', '')
excel_test = config.get('excel_test', '')
environment = config.get('environment', '')
days_ago = 7
conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=lulimdbserver.database.windows.net;'
    'DATABASE=lulimdb;'
    'UID=shmuelstav;'
    'PWD=5tgbgt5@'
)

# URL encoding for connection string
params = urllib.parse.quote_plus(conn_str)
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')


# Print or use the values as needed
print("Excel Prod:", excel_prod)
print("Excel Test:", excel_test)
print("Environment:", environment)

farms = '\\fandy farms'
reports = '\\current active flocks\\'
sheet_name_tmuta = 'tmuta'
sheet_name_sivuk = 'shivuk'
sheet_name_skila = 'סיכום שקילות'
excel_file_name_finish = 'current flock '
excel_end = '.xlsx'''
excel_middle_name = '\\current flock\\'


#Functions
def read_excel(path, sheet_name):

    try:
        #df = pd.read_excel(path, sheet_name=sheet_name)
        #return df
        workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)

        # Check if the specified sheet exists
        if sheet_name not in workbook.sheetnames:
            print(f"Sheet '{sheet_name}' not found.")
            return None

        # Access the specified sheet even if it's hidden
        sheet = workbook[sheet_name]

        # Read the sheet data into a DataFrame
        data = []
        for row in sheet.iter_rows(values_only=True):
            data.append(row)

        df = pd.DataFrame(data, columns=data[0])

        # Close the workbook
        workbook.close()

        return df

    except Exception as e:
        # Handle any exceptions (e.g., file not found, sheet not found) and return an empty DataFrame
        print(f"An error occurred: {str(e)}")
        return pd.DataFrame()

def subfolder_names(path):
    folder_names = []

    # List all directories and files in the given path
    entries = os.listdir(path)

    for entry in entries:
        entry_path = os.path.join(path, entry)

        # Check if the entry is a directory (subfolder)
        if os.path.isdir(entry_path):
            folder_names.append(entry)

    return folder_names


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

def find_hathala(farm, day,df):
    df = df[df['farm name'] == farm]
    df = df.groupby('house number').agg({'mixed start quantity': 'max'}).reset_index()
    column_sum = df['mixed start quantity'].sum()
    return column_sum


def find_tmuta_iomit(farm, day,df):
    df = df[df['farm name'] == farm]
    df = df[df['growth day'] == day]
    column_sum = df['mixed daily mortality'].sum()
    return column_sum
def write_to_mongo_and_delete (df,db_name,collection_name):
    # MongoDB connection settings
    mongo_uri = "mongodb://localhost:27017/"
    database_name = db_name
    collection_name = collection_name

    # Convert DataFrame to a list of dictionaries
    data_list = df.to_dict(orient='records')

    # Convert datetime.date objects to datetime.datetime objects

    for record in data_list:
        for key, value in record.items():
            if isinstance(value, date):
                record[key] = value.strftime('%Y-%m-%d')
    # Connect to MongoDB
    client = MongoClient(mongo_uri)

    # Access the lulim database (it will be created if it doesn't exist)
    db = client[database_name]

    # Access the tmuta_end collection
    collection = db[collection_name]

    # Delete existing data in the collection
    collection.delete_many({})

    # Insert new data into the collection
    collection.insert_many(data_list)

    # Close the MongoDB connection
    client.close()


def translate(farm):
    translations = {
        'gotliv 4': 'גוטליב 4',
        'gotliv 2': 'גוטליב 2',
        'megadim': 'מגדים',
        'megido': 'מגידו',
        'ranen': 'רנן',
        'shaal morad': 'שעל מורד',
        'kalanit': 'כלנית',
        'ramat zvi haim': 'רמת צבי חיים',
        'ramat zvi moshe': 'רמת צבי משה',
        'ramot naftali': 'רמות נפתלי',
        'ranen': 'רנן',
        'shaal moyal': 'שעל מויאל',
        'musayel': 'מוסייל',
        'sigler': 'סיגלר',
        'gazit': 'גזית',
        'sadmot dvora': 'שדמות דבורה',
        'mawiya': 'מעוואיה',
        'sharona': 'שרונה'
        # Add more translations as needed
    }

    # Check if farm exists in translations dictionary
    if farm in translations:
        return translations[farm]
    else:
        print(f"Translation not found for '{farm}'")
        return f"Translation not found for '{farm}'"


def udate_skila():
    farms_names = subfolder_names(excel_prod + farms)
    tmuta_results = pd.DataFrame()
    for farm in farms_names:
        path = f"{excel_prod}{farms}\\{farm}{excel_middle_name}{excel_file_name_finish}{farm}{excel_end}"
        data = read_excel(path, sheet_name_skila)

        if not data.empty:
            data = data.replace('', np.nan)

            # Drop rows and columns where all elements are NaN
            df_cleaned = data.dropna(how='all').dropna(axis=1, how='all')

            columns_to_keep = [0, 1, 6, 7]
            df_cleaned = df_cleaned.iloc[:, columns_to_keep].dropna()

            new_columns = df_cleaned.iloc[0]
            df_cleaned = df_cleaned[1:]
            df_cleaned.columns = new_columns
            df_cleaned = df_cleaned.reset_index(drop=True)

            # Create new DataFrame for SQL
            df = pd.DataFrame()
            df['grotwh_day'] = pd.to_numeric(df_cleaned['יום'])
            df['avg_mixed'] = pd.to_numeric(df_cleaned['ממוצע מעורב'])
            df['avg_mixed_percent'] = pd.to_numeric(df_cleaned['אחוז תקן מעורב'])
            df['mivne'] = pd.to_numeric(df_cleaned['מבנה'])
            df['midgar'] = 1
            df['farm_name'] = str(translate(farm))
            df['avg_mixed'] = df['avg_mixed'].round(3)
            df['avg_mixed_percent'] = df['avg_mixed_percent'].round(3)

            # Load existing data and normalize
            existing_data = pd.read_sql('SELECT grotwh_day, mivne, midgar, farm_name FROM skila_svuit', con=engine)
            existing_data = existing_data.astype(
                {'grotwh_day': 'int64', 'mivne': 'int64', 'midgar': 'int64', 'farm_name': 'str'})
            existing_data.set_index(['grotwh_day', 'mivne', 'midgar', 'farm_name'], inplace=True)

            df = df.astype({'grotwh_day': 'int64', 'mivne': 'int64', 'midgar': 'int64', 'farm_name': 'str'})
            df.set_index(['grotwh_day', 'mivne', 'midgar', 'farm_name'], inplace=True)

            # Identify new rows
            new_rows = df[~df.index.isin(existing_data.index)].reset_index()

            # Upsert logic: Insert new rows and update existing ones
            if not new_rows.empty:
                with engine.begin() as connection:
                    for _, row in new_rows.iterrows():
                        stmt = text("""
                        MERGE INTO skila_svuit AS target
                        USING (SELECT :grotwh_day AS grotwh_day, :mivne AS mivne, :midgar AS midgar, :farm_name AS farm_name) AS source
                        ON target.grotwh_day = source.grotwh_day AND target.mivne = source.mivne AND target.midgar = source.midgar AND target.farm_name = source.farm_name
                        WHEN MATCHED THEN
                            UPDATE SET avg_mixed = :avg_mixed, avg_mixed_percent = :avg_mixed_percent
                        WHEN NOT MATCHED THEN
                            INSERT (grotwh_day, mivne, midgar, farm_name, avg_mixed, avg_mixed_percent)
                            VALUES (:grotwh_day, :mivne, :midgar, :farm_name, :avg_mixed, :avg_mixed_percent);
                        """)
                        params = {
                            'grotwh_day': row['grotwh_day'],
                            'mivne': row['mivne'],
                            'midgar': row['midgar'],
                            'farm_name': row['farm_name'],
                            'avg_mixed': row['avg_mixed'],
                            'avg_mixed_percent': row['avg_mixed_percent']
                        }
                        connection.execute(stmt, params)  # Use params dictionary
                print(f"Upserted rows successfully for {farm}.")
            else:
                print(f"No new rows to upsert for {farm}.")

    # Fetch data and write to MongoDB
    df_view = pd.read_sql("SELECT * FROM dbo.skila_svuit_highest_grotwh_day;", con=engine)
    write_to_mongo_and_delete(df_view, 'lulim_new', 'skila')
    print('בכרבר')

def udate_tmuta():
    #run over all the farm files
    farms_names = subfolder_names(excel_prod + farms)
    for farm in farms_names:
        path = f"{excel_prod}{farms}\\{farm}{excel_middle_name}{excel_file_name_finish}{farm}{excel_end}"
        data = read_excel(path, sheet_name_tmuta)

        if not data.empty:
            data = data.replace('', np.nan)

            # Drop rows and columns where all elements are NaN
            df_cleaned = data.dropna(how='all').dropna(axis=1, how='all')
            if not df_cleaned.empty:
                threshold = 5
                # Delete columns with fewer non-null values than the threshold
                df_cleaned = df_cleaned.dropna(axis=1, thresh=threshold)
                df_cleaned = df_cleaned.dropna(axis=0, thresh=threshold)
                df_cleaned.columns = df_cleaned.iloc[0]
                df_cleaned = df_cleaned.iloc[1:]
                df_cleaned = df_cleaned.reset_index(drop=True)
                df_cleaned = df_cleaned[df_cleaned['mixed daily mortality'] > 0]

            if not df_cleaned.empty:
                # Create new DataFrame for SQL
                df = pd.DataFrame()
                df_cleaned['site'] = df_cleaned['site'].astype(str)
                df_cleaned['site'] = df_cleaned['site'].str.split('.').str[0]
                df_cleaned['site'] = pd.to_numeric(df_cleaned['site'])
                df_cleaned['site'] = pd.to_numeric(df_cleaned['site'])
                df_cleaned['house_number'] = pd.to_numeric(df_cleaned['house number'])
                df_cleaned['parent_flock'] = pd.to_numeric(df_cleaned['perent flock'])
                df_cleaned['mixed_start'] = pd.to_numeric(df_cleaned['mixed start quantity'])
                df_cleaned['daily_mortality'] = pd.to_numeric(df_cleaned['daily mortality'])
                df_cleaned['growth day'] = pd.to_numeric(df_cleaned['growth day'])
                # Convert columns to string
                df_cleaned['farm_name'] = df_cleaned['farm name'].astype(str)
                df_cleaned['hatchery'] = df_cleaned['hatchery'].astype(str)
                df_cleaned['line'] = df_cleaned['line'].astype(str)

                try: # Convert 'date' to datetime
                    df_cleaned['date'] = pd.to_datetime(df_cleaned['date'])
                except:
                    print("error "+farm)

                # Read existing data from the database
                # Read existing data from the database
                print('now '+farm)
                existing_data = pd.read_sql('SELECT grotwh_day, mivne, midgar, farm_name FROM skila_svuit', con=engine)
                existing_data = existing_data.astype(
                    {'grotwh_day': 'int64', 'mivne': 'int64', 'midgar': 'int64', 'farm_name': 'str'})
                existing_data.set_index(['grotwh_day', 'mivne', 'midgar', 'farm_name'], inplace=True)

                # Ensure df_cleaned has required columns for further processing
                required_columns_df = ['growth day', 'site', 'house_number', 'farm_name', 'hatchery', 'line', 'date',
                                       'mixed_start', 'daily_mortality']
                missing_columns_df = [col for col in required_columns_df if col not in df_cleaned.columns]

                if missing_columns_df:
                    print(f"Missing columns in df_cleaned for further processing: {missing_columns_df}")
                else:
                    # Create a new DataFrame df based on df_cleaned with required columns
                    df = df_cleaned[
                        ['growth day', 'site', 'house_number', 'farm_name', 'hatchery', 'line', 'date', 'mixed_start',
                         'daily_mortality']]

                    # Convert new DataFrame columns to appropriate types
                    df = df.astype(
                        {'growth day': 'int64', 'site': 'int64', 'house_number': 'int64', 'farm_name': 'str'})
                    df.set_index(['growth day', 'site', 'house_number', 'farm_name'], inplace=True)

                    # Identify new rows
                    new_rows = df[~df.index.isin(existing_data.index)].reset_index()

                    # Upsert logic: Insert new rows and update existing ones
                    if not new_rows.empty:
                        with engine.begin() as connection:
                            for _, row in new_rows.iterrows():
                                try:
                                    stmt = text("""
                                            MERGE INTO tmuta AS target
                                            USING (SELECT :growth_day AS growth_day, 
                                                          :site AS site, 
                                                          :house_number AS house_number, 
                                                          :parent_flock AS parent_flock, 
                                                          :farm_name AS farm_name, 
                                                          :hatchery AS hatchery, 
                                                          :line AS line, 
                                                          :date AS date, 
                                                          :mixed_start_quantity AS mixed_start_quantity, 
                                                          :daily_mortality AS daily_mortality) AS source
                                            ON target.growth_day = source.growth_day 
                                               AND target.site = source.site 
                                               AND target.house_number = source.house_number 
                                               AND target.parent_flock = source.parent_flock 
                                               AND target.farm_name = source.farm_name 
                                               AND target.hatchery = source.hatchery 
                                               AND target.line = source.line 
                                               AND target.date = source.date
                                            WHEN MATCHED THEN
                                                UPDATE SET target.mixed_start_quantity = source.mixed_start_quantity, 
                                                           target.daily_mortality = source.daily_mortality
                                            WHEN NOT MATCHED BY TARGET THEN
                                                INSERT (growth_day, site, house_number, parent_flock, farm_name, hatchery, line, date, mixed_start_quantity, daily_mortality)
                                                VALUES (:growth_day, :site, :house_number, :parent_flock, :farm_name, :hatchery, :line, :date, :mixed_start_quantity, :daily_mortality);
                                        """)

                                    params = {
                                        'growth_day': row['growth day'],
                                        'site': row['site'],
                                        'house_number': row['house_number'],
                                        'parent_flock': 0,
                                        'farm_name': row['farm_name'],
                                        'hatchery': row['hatchery'],
                                        'line': row['line'],
                                        'date': row['date'],
                                        'mixed_start_quantity': row['mixed_start'],
                                        'daily_mortality': row['daily_mortality']
                                    }

                                    connection.execute(stmt, params)
                                    print("success"+farm)
                                except Exception as e:
                                    print(f"An error occurred: {e}")
                        print("Upserted rows successfully.")
                    else:
                        print("No new rows to upsert.")

                        # Fetch data and write to MongoDB
                    df_view = pd.read_sql("SELECT * FROM dbo.skila_svuit_highest_grotwh_day;", con=engine)
                    df_view2 = pd.read_sql("SELECT [שם חווה],[כמות התחלתית],[תמותה כוללת],[יום גידול],[אחוז תמותה כולל],[יום עדכון אחרון] FROM tmuta14 ORDER BY [data_taken_date] desc;", con=engine)

                    # Assuming write_to_mongo_and_delete is a defined function
                    write_to_mongo_and_delete(df_view, 'lulim_new', 'tmuta')
                    write_to_mongo_and_delete(df_view2, 'lulim_new', 'tmuta14')




def udate_sivuk():
    farms_names = subfolder_names(excel_prod + farms)
    sivuk_results = pd.DataFrame()
    for farm in farms_names:
        # check if excel file has changed
        path = excel_prod + farms + '\\' + farm + excel_middle_name + excel_file_name_finish + farm + excel_end
        data = read_excel(path, sheet_name_sivuk)
        if not data.empty:
            threshold = 5
            # Delete columns with fewer non-null values than the threshold
            data = data.dropna(axis=1, thresh=threshold)
            data = data.dropna(axis=0, thresh=threshold)
            data.columns = data.iloc[0]
            data = data.iloc[1:]
            data = data.reset_index(drop=True)
            data['neto weight '] = pd.to_numeric(data['neto weight '], errors='coerce')
            data = data[data['neto weight '] > 0]
            sivuk_results = pd.concat([sivuk_results, data], ignore_index=True)

    # Convert the 'marketing date' column to a datetime format
    sivuk_results['marketing date'] = pd.to_datetime(sivuk_results['marketing date'], format='%d.%m.%y')

    # Format the datetime objects without leading zeros
    sivuk_results['marketing date'] = sivuk_results['marketing date'].dt.strftime('%Y.%m.%d')








def update_data():

    farms_names = subfolder_names(excel_prod + farms)
    tmuta_results = pd.DataFrame()
    for farm in farms_names:
        # check if excel file has changed
        path = excel_prod + farms + '\\' + farm + excel_middle_name + excel_file_name_finish + farm + excel_end
        data = read_excel(path, sheet_name_tmuta)
        if not data.empty:
            threshold = 5
            # Delete columns with fewer non-null values than the threshold
            data = data.dropna(axis=1, thresh=threshold)
            data = data.dropna(axis=0, thresh=threshold)
            data.columns = data.iloc[0]
            data = data.iloc[1:]
            data = data.reset_index(drop=True)
            data = data[data['mixed daily mortality'] > 0]

            client = MongoClient('mongodb://localhost:27017/')
            db = client['lulim_new']
            collection = db['tmuta']

            # Iterate over DataFrame rows
            for index, row in data.iterrows():
                # Check if the row already exists in the collection
                existing_row = collection.find_one(row.to_dict())
                if existing_row is None:
                    # Insert the row into the collection
                    collection.insert_one(row.to_dict())
                    print("Inserted row:", row.to_dict())
                else:
                    print("Row already exists:", row.to_dict())

            print("Data insertion completed.")

        path = excel_prod + farms + '\\' + farm + excel_middle_name + excel_file_name_finish + farm + excel_end
        data = read_excel(path, sheet_name_sivuk)
        if not data.empty:
            threshold = 5
            # Delete columns with fewer non-null values than the threshold
            data = data.dropna(axis=1, thresh=threshold)
            data = data.dropna(axis=0, thresh=threshold)
            data.columns = data.iloc[0]
            data = data.iloc[1:]
            data = data.reset_index(drop=True)
            data['neto weight '] = pd.to_numeric(data['neto weight '], errors='coerce')
            data = data[data['neto weight '] > 0]
            client = MongoClient('mongodb://localhost:27017/')
            db = client['lulim_new']
            collection = db['sivuk']

            # Iterate over DataFrame rows
            for index, row in data.iterrows():
                # Check if the row already exists in the collection
                existing_row = collection.find_one(row.to_dict())
                if existing_row is None:
                    # Insert the row into the collection
                    collection.insert_one(row.to_dict())
                    print("Inserted row:", row.to_dict())
                else:
                    print("Row already exists:", row.to_dict())

            print("Data insertion completed.")


def update_results():

    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['lulim_new']  # Replace 'your_database' with your actual database name
    collection = db['tmuta']  # Replace 'your_collection' with your actual collection name

    # Convert MongoDB collection to DataFrame
    cursor = collection.find()

    df = pd.DataFrame(list(cursor))
    #df['date'] = pd.to_datetime(df['date'])

    # Check for missing values
    #print(df.isnull().sum())

    # If there are missing values, handle or remove them as needed

    # Perform the groupby operation after ensuring data type compatibility

    df = df[df['date'] != '#VALUE!']
    # Display the rows with invalid dates


    latest_dates = df.groupby('farm name')['date'].max().reset_index()
    # Group by farm name and find the latest date for each farm
    latest_dates = df.groupby('farm name')['date'].max().reset_index()

    filtered_df = pd.merge(df, latest_dates[['farm name', 'date']], on=['farm name', 'date'])

    filtered_df_date = df.groupby('farm name')['growth day'].max().reset_index()

    filtered_df_date =pd.merge(filtered_df_date, latest_dates[['farm name','date']], on=['farm name'])

    filtered_df_date ['begin_date'] = pd.to_datetime(filtered_df['date']) - pd.to_timedelta(filtered_df['growth day'],unit='d')

    filtered_df_date.set_index('farm name', inplace=True)

    # Convert filtered_df to a dictionary
    filtered_dict = filtered_df_date.to_dict(orient='index')

    #df['begin_date'] = ''
    df['begin_date'] = df['farm name'].map(lambda x: filtered_dict.get(x, pd.NaT))
    df['begin_date'] = df['begin_date'].apply(lambda x: x['begin_date'] if isinstance(x, dict) else x)
    df['date'] = pd.to_datetime(df['date'])
    df['begin_date'] = pd.to_datetime(df['begin_date'])

    # Filter the DataFrame where 'date' is greater than or equal to 'begin_date'
    df = df[df['date'] >= df['begin_date']]

    # Iterate over the dictionary and populate the 'begin_date' column in filtered_df
    for farm_name, begin_date in filtered_dict.items():
        df.loc[df['farm name'] == farm_name, 'begin_date'] = begin_date

    result_aggregete = df.groupby('farm name').agg(
        {'mixed daily mortality': 'sum', 'growth day': 'max', 'date': 'max'}). \
        reset_index()
    result_aggregete['hacnasa'] = result_aggregete.apply(
        lambda row: find_hathala(row['farm name'], row['growth day'], df), axis=1)
    result_aggregete['notru_lesivuk'] = result_aggregete['hacnasa'] - result_aggregete['mixed daily mortality']
    result_aggregete['tmuta_iomit'] = result_aggregete.apply(
        lambda row: find_tmuta_iomit(row['farm name'], row['growth day'], df), axis=1)
    write_to_mongo_and_delete(result_aggregete,'lulim_new','tmuta_end')

    client = MongoClient('mongodb://localhost:27017/')
    db = client['lulim_new']  # Replace 'your_database' with your actual database name
    collection = db['sivuk']  # Replace 'your_collection' with your actual collection name

    # Convert MongoDB collection to DataFrame
    cursor = collection.find()

    df = pd.DataFrame(list(cursor))

    # Group by farm name and find the latest date for each farm
    try:
     invalid_date_rows = df[df['marketing date'] == '#VALUE!']
     # Display the rows with invalid dates
     print(invalid_date_rows)
    except KeyError as e:
        print(f"KeyError: {e}")

    latest_dates = df.groupby('farm name')['marketing date'].max().reset_index()

    filtered_df = pd.merge(df, latest_dates[['farm name', 'marketing date']], on=['farm name', 'marketing date'])

    filtered_df_date = df.groupby('farm name')['marketing date'].max().reset_index()

    filtered_df_date = pd.merge(filtered_df_date, latest_dates[['farm name', 'marketing date']], on=['farm name'])

    filtered_df_date['begin_date'] = pd.to_datetime(filtered_df['date']) - pd.to_timedelta(filtered_df['growth day'],
                                                                                           unit='d')

    filtered_df_date.set_index('farm name', inplace=True)

    # Convert filtered_df to a dictionary
    filtered_dict = filtered_df_date.to_dict(orient='index')

    # df['begin_date'] = ''
    df['begin_date'] = df['farm name'].map(lambda x: filtered_dict.get(x, pd.NaT))
    df['begin_date'] = df['begin_date'].apply(lambda x: x['begin_date'] if isinstance(x, dict) else x)
    df['date'] = pd.to_datetime(df['date'])
    df['begin_date'] = pd.to_datetime(df['begin_date'])

    # Filter the DataFrame where 'date' is greater than or equal to 'begin_date'
    df = df[df['date'] >= df['begin_date']]

    # Iterate over the dictionary and populate the 'begin_date' column in filtered_df
    for farm_name, begin_date in filtered_dict.items():
        df.loc[df['farm name'] == farm_name, 'begin_date'] = begin_date

        # Convert the 'marketing date' column to a datetime format
    sivuk_results = df
    sivuk_results['marketing date'] = pd.to_datetime(sivuk_results['marketing date'], format='%d.%m.%y')

    # Format the datetime objects without leading zeros
    sivuk_results['marketing date'] = sivuk_results['marketing date'].dt.strftime('%Y.%m.%d')

    # Create a date object three days ago
    three_days_ago = pd.to_datetime('today') - pd.to_timedelta(days_ago, unit='D')

    # Filter based on the condition
    agg_sivuk_results = sivuk_results[pd.to_datetime(sivuk_results['marketing date']) >= three_days_ago]

    columns_to_drop = ['site', 'flock']
    agg_sivuk_results.drop(columns=columns_to_drop, inplace=True)
    agg_sivuk_results['averrage weight '] = agg_sivuk_results['averrage weight '].fillna(0).round(2)
    agg_sivuk_small = agg_sivuk_results[
        ['farm name', 'marketing date', 'house', 'receipt', 'destination', 'marketed quantity', 'averrage weight ',
         'marketed age']]
    write_to_mongo_and_delete(agg_sivuk_small, 'lulim_new', 'sivuk_end')


# function that calculates diff between 2 dates


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    udate_skila()
    udate_tmuta()
    #update_sivuk()
    #update_data()
    #update_results()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
