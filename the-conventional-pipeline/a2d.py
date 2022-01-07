import requests
import sqlite3
import datetime
import time
import pandas as pd

def retrieve_data(location):
    """Request data from api

    Args:
        location (str/list): A string or list of location

    Returns:
        dict/list: A dictionary or list of response from api
    """
    api_key = '08bfdd439df244f4a53173531211812'

    if type(location) == str:
        loc = location
        api_url = f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={loc}' 
        response = requests.get(api_url).json()
        return response

    elif type(location) == list:
        responses = list()
        for l in range(len(location)):
            loc = location[l]
            api_url = f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={loc}'
            responses.append(requests.get(api_url).json())
        return responses  

def preprocessing_data(response):
    """Preprocessing raw data

    Args:
        response (dict): A dictionary of raw data

    Returns:
        dict: A dictionary of cleaned data
    """
    city = response.get('location').get('name')
    region = response.get('location').get('region')
    country = response.get('location').get('country')
    request_dt = response.get('location').get('localtime')
    lat = response.get('location').get('lat')
    lon = response.get('location').get('lon')
    last_update_dt = response.get('current').get('last_updated')
    temp = response.get('current').get('temp_c')
    feelslike = response.get('current').get('feelslike_c')
    windspeed = response.get('current').get('wind_kph')
    winddegree = response.get('current').get('wind_degree')
    pressure = response.get('current').get('pressure_mb')
    precip = response.get('current').get('precip_mm')
    humidity = response.get('current').get('humidity')
    cloud = response.get('current').get('cloud')
    vis = response.get('current').get('vis_km')
    uv = response.get('current').get('uv')
    gust = response.get('current').get('gust_kph')
    
    request_dt = datetime.datetime.strptime(request_dt, '%Y-%m-%d %H:%M')
    request_dt = request_dt + datetime.timedelta(seconds=0)
    
    last_update_dt = datetime.datetime.strptime(last_update_dt, '%Y-%m-%d %H:%M')
    last_update_dt = last_update_dt + datetime.timedelta(seconds=0)
    
    data = {'city': city,
            'request_dt': request_dt,
            'region': region,
            'country': country,
            'lat': lat,
            'lon': lon,
            'last_update_dt': last_update_dt,
            'temp': temp,
            'feelslike': feelslike,
            'windspeed': windspeed,
            'winddegree': winddegree,
            'pressure': pressure,
            'precip': precip,
            'humidity': humidity,
            'cloud': cloud,
            'vis': vis,
            'uv': uv,
            'gust': gust}

    return data
 
def preprocess_data(response):
    """Evaluate response type and call preprocessing data functions

    Args:
        response (dict/list): A dictionary of list of raw data

    Returns:
        dict/list: A dictionary or list of cleaned data
    """
    if type(response) == dict:
        data = preprocessing_data(response)
    elif type(response) == list:
        data = list()
        for l in range(len(response)):
            res = response[l]
            d = preprocessing_data(res)
            data.append(d)

    return data

def connection_(db_directory):
    """Create an empty database / Check database connection

    Args:
        db_directory (str): Path to database directory
    """
    connection = None
    try:
        connection = sqlite3.connect(db_directory, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        print(sqlite3.version)
    except sqlite3.Error as error:
        print(error)
    finally:
        if connection:
            connection.close()

def createt_(db_directory: str):
    """Create an empty table

    Args:
        db_directory (str): Path to database directory
    """    
    connection = sqlite3.connect(db_directory, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    with connection:
        connection.execute("""
            CREATE TABLE weather (
                unique_id INTEGER PRIMARY KEY AUTOINCREMENT, 
                city TEXT,
                request_dt DATETIME NOT NULL,
                region TEXT,
                country TEXT,
                lat FLOAT,
                lon FLOAT,
                last_update_dt DATETIME,
                temperature FLOAT,
                feelslike FLOAT,
                windspeed FLOAT,
                winddegree FLOAT,
                pressure FLOAT,
                precipitation FLOAT,
                humidity FLOAT,
                cloud FLOAT,
                vis FLOAT,
                uv FLOAT,
                gust FLOAT
            );
        """)
        
    connection.commit()   
    connection.close()
    print('Successfully created weather table')

def insertt_(db_directory: str, obj): 
    """Insert into table

    Args:
        db_directory (str): Path to database directory
        obj (dict/list): Values to be insert 
    """    
    try:
        connection = sqlite3.connect(db_directory, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        cursor = connection.cursor()
        # print('Successfully connected')

        if type(obj) == dict:
            insert_ = """ INSERT INTO weather (
            city,
            request_dt,
            region,
            country,
            lat,
            lon,
            last_update_dt,
            temperature,
            feelslike,
            windspeed,
            winddegree,
            pressure,
            precipitation,
            humidity,
            cloud,
            vis,
            uv,
            gust)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            
            data_tuple = tuple(obj.values())
            cursor.execute(insert_, data_tuple)
            cursor.close()
            connection.commit()
            # print('Successfully inserted')

        elif type(obj) == list:
            for i in range(len(obj)):
                insert_ = """ INSERT INTO weather (
                city,
                request_dt,
                region,
                country,
                lat,
                lon,
                last_update_dt,
                temperature,
                feelslike,
                windspeed,
                winddegree,
                pressure,
                precipitation,
                humidity,
                cloud,
                vis,
                uv,
                gust) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
                
                data_tuple = tuple(obj[i].values())
                cursor.execute(insert_, data_tuple)

            cursor.close()
            connection.commit()    
            # print('Successfully inserted')
        
    except sqlite3.Error as error:
        print('Error while working with SQLite', error)
    finally:
        if(connection):
            connection.close()
            # print('Connection closed')

def dropt_(db_directory: str, table_name: str):
    """Drop a table from database

    Args:
        db_directory (str): Path to database directory
        table_name (str): Correspond table name
    """
    connection = sqlite3.connect(db_directory)
    cursor = connection.cursor()
    cursor.execute("DROP TABLE {}".format(table_name))
    connection.commit()
    connection.close()
    print('Successfully drop table')

def queryt_(db_directory: str, table_name: str):
    """Query a table from database

    Args:
        db_directory (str): Path to database directory
        table_name (str): Correspond table name

    Returns:
        pandas.DataFrame: A pandas dataframe of records
    """    
    try:
        connection = sqlite3.connect(db_directory, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        cursor = connection.cursor()
        # print('Successfully connected')
        
        query_ = """SELECT * from {}""".format(table_name)
        cursor.execute(query_)
        records = cursor.fetchall()
        cursor.close()
        records = pd.DataFrame(records, columns=[c[0] for c in cursor.description])
        records['request_dt'] = pd.to_datetime(records['request_dt'])
        records['last_update_dt'] = pd.to_datetime(records['last_update_dt'])
        
        return records  

    except sqlite3.Error as error:
        print('Error while working with SQLite', error)
    finally:
        if(connection):
            connection.close()
            # print('Connection closed')

def main():
    """Retrieve data from api and insert into database
    """
    DATABASE_NAME = 'thedb.db'
    DB_DIRECTORY = r'c:\Users\User\Desktop\Archive\the-conventional-pipeline\{}'.format(DATABASE_NAME)
    TIME_INTERVAL = float(60.0)
    CITIES = ['Kuala Lumpur',
    'Teluk Intan',
    'Kota Kinabalu',
    'Melaka',
    'George Town',
    'Johor Bahru',
    'Seremban',
    'Alor Setar',
    'Kuantan',
    'Ipoh',
    'Kangar',
    'Kuching',
    'Kuala Terengganu']

    connection_(DB_DIRECTORY)
    createt_(DB_DIRECTORY)

    # data = queryt_(db_directory=DB_DIRECTORY, table_name='weather')
    # print(data)

    starttime = time.time()
    try:
        while True:
            response = retrieve_data(location=CITIES)
            data = preprocess_data(response)
            insertt_(DB_DIRECTORY, data)
            print('Last Retrieve: {}'.format(datetime.datetime.now()))
            time.sleep(TIME_INTERVAL- ((time.time() - starttime) % TIME_INTERVAL))

    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()
    

