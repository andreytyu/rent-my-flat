import os
import requests
import pandas as pd

url = 'https://op.mos.ru/EHDWSREST/catalog/export/get?id='
ids = [785561, 785489, 785513, 785597, 785477, 784213]
temp_file = 'temp.zip'

def download_url(url, save_path=temp_file, chunk_size=128):
    '''
    gets zipped csv from url to local file
    '''
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

def read_data(filepath=temp_file):
    df = pd.read_csv(temp_file, sep=';', encoding='cp1251')
    df = df[df.global_id != 'global_id']  # fix headers in data
    return df

### calendar.txt 
download_url(url+str(ids[0]))
df = read_data()
df[['service_id', 'monday', 'tuesday', 'wednesday', 'thursday',
   'friday', 'saturday', 'sunday', 'start_date',
   'end_date']].to_csv('data/gtfs/calendar.txt', sep=',', index=False)

### routes.txt
download_url(url+str(ids[1]))
df = read_data()
df = df[['route_id', 'agency_code', 'route_short_name', 'route_long_name', 'route_type', 'route_desc']]
df = df.rename(columns = {'agency_code':'agency_id'})  # change column name to match gtfs format
df.route_type = df.route_type.replace('5', '11')  # fix trolleybus data type
df.to_csv('data/gtfs/routes.txt', sep=',', index=False)

### stops.txt
download_url(url+str(ids[2]))
df = read_data()
df['location_type'] = 0  # all stops have type zero
coords = [x.split('[')[1][:-2].split(', ') for x in df.geoData]  # extract coordinates
df['stop_lon'] = [float(x[0]) for x in coords]
df['stop_lat'] = [float(x[1]) for x in coords]
df = df[['stop_id', 'stop_name', 'stop_lon', 'stop_lat', 'location_type']]
df.to_csv('data/gtfs/stops.txt', sep=',', index=False)

### stop_times.txt
download_url(url+str(ids[3]))
df = read_data()
df[['trip_id', 'arrival_time',
  'departure_time', 'stop_id', 'stop_sequence',
   'stop_headsign', 'pickup_type']].to_csv('data/gtfs/stop_times.txt', sep=',', index=False)

### trips.txt
download_url(url+str(ids[4]), temp_file)
df = read_data()
df[['route_id', 'trip_id', 'service_id',
   'trip_headsign', 'block_id']].to_csv('data/gtfs/trips.txt', sep=',', index=False)

### agency.txt
download_url(url+str(ids[5]), temp_file)
df = read_data()
df = df.rename(columns = {'agency_code':'agency_id'})  # change column name to match gtfs format
df[['agency_id', 'agency_name',
   'agency_url', 'agency_timezone']].to_csv('data/gtfs/agency.txt', sep=',', index=False)

os.remove(temp_file)
