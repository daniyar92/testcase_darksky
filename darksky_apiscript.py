import argparse
from requests import get
import datetime
import sqlite3
import logging
import prettytable
import csv

logging.name = logging.getLogger('darksky_apiscript')

darksky_api_key = 'bc43015099464582893306ea62d58e9f'
db_connect = sqlite3.connect('data.db')                 #just a testcase with few queries, so no need to use ORM
time_delta = datetime.timedelta(minutes=1)
nowtime = datetime.datetime.now()


class ApiError(Exception):
    def __init__(self, status):
        self.status = status

    def __str__(self):
        return "APIError: status={}".format(self.status)


def write_weather_to_db(data, city_id, time_tag=nowtime.timestamp()):
    '''
    insert data from api query to db
    :param data: api query result
    :param city_id: int id of required city
    :param time_tag: timestamp for minutely forecast
    :return:
    '''
    try:
        cur = db_connect.cursor()
        query = '''Insert into weather(time, summary, windSpeed, temperature, uvIndex, visibility, city_id) 
                    values (?,?,?,?,?,?,?);'''
        cur.execute(query,(time_tag, data['summary'], data['windSpeed'], data['temperature'], data['uvIndex'], data['visibility'], city_id,))
        res = cur.fetchall()
        db_connect.commit()
        logging.info(res)
    except Exception as e:
        logging.exception(e)


def check_interval(city_id):
    '''

    :param city_id: int id of required city
    :return:
    '''
    try:
        cur = db_connect.cursor()
        query = '''Select MAX(time) 
                    from weather 
                    where city_id=?'''
        cur.execute(query,(city_id,))
        res = cur.fetchall()
        return res[0][0]
    except Exception as e:
        logging.exception(e)


def query_to_api(lat, lon, time_tag=''):
    '''

    :param lat: lattitude
    :param lon: longtitude
    :param time_tag: timestamp for arg in api query
    :return: data from api query
    '''
    if len(str(time_tag)) > 0:
        url = 'https://api.darksky.net/forecast/' + darksky_api_key + '/' + str(lat) + ',' + str(lon) + ',' + str(time_tag) + '?'+ 'units=si'
    else:
        url = 'https://api.darksky.net/forecast/' + darksky_api_key + '/' + str(lat) + ',' + str(lon) + '?' + 'units=si'
    jsn = get(url)
    if jsn.status_code != 200:
        raise ApiError('GET /tasks/ {}'.format(jsn.status_code))
    else:
        result = jsn.json()
        return (result)


def get_weather_by_city_id(city_id, time_tag=0):            #input city name (city_id)
    '''

    :param city_id: int id of required city
    :param time_tag:
    :return: data from db for required city
    '''
    try:
        cur = db_connect.cursor()
        if time_tag == 0:
            query = '''Select MAX(weather.time), summary, windSpeed, temperature, uvIndex, visibility, weather.city_id 
                        from weather, cities 
                         where weather.city_id=?;'''
        else:
            query = '''Select weather.time, summary, windSpeed, temperature, uvIndex, visibility, weather.city_id 
                                    from weather, cities 
                                    where  weather.city_id=?
                                    order by weather.time desc limit 10;'''

        cur.execute(query, (city_id,))
        res = cur.fetchall()
        return res
    except Exception as e:
        logging.exception(e)


def get_all_cities():
    '''

    :return: all cities fields value
    '''
    try:
        cur = db_connect.cursor()
        query = '''Select * from cities;'''
        cur.execute(query)
        res = cur.fetchall()
        return res
    except Exception as e:
        logging.exception(e)


def get_10_mins_avg_weather(city_id):
    '''

    :param city_id: int id of required city
    :return: print min max avg temp
    '''
    res = []
    c = get_city_info_by_id(city_id)[0]
    x = prettytable.PrettyTable(
        ['City', 'min_temperature', 'max_temperature', 'avg_temperature'])
    if (int(nowtime.timestamp()) - int(check_interval(city_id)) >= 60):
        for i in range(10):
            q = query_to_api(c[2], c[3], int((nowtime-(i*time_delta)).timestamp()))
            # write_weather_to_db(q['currently'], c[0], (nowtime-(i*time_delta)).timestamp())
            res.append(float(q['currently']['temperature']))
    # res = get_weather_by_city_id(city_id, 1)
    values = find_min_max_avg(res)
    x.add_row([c[1],values[0], values[1], values[2]])
    print(x)


def find_min_max_avg(res_query):
    '''

    :param res_query: data for process
    :return: min max avg
    '''
    min_v = min(res_query)
    max_v = max(res_query)
    avg_v = sum(res_query)/len(res_query)
    return [min_v, max_v, avg_v]


def get_city_info_by_id (city_id):
    '''

    :param city_id: int id of required city
    :return: fields values
    '''
    try:
        cur = db_connect.cursor()
        query = '''Select * 
                    from cities 
                    where cities.city_id=?;'''
        cur.execute(query, (city_id,))
        res = cur.fetchall()
        return res
    except Exception as e:
        logging.exception(e)


def get_current_weather():
    '''
    without any parameters script shows current weather in all cities from table Cities
    :return:
    '''
    cities = get_all_cities()
    x = prettytable.PrettyTable()
    x.field_names = ['City', 'time', 'summary', 'windSpeed', 'temperature', 'uvIndex', 'visibility', 'city_id']
    for c in cities:
        if (check_interval(c[0]) is not None):
            if (int(nowtime.timestamp()) - int(check_interval(c[0])) >= 60):
                q = query_to_api(c[2], c[3])
                write_weather_to_db(q['currently'], c[0])
        else:
            q = query_to_api(str(c[2]), str(c[3]))
            write_weather_to_db(q['currently'], c[0])
        res = get_weather_by_city_id(c[0])
        x.add_row([c[1]]+[str(i) for i in res[0]])
    print(x)


def export_to_csv(filename):
    '''

    :param filename: output filename
    :return:
    '''
    try:
        cur = db_connect.cursor()
        query = '''Select cities.name AS cname, weather.time, summary, windSpeed, temperature, uvIndex, visibility, weather.city_id 
                    from weather, cities 
                    where cities.city_id=weather.city_id;'''
        cur.execute(query)
        res = cur.fetchall()

        with open(filename, 'w') as csv_f:
            csv_writer = csv.writer(csv_f, delimiter="\t")
            for r in res:
                csv_writer.writerow([i for i in r])
    except Exception as e:
        logging.exception(e)
    pass


def main(*args, **kwargs):
    parser = argparse.ArgumentParser()

    parser.add_argument('--fname')
    parser.add_argument('--city_id')

    args=parser.parse_args()
    if args.fname is not None:
        export_to_csv(args.fname)
    elif args.city_id is not None:
        get_10_mins_avg_weather(args.city_id)
    else:
        get_current_weather()


if __name__ == '__main__':
    main()
