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
    try:
        cur = db_connect.cursor()
        query = '''Insert into weather(time, summary, windSpeed, temperature, uvIndex, visibility, city_id) 
                    values (?,?,?,?,?,?,?);'''
        cur.execute(query,(time_tag, data['summary'], data['windSpeed'], data['temperature'], data['uvIndex'], data['visibility'], city_id,))
        res = cur.fetchall()
        logging.info(res)
    except Exception as e:
        logging.exception(e)


def check_interval(city_id):
    try:
        cur = db_connect.cursor()
        query = '''Select MAX(time) 
                    from weather 
                    where city_id=?'''
        cur.execute(query,(city_id,))
        res = cur.fetchall()
        logging.info(res)
    except Exception as e:
        logging.exception(e)


def query_to_api(lat, lon, time_tag=''):
    if len(time_tag) > 0:
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
    try:
        cur = db_connect.cursor()
        query = '''Select * from cities;'''
        cur.execute(query)
        res = cur.fetchall()
        return res
    except Exception as e:
        logging.exception(e)


def get_10_mins_avg_weather(city_id):

    c = get_city_info_by_id(city_id)
    x = prettytable.PrettyTable(
        ['City', 'min_temperature', 'max_temperature', 'avg_temperature'])
    x.set_field_align("City", "l")
    x.set_padding_width(1)
    if (int(nowtime.timestamp()) - int(check_interval(city_id)) >= 60):
        for i in range(10):
            q = query_to_api(c[2], c[3], (nowtime-(i*time_delta)).timestamp())
            write_weather_to_db(q['currently'], c[0], (nowtime-(i*time_delta)).timestamp())
    res = get_weather_by_city_id(city_id, 1)
    values = find_min_max_avg(res)
    x.add_row([c[1],values[0], values[1], values[2]])
    print(x)


def find_min_max_avg(res_query):
    min_v = min(res_query)[3]
    max_v = max(res_query)[3]
    avg_v = sum(res_query)[3]/len(res_query)
    return [min_v, max_v, avg_v]


def get_city_info_by_id (city_id):
    try:
        cur = db_connect.cursor()
        query = '''Select * 
                    from cities 
                    where cities.id=?;'''
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
        if (check_interval(c[0])):
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


if __name__ == '__main__':
    get_current_weather()

    # pprint.pprint(query_to_api(q[0][0], q[0][1]))
