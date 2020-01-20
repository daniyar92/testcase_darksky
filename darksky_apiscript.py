import argparse
from requests import get
import datetime, time
import sqlite3


darksky_api_key = 'bc43015099464582893306ea62d58e9f'
db_connect = sqlite3.connect('data.db')                 #just a testcase with few queries, so no need to use ORM
nowtime = int(time.time())

class ApiError(Exception):
    def __init__(self, status):
        self.status = status

    def __str__(self):
        return "APIError: status={}".format(self.status)

def write_to_db():
    pass

def clean_json(raw_jsn):
    pass

def query_to_api(lat, lon):
    url = 'https://api.darksky.net/forecast/' + darksky_api_key + '/' + str(lat) + ',' + str(lon) + ',' + str(nowtime) + '?'+ 'units=si'
    jsn = get(url)
    if jsn.status_code != 200:
        raise ApiError('GET /tasks/ {}'.format(jsn.status_code))
    else:
        result = jsn.json()
        return (result)

def get_weather_by_city_id(city_id):
    cur = db_connect.cursor()
    query = '''Select lat, lon from cities where name=?;'''
    cur.execute(query, (city_id,))
    res = cur.fetchall()
    print(res)
    return res
    pass

def export_to_csv():
    pass


if __name__ == '__main__':
    import pprint
    q = get_weather_by_city_id('Sydney')
    pprint.pprint(query_to_api(q[0][0], q[0][1]))
