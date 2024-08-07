from tenacity import *
import logging
import psycopg2
import os
import pandas as pd
import datetime
import pytz

logging.getLogger().setLevel(logging.INFO)
host = os.getenv('host')
pw = os.getenv('pw')
db = os.getenv('db')
user = os.getenv('user')
port = os.getenv('port')
table = 'controlroom.tag_data'

@retry(stop=(stop_after_delay(10) | stop_after_attempt(5)), wait=wait_fixed(1))
def open_connection(host, db, port, user,pw):
    #logging.info("tying to open DB connection"+ str(host)+str(db)+str(port)+str(user)+str(pw))
    conn = psycopg2.connect(
        dbname=db,
        user=user,
        host=host,
        password=pw,
        port=port,
        connect_timeout=3,
        keepalives=1,
        keepalives_idle=5,
        keepalives_interval=2,
        keepalives_count=2)
    return conn

# def derived_tags_is_cleaning_bool(input_data):
#     input_data_a = next((item['InputValue'] for item in input_data if item['InputColumn'] == 'RX6:MODE_1.II0001'), None)
#     input_data_b = next((item['InputValue'] for item in input_data if item['InputColumn'] == 'RX6_A:FQI4614.OUT'), None)
#     input_data_c = next((item['InputValue'] for item in input_data if item['InputColumn'] == 'RX6_A:TIC4600.MEAS'), None)

#     calculated_value = int(input_data_a == 0 and input_data_b > 500 and input_data_c > 250) if input_data_a is not None and input_data_b is not None and input_data_c is not None else 0
#     quality = int(any(d.get('Quality', 0) == 0 for d in input_data))
#     tag_char_value = None
#     collection_time = max(input_data, key=lambda x: x['CollectionTime'])['CollectionTime']
#     return calculated_value, tag_char_value, quality, collection_time

# def derived_tags_is_batch_bool(input_data):
#     logging.info("inside function")
#     logging.info(input_data)
#     input_data_a = input_data[0]['InputValue']
#     logging.info("input_a"+str(input_data_a))
#     quality = int(input_data[0]['Quality'])
#     logging.info("quality"+str(quality))
#     calculated_value = int(input_data_a >= 4) if input_data_a is not None else 0
#     logging.info("calculated_value"+str(calculated_value))
#     quality = int(input_data[0]['Quality'])
#     logging.info("quality"+str(quality))
#     tag_char_value = None
#     collection_time = input_data[0]['CollectionTime']
#     logging.info(str(collection_time))
#     return calculated_value, tag_char_value, quality, collection_time

def midnight_temperature_delta(input_data, timez):
    logging.info("inside throat_temp")
    dbconn = open_connection(host,db,port,user,pw)
    logging.info("conn done")
    # cursor = dbconn.cursor()
    logging.info(input_data)
    input_data = input_data[0]
    current_value = input_data['InputValue']
    tag = input_data['InputColumn']
    timestamp = input_data['TagTime']
    time = pd.to_datetime(timestamp)
    logging.info("timestamp")
    logging.info(timestamp)
    logging.info(time)
    if timez == 'est':
        est_timezone = pytz.timezone('America/New_York')
    elif timez == 'cst':
        est_timezone = pytz.timezone('America/Chicago')
    logging.info(est_timezone)
    current_date_est = time.astimezone(est_timezone).date()
    logging.info(current_date_est)
    # Create midnight EST timestamp
    midnight_est = est_timezone.localize(datetime.datetime.combine(current_date_est, datetime.datetime.min.time()))
    logging.info(midnight_est)
    # Convert midnight EST timestamp to UTC
    midnight_utc = midnight_est.astimezone(pytz.utc)
    logging.info(midnight_utc)
    formatted_midnight_utc = midnight_utc.strftime("%Y-%m-%d %H:%M")
    logging.info(formatted_midnight_utc)
    if time == midnight_utc:
        logging.info("midnight")
        calculated_value = 0
    else:
        utc = str(formatted_midnight_utc)
        logging.info("not midnight")
        cursor3 = dbconn.cursor()
        query = f"SELECT MAX(tag_value) FROM {table} WHERE tag_time > \'{utc}\'"
        query += f"AND tag_time < date_trunc('minute', \'{utc}\'::timestamptz) + INTERVAL '1 minute'"
        query += f"AND tag_name = '{tag}';"
        logging.info(query)
        cursor3.execute(query)
        logging.info("midnight query exec done")
        rows3 = cursor3.fetchall()
        logging.info(rows3)
        logging.info(len(rows3))
        if len(rows3) == 0:
            cursor3 = dbconn.cursor()
            query = f"SELECT tag_value FROM {table} WHERE tag_name = '{tag}' "
            query += f"ORDER BY tag_time DESC LIMIT 1;" 
            cursor3.execute(query)
            rows3 = cursor3.fetchall()
            logging.info(rows3)
            logging.info(len(rows3))
            if len(rows3) == 0:
                rows3 = [(0,)]
        if rows3[0][0] is None:
            rows3 = [(0,)]
        logging.info("calculated value")
        logging.info(current_value)
        logging.info(rows3[0][0])
        logging.info(current_value - rows3[0][0])
        calculated_value = current_value - rows3[0][0]
    logging.info(calculated_value)
    tag_char_value = None
    quality = input_data['Quality']
    # if input_data['Quality']:
    #     quality = 1
    # else:
    #     quality = 0
    collection_time = input_data['CollectionTime']
    logging.info(tag_char_value)
    logging.info(quality)
    logging.info(collection_time)
    return calculated_value, tag_char_value, quality, collection_time

def F1_throat_temp_diff(input_data):
    logging.info('inside F1_throat_temp_diff')
    calculated_value, tag_char_value, quality, collection_time = midnight_temperature_delta(input_data, 'est')
    is_numeric = 1
    is_continuous = 1
    is_sampled = 0
    q = quality
    v = calculated_value
    return is_numeric, is_continuous, is_sampled, q, v, collection_time

def F2_throat_temp_diff(input_data):
    logging.info('inside F1_throat_temp_diff')
    calculated_value, tag_char_value, quality, collection_time = midnight_temperature_delta(input_data, 'est')
    is_numeric = 1
    is_continuous = 1
    is_sampled = 0
    q = quality
    v = calculated_value
    return is_numeric, is_continuous, is_sampled, q, v, collection_time
