import logging
import azure.functions as func
from typing import List
import json
import logging
import datetime
import os
from derived_tags_utils import helper
import pandas as pd

#logging.getLogger().setLevel(logging.INFO)

eventhub_name = "controlroomseventhub-vedhatest"
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
#app = func.FunctionApp()

#@app.route(route="jesumehttp", auth_level=func.AuthLevel.ANONYMOUS)
#def jesumehttp(req: func.HttpRequest) -> func.HttpResponse:

@app.function_name(name="eventhub_output")
@app.route(route="eventhub_output")
#@app.route(route="eventhub_output", auth_level=func.AuthLevel.ANONYMOUS)
@app.event_hub_output(arg_name="event",
                      event_hub_name=eventhub_name,
                      connection='event_hub_connection')
def eventhub_output(req: func.HttpRequest, event: func.Out[str]):
#def eventhub_output(req: func.HttpRequest, event: func.Out[str]) -> func.HttpResponse:
    logging.warning("inside event_hub_output function")
#def eventhub_output(req: func.HttpRequest):

# @app.function_name(name="derived_tags")
# @app.event_hub_output(name="event",
#                       event_hub_name=eventhub_name,
#                       connection=connection_str)
# @app.route(route="derived_tags")
# #def derived_tags(req: func.HttpRequest):
# def derived_tags(req: func.HttpRequest, event: func.Out[str]):
    connection_str = os.getenv('event_hub_connection')
    # logging.info("***********************************************")
    # logging.info(connection_str)
    # logging.info("***********************************************")
    try:
        logging.warning("inside try " + str(req.get_json()))
        req_body = req.get_json()
        event_count = 0
        logging.info("count")
        logging.info(event_count)
        for req in req_body:
            event_count += 1
            logging.info("count")
            logging.info(event_count)
            #logging.info(req["Events"])
            input_data = req["Events"] if isinstance(req["Events"], list) else []
            logging.info("input_data")
            logging.info(input_data)
            derived_tag_data = calculate(input_data)
            #logging.info("dertived_tag_data" + str(derived_tag_data))

            id = derived_tag_data["DerivedTag"]
            is_numeric = derived_tag_data["is_numeric"]
            is_continuous = derived_tag_data["is_continuous"]
            is_sampled = derived_tag_data["is_sampled"]
            t = derived_tag_data["Timestamp"]
            q = derived_tag_data["Quality"]
            v = derived_tag_data["CalculatedValue"]
            eventProcessedUtcTime = derived_tag_data["CollectionTime"]
            eventEnqueuedUtcTime = derived_tag_data["CollectionTime"]
            #logging.info("before timestamp")
            timestamp = timestamp_to_milliseconds(t)
            #logging.info("after timestamp")
        
            event_data = {
            "id": id,
            "is_numeric": is_numeric,
            "is_continuous": is_continuous,
            "is_sampled": is_sampled,
            "t": t,
            "q": q,
            "v": v,
            "eventProcessedUtcTime": eventProcessedUtcTime,
            "eventEnqueuedUtcTime": eventEnqueuedUtcTime,
            "timestamp": timestamp  # Convert timestamp to milliseconds
            }
            #logging.info(event_data)
            event.set(json.dumps(event_data))
            logging.info("Event sent to Event Hub successfully.")
        return func.HttpResponse("Events sent to Event Hub successfully.", status_code=200)
    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        return func.HttpResponse("Error processing request", status_code=500)

# import azure.functions as func
# from azure.eventhub.aio import EventHubProducerClient
# from azure.eventhub import EventData
# import logging
# import json
# import psycopg2
# import os
# from tenacity import *
# import datetime

# from derived_tags_utils import helper

# connection_str = "Endpoint=sb://evthub-vedhatest-ns.servicebus.windows.net/;SharedAccessKeyName=vedhatestControlRoomsMQTTRepartitioningASA__policy;SharedAccessKey=8Nei+zxHOukxxVhunQ/2jNK4J9i8bkkDd+AEhCoVxhk=;EntityPath=controlroomseventhub-vedhatest"
# eventhub_name = "controlroomseventhub-vedhatest"
# app = func.FunctionApp()

# @app.function_name("derived_tags")
# @app.event_hub_output(name="event",
#                       event_hub_name=eventhub_name,
#                       connection=connection_str)
# @app.route(route="derived_tags")
# def derived_tags(req: func.HttpRequest):
# #def derived_tags(req: func.HttpRequest, event: func.Out[str]) -> func.HttpResponse:
#     try:
#         logging.info("inside try " + str(req.get_json()))
#         req_body = req.get_json()
#         logging.info(req_body[0]["Events"])
#         input_data = req_body[0]["Events"] if isinstance(req_body[0]["Events"], list) else []
#         logging.info("input_data")
#         logging.info(input_data)
#         derived_tag_data = calculate(input_data)
#         logging.info("dertived_tag_data" + str(derived_tag_data))

#         id = derived_tag_data["DerivedTag"]
#         is_numeric = derived_tag_data["is_numeric"]
#         is_continuous = derived_tag_data["is_continuous"]
#         is_sampled = derived_tag_data["is_sampled"]
#         t = derived_tag_data["Timestamp"]
#         q = derived_tag_data["Quality"]
#         v = derived_tag_data["CalculatedValue"]
#         eventProcessedUtcTime = derived_tag_data["CollectionTime"]
#         eventEnqueuedUtcTime = derived_tag_data["CollectionTime"]
#         timestamp = timestamp_to_milliseconds(t)
        
#         event_data = {
#             "id": id,
#             "is_numeric": is_numeric,
#             "is_continuous": is_continuous,
#             "is_sampled": is_sampled,
#             "t": t,
#             "q": q,
#             "v": v,
#             "eventProcessedUtcTime": eventProcessedUtcTime,
#             "eventEnqueuedUtcTime": eventEnqueuedUtcTime,
#             "timestamp": timestamp  # Convert timestamp to milliseconds
#         }
#         #event.set(json.dumps(event_data))
#         logging.info("Event sent to Event Hub successfully.")
#         return func.HttpResponse("Event sent to Event Hub successfully.", status_code=200)
#     except Exception as e:
#         logging.error(f"Error processing request: {str(e)}")
#         return func.HttpResponse("Error processing request", status_code=500)
    
def timestamp_to_milliseconds(timestamp):
    #logging.info("inside timestamp function")
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    #logging.info("line 146")
    #logging.info(pd.to_datetime(timestamp))
    #logging.info(epoch)
    delta = pd.to_datetime(timestamp) - epoch
    #logging.info("line 148")
    milliseconds = int(delta.total_seconds() * 1000)
    #logging.info("line 150")
    #logging.info(milliseconds)
    return milliseconds

def retrieve_value(val):
    if val is not None and (type(val) == int or type(val) == float):
        num_val = val
        char_val = "NULL"
    elif val is not None and (type(val) == str or type(val) == bool):
        num_val = "NULL"
        val = val.replace('\'', '\'\'')
        char_val = val
    else:
        num_val = "NULL,"
        char_val = "NULL,"
    return num_val, char_val


def calculate(input_data):
    # Create a dictionary to store input values for each derived tag and partition
    data_groups = {}
    key = input_data[0]['t']
    derived_tag = input_data[0]['derived_tag_name']
    logging.info("inside calculate")
    logging.info(input_data[0]['function_name'])
    derivation_function = getattr(helper, input_data[0]['function_name'])
    logging.info("derivation_function_string"+str(derivation_function))
    partition = input_data[0]['partition_key']
    data_groups = {'Timestamp':key,'DerivedTag':derived_tag,'Partition':partition,'InputData':[]}

    # Iterate through the input data to organize it
    for item in input_data:
        timestamp = item['t']
        input_column = item['input_tag']
        input_value,char_value = retrieve_value(item['v'])
        quality = item['q']
        collection_time = item['EventEnqueuedUtcTime']

        key = timestamp
        data_groups['InputData'].append({
            'InputColumn': input_column,
            'InputValue': input_value,
            'InputCharValue': char_value,
            'Quality': quality,
            'CollectionTime': collection_time,
            'TagTime': timestamp
        })


    #Calculate the result for each group
    logging.info("data_groups "+str(data_groups))
    is_numeric, is_continuous, is_sampled, q, v, collection_time = derivation_function(data_groups['InputData'])
    logging.info(str(v)+ str(q)+ str(collection_time))
    calculated_result = {
                'Timestamp':data_groups['Timestamp'],
                'DerivedTag': data_groups['DerivedTag'],
                'Partition': data_groups['Partition'],
                'CalculatedValue': v,
                'is_numeric': is_numeric,
                'is_continuous': is_continuous,
                'is_sampled': is_sampled,
                'Quality': q,
                'CollectionTime': collection_time
            }

    return calculated_result





@app.route(route="jesumehttp", auth_level=func.AuthLevel.ANONYMOUS)
def jesumehttp(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )