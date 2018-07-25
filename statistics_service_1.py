

import pandas as pd
import traceback
import json
import numpy as np
import services.constants as constants
import services.wrangling.functions.redis_service as redis
import services.logger_config as config
import logging.config
import json, pprint, requests, textwrap,time


logging.config.dictConfig(config.logger_config)
logger = logging.getLogger()
redis_connection = redis.get_redis_connection()



def get_feature_stat(source_file,project_id,file_id):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        #session_id = redis.read_key_value(redis_connection, load_key)
        session_id = "1"
        source_file= "/home/ubuntu/rajan/nodeapi/datafile/1530246480023_76_406_1522096458.41.cs"
        data = {
            'code': textwrap.dedent("""
                        from pyspark.sql.functions import *
                        df2 = spark.read.format("csv") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .load('"""+source_file+"""')
                        rank = 0
                        output = []
                        for column in df2.columns:
                            unique_count = df2.agg(countDistinct(column)).collect()[0][0]
                            total = df2.count()
                            result = {}
                            result['column_name'] = column
                            if unique_count <= total / 10:
                                result["ftype"] = "classification"
                            else:
                                result["ftype"] = "continuous"
                            if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                temp['type'] = 'integer'
                            else:
                                temp['type'] = 'string' 
                            result['rank'] = rank
                            rank = rank + 1
                            output.append(result)
                        print(json.dumps(output))
                """)
                   }
        statements_url = constants.LIVY_HOST + '/sessions/' + session_id + '/statements'
        r = requests.post(statements_url, data=json.dumps(data), headers=constants.LIVY_HEADERS)
        resp = r.json()
        statement_url = constants.LIVY_HOST + r.headers['location']
        while resp['state'] == 'waiting' or resp['state'] == 'running':
          r = requests.get(statement_url, headers=constants.LIVY_HEADERS)
          resp = r.json()
        logger.error(resp)
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
    except Exception:
             logger.error(traceback.format_exc())
             response['message'] = "Internal Server Error"
    return response





def get_min_max(project_id,file_id,column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
        'code': textwrap.dedent("""
                                            import json
                                            from pyspark.sql.functions import *
                                            result = {}
                                            result['min'] = int(float(df.describe('""" + column + """').collect()[3][1]))
                                            result['max'] = int(float(df.describe('""" + column + """').collect()[4][1]))
                                            print(json.dumps(result))
                                      """)
                }

        statements_url = constants.LIVY_HOST + '/sessions/' + session_id + '/statements'
        r = requests.post(statements_url, data=json.dumps(data), headers=constants.LIVY_HEADERS)
        resp = r.json()
        statement_url = constants.LIVY_HOST + r.headers['location']
        while resp['state'] == 'waiting' or resp['state'] == 'running':
            r = requests.get(statement_url, headers=constants.LIVY_HEADERS)
            resp = r.json()
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

