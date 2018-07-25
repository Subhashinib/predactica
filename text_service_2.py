import pandas as pd
import traceback
import services.constants as constants
import services.wrangling.functions.redis_service as redis
import json, pprint, requests, textwrap,time
import services.logger_config as config
import logging.config
import database_service

logging.config.dictConfig(config.logger_config)
logger = logging.getLogger()
redis_connection = redis.get_redis_connection()


def split_column(project_id, file_id, column, delimiter):
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        startTime = time.time()
        data = {
            'code': textwrap.dedent("""
                            from pyspark.sql.functions import *
                            result = {}
                            maxcolSize=(df.withColumn("colsize",(size(split('"""+column+"""','"""+delimiter+ """')))).select(max("colsize"))).collect()[0][0]
                            for i in range(0, maxcolSize):
                              df= df.withColumn("column_"+str(i),split('"""+column+"""','"""+delimiter+ """').getItem(i))   
                            columns = len(df.columns)
                            result["rows"] = rows
                            result["columns"] = columns
                            result["sampling"] = "Random Sampling"
                            result["sampling_size"] = rows
                            metaresult = []
                            index = 0
                            for column in df.columns:
                             temp = {}
                             if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                              temp['type'] = 'integer'
                             else:
                              temp['type'] = df.dtypes[index][1] 
                             temp['index'] = index
                             temp['label'] = column
                             metaresult.append(temp)
                             index = index+1
                            result_data = []
                            temp_data = df.limit(1000).toJSON().collect()
                            for data in temp_data:
                                result_data.append(json.loads(data))
                            result['metadata'] = metaresult
                            result['data'] = result_data
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
        logger.error(r.json())
        logger.error("Process time:#############")
        logger.error(time.time() - startTime)

        transformation_id = database_service.save_rules(project_id, file_id, '', 'SPLIT COLUMN', column,
                                                        constants.split)
        database_service.store_values(transformation_id, delimiter)

        return json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())


def upper_case(project_id, file_id, column):
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        startTime = time.time()
        data = {
            'code': textwrap.dedent("""
                            from pyspark.sql.functions import *
                            result = {}
                            df = df.withColumn('""" + column + """', upper(col('""" + column + """')))      
                            columns = len(df.columns)
                            result["rows"] = rows
                            result["columns"] = columns
                            result["sampling"] = "Random Sampling"
                            result["sampling_size"] = rows
                            metaresult = []
                            index = 0
                            for column in df.columns:
                             temp = {}
                             if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                              temp['type'] = 'integer'
                             else:
                              temp['type'] = df.dtypes[index][1] 
                             temp['index'] = index
                             temp['label'] = column
                             metaresult.append(temp)
                             index = index+1

                            result_data = []
                            temp_data = df.limit(1000).toJSON().collect()
                            for data in temp_data:
                                result_data.append(json.loads(data))
                            result['metadata'] = metaresult
                            result['data'] = result_data
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
        logger.error("Process time:#############")
        logger.error(time.time() - startTime)
        database_service.save_rules(project_id, file_id, '', 'TO UPPER CASE', column, constants.upper_case)
        return json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())


def upper_case_feature(project_id, file_id, column, new_column):
    response = {}
    try:
        new_column = new_column.replace(" ", "_")
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        startTime = time.time()
        data = {
            'code': textwrap.dedent("""
                                 from pyspark.sql.functions import *
                                 response = {}
                                 if '"""+new_column+"""' in df.columns:
                                    response['message'] = "Column Name Already Exists"
                                    print(json.dumps(response))
                                 else:
                                    result = {}
                                    df = df.withColumn('""" + new_column + """', upper(col('""" + column + """')))    
                                    columns = len(df.columns)
                                    result["rows"] = rows
                                    result["columns"] = columns
                                    result["sampling"] = "Random Sampling"
                                    result["sampling_size"] = rows
                                    metaresult = []
                                    index = 0
                                    for column in df.columns:
                                     temp = {}
                                     if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                      temp['type'] = 'integer'
                                     else:
                                      temp['type'] = df.dtypes[index][1]
                                     temp['index'] = index
                                     temp['label'] = column
                                     metaresult.append(temp)
                                     index = index+1
                                    result_data = []
                                    temp_data = df.limit(1000).toJSON().collect()
                                    for data in temp_data:
                                        result_data.append(json.loads(data))
                                    result['metadata'] = metaresult
                                    result['data'] = result_data
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
        output = resp['output']['data']['text/plain']
        if 'message' in output:
            return (json.loads(resp['output']['data']['text/plain']))
        else:
            response["message"] = "success"
            response["response"] = json.loads(resp['output']['data']['text/plain'])
            transformation_id = database_service.save_rules(project_id, file_id, '', 'TO UPPER CASE', column,
                                                        constants.upper_case_feature)
            database_service.store_values(transformation_id, new_column)
            return response
    except Exception:
        logger.error(traceback.format_exc())
        response["message"] = "Internal Server Error"
    return response


def lower_case_feature(project_id, file_id, column, new_column):
    response = {}
    try:
        new_column = new_column.replace(" ", "_")
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        startTime = time.time()
        data = {
            'code': textwrap.dedent("""
                                 from pyspark.sql.functions import *
                                 response = {}
                                 if '"""+new_column+"""' in df.columns:
                                    response['message'] = "Column Name Already Exists"
                                    print(response)
                                 else:
                                    result = {}
                                    df = df.withColumn('""" + new_column + """', lower(col('""" + column + """')))    
                                    columns = len(df.columns)
                                    result["rows"] = rows
                                    result["columns"] = columns
                                    result["sampling"] = "Random Sampling"
                                    result["sampling_size"] = rows
                                    metaresult = []
                                    index = 0
                                    for column in df.columns:
                                     temp = {}
                                     if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                      temp['type'] = 'integer'
                                     else:
                                      temp['type'] = df.dtypes[index][1]
                                     temp['index'] = index
                                     temp['label'] = column
                                     metaresult.append(temp)
                                     index = index+1
                                    result_data = []
                                    temp_data = df.limit(1000).toJSON().collect()
                                    for data in temp_data:
                                        result_data.append(json.loads(data))
                                    result['metadata'] = metaresult
                                    result['data'] = result_data
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
        logger.error(resp)
        output = resp['output']['data']['text/plain']
        if 'message' in output:
            return (resp['output']['data']['text/plain'])
        else:
            response["message"] = "success"
            response["response"] = json.loads(resp['output']['data']['text/plain'])
            transformation_id = database_service.save_rules(project_id, file_id, '', 'TO LOWER CASE', column,
                                                        constants.lower_case_feature)
            database_service.store_values(transformation_id, new_column)
            return response
    except Exception:
        logger.error(traceback.format_exc())
        response["message"] = "Internal Server Error"
    return response



def concat_feature(project_id, file_id, column_list, new_column):
    response = {}
    try:
        new_column = new_column.replace(" ", "_")
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        column_string = '^'.join(column_list)
        data = {
            'code': textwrap.dedent("""
                                         from pyspark.sql.functions import *
                                         response = {}
                                         if '""" + new_column + """' in df.columns:
                                            response['message'] = "Column Name Already Exists"
                                            print(response)
                                         else:
                                            result = {}
                                            column_list='""" + column_string + """'.split('^')
                                            size = len(column_list)
                                            mergedString = (df[column_list[0]]).cast("string")
                                            for i in range(1, size): 
                                              mergedString = concat(mergedString,(df[column_list[i]]).cast("string"))
                                            df = df.withColumn('""" + new_column + """', mergedString)  
                                            columns = len(df.columns)
                                            result["rows"] = rows
                                            result["columns"] = columns
                                            result["sampling"] = "Random Sampling"
                                            result["sampling_size"] = rows
                                            metaresult = []
                                            index = 0
                                            for column in df.columns:
                                               temp = {}
                                               if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                                temp['type'] = 'integer'
                                               else:
                                                temp['type'] = df.dtypes[index][1]
                                               temp['index'] = index
                                               temp['label'] = column
                                               metaresult.append(temp)
                                               index = index+1
                                            result_data = []
                                            temp_data = df.limit(1000).toJSON().collect()
                                            for data in temp_data:
                                                 result_data.append(json.loads(data))
                                            result['metadata'] = metaresult
                                            result['data'] = result_data
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
        logger.error(resp)
        output = resp['output']['data']['text/plain']
        if 'message' in output:
            return (resp['output']['data']['text/plain'])
        else:
            response["message"] = "success"
            response["response"] = json.loads(resp['output']['data']['text/plain'])
            return response
    except Exception:
        logger.error(traceback.format_exc())
        response["message"] = "Internal Server Error"
    return response


def substring_feature(project_id, file_id, column, new_column, start, length):
    response = {}
    try:
        new_column = new_column.replace(" ", "_")
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        startTime = time.time()
        data = {
            'code': textwrap.dedent("""
                                   from pyspark.sql.functions import *
                                   response = {}
                                   if '""" + new_column + """' in df.columns:
                                      response['message'] = "Column Name Already Exists"
                                      print(response)
                                   else:
                                      result = {}
                                      df = df.withColumn('""" + new_column + """', substring(col('""" + column + """'),"""+start+""", """+length+"""))
                                      columns = len(df.columns)
                                      result["rows"] = rows
                                      result["columns"] = columns
                                      result["sampling"] = "Random Sampling"
                                      result["sampling_size"] = rows
                                      metaresult = []
                                      index = 0
                                      for column in df.columns:
                                       temp = {}
                                       if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                        temp['type'] = 'integer'
                                       else:
                                        temp['type'] = df.dtypes[index][1]
                                       temp['index'] = index
                                       temp['label'] = column
                                       metaresult.append(temp)
                                       index = index+1
                                      result_data = []
                                      temp_data = df.limit(1000).toJSON().collect()
                                      for data in temp_data:
                                          result_data.append(json.loads(data))
                                      result['metadata'] = metaresult
                                      result['data'] = result_data
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
        output = resp['output']['data']['text/plain']
        if 'message' in output:
            return (resp['output']['data']['text/plain'])
        else:
            response["message"] = "success"
            response["response"] = json.loads(resp['output']['data']['text/plain'])
            transformation_id = database_service.save_rules(project_id, file_id, '', 'CREATED COLUMN BY STRING SUBSTR',
                                                        column, constants.feature_substr)
            database_service.store_values(transformation_id,
                                      str(start) + constants.seperator + str(length) + constants.seperator + new_column)
            return response
    except Exception:
        logger.error(traceback.format_exc())
        response["message"] = "Internal Server Error"
    return response


def lower_case(project_id, file_id, column):
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        startTime = time.time()
        data = {
            'code': textwrap.dedent("""
                            from pyspark.sql.functions import *
                            result = {}
                            df = df.withColumn('""" + column + """', lower(col('""" + column + """')))   
                            columns = len(df.columns)
                            result["rows"] = rows
                            result["columns"] = columns
                            result["sampling"] = "Random Sampling"
                            result["sampling_size"] = rows
                            metaresult = []
                            index = 0
                            for column in df.columns:
                             temp = {}
                             if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                              temp['type'] = 'integer'
                             else:
                              temp['type'] = df.dtypes[index][1]
                             temp['index'] = index
                             temp['label'] = column
                             metaresult.append(temp)
                             index = index+1

                             result_data = []
                             temp_data = df.limit(1000).toJSON().collect()
                             for data in temp_data:
                                result_data.append(json.loads(data))
                            result['metadata'] = metaresult
                            result['data'] = result_data
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
        logger.error("Process time:#############")
        logger.error(time.time() - startTime)
        database_service.save_rules(project_id, file_id, '', 'TO LOWER CASE', column, constants.lower_case)
        return json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())


def ascending(project_id, file_id, column):
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        startTime = time.time()
        data = {
            'code': textwrap.dedent("""
                            from pyspark.sql.functions import *
                            result = {}
                            df = df.sort(asc('""" + column + """')) 
                            columns = len(df.columns)
                            result["rows"] = rows
                            result["columns"] = columns
                            result["sampling"] = "Random Sampling"
                            result["sampling_size"] = rows
                            metaresult = []
                            index = 0
                            for column in df.columns:
                             temp = {}
                             if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                              temp['type'] = 'integer'
                             else:
                              temp['type'] = df.dtypes[index][1]
                             temp['index'] = index
                             temp['label'] = column
                             metaresult.append(temp)
                             index = index+1

                            result_data = []
                            temp_data = df.limit(1000).toJSON().collect()
                            for data in temp_data:
                                result_data.append(json.loads(data))
                            result['metadata'] = metaresult
                            result['data'] = result_data
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
        logger.error("Process time:#############")
        logger.error(time.time() - startTime)
        database_service.save_rules(project_id, file_id, '', 'ASCENDING', column, constants.ascending)
        return json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())


def ascending_date(project_id, file_id, column):
    try:
        filename = constants.OUTPUT_HOME + str(project_id) + "/" + str(file_id) + "/original_object"
        dataframe = pd.read_pickle(filename)
        load_key = constants.getKeyForRedis(project_id, file_id, 8)
        dataframe[column] = pd.to_datetime(dataframe[column])
        dataframe = dataframe.sort_values([column], na_position="last", ascending=True)
        date_format_key = str(project_id) + str(file_id) + str(column)
        format = redis.read_key_value(redis_connection, date_format_key)
        dataframe[column] = dataframe[column].dt.strftime(format)
        dataframe.to_pickle(filename)
        row_column = dataframe.shape
        rows = row_column[0]
        if int(rows) >= 1000:
            dataframe = dataframe.head(1000)
        redis.write_key_value(redis_connection, load_key, dataframe)
        json_data = dataframe.to_json(orient="records")
        data = {}
        data['data'] = json.loads(json_data)
        meta_key = constants.getKeyForRedis(project_id, file_id, 100)
        meta_data = redis.read_key_value(redis_connection, meta_key)
        data['metadata'] = meta_data
        data["rows"] = rows
        data["sampling"] = "Random Sampling"
        data["sampling_size"] = rows
        database_service.save_rules(project_id, file_id, '', 'ASCENDING', column, constants.ascending)
        return data
    except Exception:
        logger.error(traceback.format_exc())



def descending(project_id, file_id, column):
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        startTime = time.time()
        data = {
            'code': textwrap.dedent("""
                            from pyspark.sql.functions import *
                            result = {}
                            df = df.sort(desc('""" + column + """')) 
                            columns = len(df.columns)
                            result["rows"] = rows
                            result["columns"] = columns
                            result["sampling"] = "Random Sampling"
                            result["sampling_size"] = rows
                            metaresult = []
                            index = 0
                            for column in df.columns:
                             temp = {}
                             if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                              temp['type'] = 'integer'
                             else:
                              temp['type'] = df.dtypes[index][1]
                             temp['index'] = index
                             temp['label'] = column
                             metaresult.append(temp)
                             index = index+1

                             result_data = []
                             temp_data = df.limit(1000).toJSON().collect()
                             for data in temp_data:
                                result_data.append(json.loads(data))
                            result['metadata'] = metaresult
                            result['data'] = result_data
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
        logger.error("Process time:#############")
        logger.error(time.time() - startTime)
        database_service.save_rules(project_id, file_id, '', 'DESCENDING', column, constants.descending)
        return json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())


def trailing(project_id, file_id, column):
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                    from pyspark.sql.functions import *
                                    result = {}
                                    df = df.withColumn('""" + column + """', trim(col('""" + column + """')))   
                                    columns = len(df.columns)
                                    result["rows"] = rows
                                    result["columns"] = columns
                                    result["sampling"] = "Random Sampling"
                                    result["sampling_size"] = rows
                                    metaresult = []
                                    index = 0
                                    for column in df.columns:
                                     temp = {}
                                     if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                      temp['type'] = 'integer'
                                     else:
                                      temp['type'] = df.dtypes[index][1]
                                     temp['index'] = index
                                     temp['label'] = column
                                     metaresult.append(temp)
                                     index = index+1

                                     result_data = []
                                     temp_data = df.limit(1000).toJSON().collect()
                                     for data in temp_data:
                                        result_data.append(json.loads(data))
                                    result['metadata'] = metaresult
                                    result['data'] = result_data
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
        database_service.save_rules(project_id, file_id, '', 'REMOVE TRAIL SPACE', column, constants.trailing_space)
        return json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())


def delete_column(project_id, file_id, column):
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        startTime = time.time()
        data = {
            'code': textwrap.dedent("""
                                   from pyspark.sql.functions import *
                                   result = {}
                                   df = df.drop('""" + column + """')     
                                   columns = len(df.columns)
                                   result["rows"] = rows
                                   result["columns"] = columns
                                   result["sampling"] = "Random Sampling"
                                   result["sampling_size"] = rows
                                   metaresult = []
                                   index = 0
                                   for column in df.columns:
                                    temp = {}
                                    if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                     temp['type'] = 'integer'
                                    else:
                                     temp['type'] = df.dtypes[index][1]
                                    temp['index'] = index
                                    temp['label'] = column
                                    metaresult.append(temp)
                                    index = index+1
                                    result_data = []
                                    temp_data = df.limit(1000).toJSON().collect()
                                    for data in temp_data:
                                       result_data.append(json.loads(data))
                                   result['metadata'] = metaresult
                                   result['data'] = result_data
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
        logger.error("Process time:#############")
        logger.error(time.time() - startTime)
        database_service.save_rules(project_id, file_id, '', 'DELETE COLUMN', column, constants.delete)
        return json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())


def delete_column_mul(project_id, file_id, column_list):
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                           from pyspark.sql.functions import *
                                           result = {}
                                           for colname in """+column_list+""":
                                            df = df.drop(colname)
                                           columns = len(df.columns)
                                           result["rows"] = rows
                                           result["columns"] = columns
                                           result["sampling"] = "Random Sampling"
                                           result["sampling_size"] = rows
                                           metaresult = []
                                           index = 0
                                           for column in df.columns:
                                            temp = {}
                                            if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                             temp['type'] = 'integer'
                                            else:
                                             temp['type'] = df.dtypes[index][1]
                                            temp['index'] = index
                                            temp['label'] = column
                                            metaresult.append(temp)
                                            index = index+1
                                            result_data = []
                                            temp_data = df.limit(1000).toJSON().collect()
                                            for data in temp_data:
                                               result_data.append(json.loads(data))
                                           result['metadata'] = metaresult
                                           result['data'] = result_data
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
        return json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())


def rename_column(project_id, file_id, column, new_column):
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                    from pyspark.sql.functions import *
                                    result = {}
                                    df = df.withColumnRenamed('""" + column + """','""" + new_column + """')  
                                    columns = len(df.columns)
                                    result["rows"] = rows
                                    result["columns"] = columns
                                    result["sampling"] = "Random Sampling"
                                    result["sampling_size"] = rows
                                    metaresult = [] 
                                    index = 0
                                    for column in df.columns:
                                     temp = {}
                                     if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                      temp['type'] = 'integer'
                                     else:
                                      temp['type'] = df.dtypes[index][1]
                                     temp['index'] = index
                                     temp['label'] = column
                                     metaresult.append(temp)
                                     index = index+1

                                     result_data = []
                                     temp_data = df.limit(1000).toJSON().collect()
                                     for data in temp_data:
                                        result_data.append(json.loads(data))
                                    result['metadata'] = metaresult
                                    result['data'] = result_data
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
        logger.error(r.json())
        transformation_id = database_service.save_rules(project_id, file_id, '', 'RENAME COLUMN', column,
                                                        constants.rename)
        database_service.store_values(transformation_id, new_column)
        return json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())


def replace_str(project_id, file_id, column, search_text, replace_text):
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                           from pyspark.sql.functions import *
                                           result = {}
                                           df = df.withColumn('""" +column+ """', regexp_replace('"""+column+"""','"""+search_text+"""','"""+replace_text+"""'))
                                           columns = len(df.columns)
                                           result["rows"] = rows
                                           result["columns"] = columns
                                           result["sampling"] = "Random Sampling"
                                           result["sampling_size"] = rows
                                           metaresult = [] 
                                           index = 0
                                           for column in df.columns:
                                            temp = {}
                                            if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                             temp['type'] = 'integer'
                                            else:
                                             temp['type'] = df.dtypes[index][1]
                                            temp['index'] = index
                                            temp['label'] = column
                                            metaresult.append(temp)
                                            index = index+1

                                            result_data = []
                                            temp_data = df.limit(1000).toJSON().collect()
                                            for data in temp_data:
                                               result_data.append(json.loads(data))
                                           result['metadata'] = metaresult
                                           result['data'] = result_data
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
        logger.error(r.json())
        transformation_id = database_service.save_rules(project_id, file_id, '', 'REPLACE STRING', column,
                                                        constants.replace_string)
        database_service.store_values(transformation_id, search_text + constants.seperator + replace_text)
        return json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())


def replace_str_feature(project_id, file_id, column, search_text, replace_text, new_column):
    response = {}
    try:
        new_column = new_column.replace(" ", "_")
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                          from pyspark.sql.functions import *
                                          response = {}
                                          if '""" + new_column + """' in df.columns:
                                             response['message'] = "Column Name Already Exists"
                                             print(response)
                                          else:
                                             result = {}
                                             df = df.withColumn('""" +new_column+ """', regexp_replace('"""+column+"""','"""+search_text+"""','"""+replace_text+"""'))
                                             columns = len(df.columns)
                                             result["rows"] = rows
                                             result["columns"] = columns
                                             result["sampling"] = "Random Sampling"
                                             result["sampling_size"] = rows
                                             metaresult = []
                                             index = 0
                                             for column in df.columns:
                                              temp = {}
                                              if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                               temp['type'] = 'integer'
                                              else:
                                               temp['type'] = df.dtypes[index][1]
                                              temp['index'] = index
                                              temp['label'] = column
                                              metaresult.append(temp)
                                              index = index+1
                                              result_data = []
                                              temp_data = df.limit(1000).toJSON().collect()
                                              for data in temp_data:
                                                 result_data.append(json.loads(data))
                                             result['metadata'] = metaresult
                                             result['data'] = result_data
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
        output = resp['output']['data']['text/plain']
        if 'message' in output:
            return (resp['output']['data']['text/plain'])
        else:
            response["message"] = "success"
            response["response"] = json.loads(resp['output']['data']['text/plain'])
            transformation_id = database_service.save_rules(project_id, file_id, '', 'REPLACE STRING', column,
                                                            constants.replace_string_constant)
            database_service.store_values(transformation_id,
                                          search_text + constants.seperator + replace_text + constants.seperator + new_column)
            return response
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response


def categorical(project_id, file_id, column, new_name):
    try:
        new_name = new_name.replace(" ", "_")
        filename = constants.OUTPUT_HOME + str(project_id) + "/" + str(file_id) + "/original_object"
        dataframe = pd.read_pickle(filename)
        if new_name in list(dataframe):
            return None
        load_key = constants.getKeyForRedis(project_id, file_id, 8)
        unique_list = list(set(dataframe[column].tolist()))
        keys = {}
        if len(unique_list) == 2:
            index = 0
        else:
            index = 1
        result_list = []
        for value in unique_list:
            if value is not None and value == value:
                keys[value] = index
                index = index + 1
        value_list = dataframe[column].tolist()
        for value in value_list:
            if value is not None and value == value:
                result_list.append(keys[value])
            else:
                result_list.append(None)
        dataframe[new_name] = result_list
        dataframe.to_pickle(filename)
        row_column = dataframe.shape
        rows = row_column[0]
        if int(rows) >= 1000:
            dataframe = dataframe.sample(1000)
        redis.write_key_value(redis_connection, load_key, dataframe)
        json_data = dataframe.to_json(orient="records")
        data = {}
        data['data'] = json.loads(json_data)
        meta_key = constants.getKeyForRedis(project_id, file_id, 100)
        meta_data = redis.read_key_value(redis_connection, meta_key)
        meta_data = get_metadata_after_column(meta_data, column, new_name, "integer")
        redis.write_key_value(redis_connection, meta_key, meta_data)
        data['metadata'] = meta_data
        data["rows"] = rows
        data["sampling"] = "Random Sampling"
        data["sampling_size"] = rows
        new_key = constants.getKeyForRedis(project_id, file_id, 2)
        redis.write_key_value(redis_connection, new_key, 1)
        transformation_id = database_service.save_rules(project_id, file_id, '', 'CONVERT TO CATEGORICAL', column,
                                                        constants.categorical)
        database_service.store_values(transformation_id, new_name)
        return data
    except Exception:
        logger.error(traceback.format_exc())


def tostring(project_id, file_id, column):
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                    from pyspark.sql.functions import *
                                    result = {}
                                    df=df.withColumn('"""+column+"""', col('"""+column+"""').cast("string"))
                                    columns = len(df.columns)
                                    result["rows"] = rows
                                    result["columns"] = columns
                                    result["sampling"] = "Random Sampling"
                                    result["sampling_size"] = rows
                                    metaresult = []
                                    index = 0
                                    for column in df.columns:
                                     temp = {}
                                     if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                      temp['type'] = 'integer'
                                     else:
                                      temp['type'] = df.dtypes[index][1]
                                     temp['index'] = index
                                     temp['label'] = column
                                     metaresult.append(temp)
                                     index = index+1

                                     result_data = []
                                     temp_data = df.limit(1000).toJSON().collect()
                                     for data in temp_data:
                                        result_data.append(json.loads(data))
                                    result['metadata'] = metaresult
                                    result['data'] = result_data
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
        database_service.save_rules(project_id, file_id, '', 'TO STRING', column, constants.to_string)
        return json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())


def to_integer(project_id, file_id, column):
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                    from pyspark.sql.functions import *
                                    from pyspark.sql.types import IntegerType
                                    result = {}
                                    df=df.withColumn('"""+column+"""', col('"""+column+"""').cast(IntegerType()))
                                    columns = len(df.columns)
                                    result["rows"] = rows
                                    result["columns"] = columns
                                    result["sampling"] = "Random Sampling"
                                    result["sampling_size"] = rows
                                    metaresult = []
                                    index = 0
                                    for column in df.columns:
                                     temp = {}
                                     if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                      temp['type'] = 'integer'
                                     else:
                                      temp['type'] = df.dtypes[index][1]
                                     temp['index'] = index
                                     temp['label'] = column
                                     metaresult.append(temp)
                                     index = index+1

                                     result_data = []
                                     temp_data = df.limit(1000).toJSON().collect()
                                     for data in temp_data:
                                        result_data.append(json.loads(data))
                                    result['metadata'] = metaresult
                                    result['data'] = result_data
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
        logger.error(resp)
        database_service.save_rules(project_id, file_id, '', 'TO STRING', column, constants.to_integer)
        return json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())


def merge_column(project_id, file_id, column_list, column_name, seperator):
    response = {}
    try:
        new_column = column_name.replace(" ", "_")
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        column_string = '^'.join(column_list)
        data = {
            'code': textwrap.dedent("""
                                         from pyspark.sql.functions import *
                                         response = {}
                                         if '""" + column_name + """' in df.columns:
                                            response['message'] = "Column Name Already Exists"
                                            print(response)
                                         else:
                                            result = {}
                                            column_list='""" + column_string + """'.split('^')
                                            size = len(column_list)
                                            mergedString = (df[column_list[0]]).cast("string")
                                            for i in range(1, size): 
                                              mergedString = concat(mergedString,lit('"""+seperator+"""'),(df[column_list[i]]).cast("string"))
                                            df = df.withColumn('""" + new_column + """', mergedString)  
                                            columns = len(df.columns)
                                            result["rows"] = rows
                                            result["columns"] = columns
                                            result["sampling"] = "Random Sampling"
                                            result["sampling_size"] = rows
                                            metaresult = []
                                            index = 0
                                            for column in df.columns:
                                               temp = {}
                                               if (df.dtypes[index][1] =='double' or df.dtypes[index][1] =='int'):
                                                temp['type'] = 'integer'
                                               else:
                                                temp['type'] = df.dtypes[index][1]
                                               temp['index'] = index
                                               temp['label'] = column
                                               metaresult.append(temp)
                                               index = index+1
                                            result_data = []
                                            temp_data = df.limit(1000).toJSON().collect()
                                            for data in temp_data:
                                                 result_data.append(json.loads(data))
                                            result['metadata'] = metaresult
                                            result['data'] = result_data
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
        logger.error(resp)
        output = resp['output']['data']['text/plain']
        if 'message' in output:
            return (resp['output']['data']['text/plain'])
        else:
            response["message"] = "success"
            response["response"] = json.loads(resp['output']['data']['text/plain'])
            return response
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response


def change_date(project_id, file_id, column, format):
    response = {}
    try:
        filename = constants.OUTPUT_HOME + str(project_id) + "/" + str(file_id) + "/original_object"
        dataframe = pd.read_pickle(filename)
        load_key = constants.getKeyForRedis(project_id, file_id, 8)
        dataframe[column] = pd.to_datetime(dataframe[column])
        dataframe[column] = dataframe[column].dt.strftime(format)
        dataframe.to_pickle(filename)
        row_column = dataframe.shape
        rows = row_column[0]
        if int(rows) >= 1000:
            dataframe = dataframe.sample(1000)
        redis.write_key_value(redis_connection, load_key, dataframe)
        json_data = dataframe.to_json(orient="records")
        data = {}
        data['data'] = json.loads(json_data)
        meta_key = constants.getKeyForRedis(project_id, file_id, 100)
        meta_data = redis.read_key_value(redis_connection, meta_key)
        for value in meta_data:
            if value['label'] == column:
                value['type'] = "date"
        new_key = constants.getKeyForRedis(project_id, file_id, 2)
        redis.write_key_value(redis_connection, new_key, 1)
        date_format_key = str(project_id) + str(file_id) + str(column)
        redis.write_key_value(redis_connection, date_format_key, format)
        data['metadata'] = meta_data
        data["rows"] = rows
        data["sampling"] = "Random Sampling"
        data["sampling_size"] = rows
        response['message'] = "success"
        response['response'] = data
        transformation_id = database_service.save_rules(project_id, file_id, '', 'CHANGE DATE FORMAT', column,
                                                        constants.change_date)
        database_service.store_values(transformation_id, format)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Column not eligible for date"
    return response


def delete_metadata(metadata, column):
    column_index = 0
    for data in metadata:
        if data['label'] == column:
            column_index = data['index']
            metadata.remove(data)
    for data in metadata:
        if data['index'] > column_index:
            data['index'] = data['index'] - 1
    return metadata


def convert_metadata(metadata, column):
    for data in metadata:
        if data['label'] == column:
            if data['type'] == "string":
                data['type'] = "integer"
            else:
                data['type'] = "string"
        break;
    return metadata


def get_metadata_split(metadata, column, count, column_list, data_frame):
    column_index = 0
    for data in metadata:
        if data['label'] == column:
            column_index = data['index']
    for data in metadata:
        if data['index'] > column_index:
            data['index'] = data['index'] + count

    for i in range(0, len(column_list)):
        temp = {}
        temp['label'] = column_list[i]
        if data_frame[column_list[i]].dtype == object or data_frame[column_list[i]].dtype.name == "category":
            temp['type'] = "string"
        else:
            temp['type'] = "integer"
        temp['index'] = column_index + i + 1
        metadata.append(temp)
    return metadata


def rename_metadata(meta_data, old_name, new_name):
    for json in meta_data:
        if json['label'] == old_name:
            json['label'] = new_name
            break
    return meta_data


def get_metadata_last(meta_data, column_name):
    output = {}
    output['label'] = column_name
    output['index'] = len(meta_data)
    output['type'] = "string"
    meta_data.append(output)
    return meta_data


def get_metadata_after_column(meta_data, column_name, new_column, data_type):
    index = 0
    for json in meta_data:
        if json['label'] == column_name:
            index = json['index']
            break
    for json in meta_data:
        if json['index'] > index:
            json['index'] = json['index'] + 1
    output = {}
    output['type'] = data_type
    output['index'] = index + 1
    output['label'] = new_column
    meta_data.append(output)
    return meta_data


def get_metadata_for_dataframe(dataframe):
    result = []
    index = 0
    column_list = []
    total_rows = dataframe.shape[0]
    """for column_name in dataframe:
        if dataframe[column_name].dtype == 'object':
            invalid = getDateError(dataframe, column_name)
            if invalid < total_rows / 2:
                column_list.append(column_name)"""
    for column in dataframe:
        temp = {}
        if dataframe[column].dtype == "object":
            temp['type'] = "string"
        else:
            temp['type'] = "integer"
        temp['index'] = index
        temp['label'] = column
        result.append(temp)
        index = index + 1
    """for column in column_list:
        for data in result:
            if data['label'] == column:
                data['type'] = "datetime"""
    return result


session_id = 1