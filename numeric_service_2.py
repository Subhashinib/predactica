    import traceback
import services.constants as constants
import services.wrangling.functions.redis_service as redis
import services.logger_config as config
import logging.config
import services.wrangling.functions.database_service as db
import json, pprint, requests, textwrap

logging.config.dictConfig(config.logger_config)
logger = logging.getLogger()
redis_connection = redis.get_redis_connection()


def get_bining(project_id,file_id,column,start_list,end_list,label_list,new_column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        column_string = '@'.join(start_list)
        end_string = '@'.join(end_list)
        label_string = '@'.join(label_list)
        data = {
            'code': textwrap.dedent("""
                                from pysparkd.sql.functions import *
                                from pyspark.ml.feature import Bucketizer
                                from pyspark.ml import Pipeline
                                from pyspark.sql.types import StringType
                                
                                result = {}
                                labellist='"""+label_string+"""'.split('@')
                                column_list='"""+column_string+"""'.split('@')
                                size = len(column_list) 
                                endlist='"""+end_string+"""'.split('@')
                                size = len(endlist) 
                                new_list = []
                                for item in column_list:
                                 new_list.append(float(item))   
                                new_list.append(float(endlist[size-1]))    
                                df = Bucketizer(splits=new_list, inputCol='"""+column+"""', outputCol='"""+new_column+"""').transform(df)
                                label=udf(lambda v: (labellist[int(v)] if v is not None else v))
                                df=df.withColumn('"""+new_column+"""',label('"""+new_column+"""'))
                                columns = len(df.columns)
                                result["rows"] = rows
                                result["columns"] = columns
                                result["sampling"] = "Random Sampling"
                                result["sampling_size"] = rows 
                                result_data = []
                                temp_data = df.limit(1000).toJSON().collect()
                                for data in temp_data:
                                    result_data.append(json.loads(data))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'DATA BINING', column,constants.bining)
        db.store_values(transformation_id,
                                    db.get_list_as_hash(start_list) + constants.seperator + db.get_list_as_hash(end_list) + constants.seperator + db.get_list_as_hash(label_list) + constants.seperator + new_column)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Invalid Data for Bining"
    return response


def get_bining_percentage(project_id,file_id,column,start_list,end_list,label_list,new_column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        column_string = '@'.join(start_list)
        end_string = '@'.join(end_list)
        label_string = '@'.join(label_list)
        data = {
            'code': textwrap.dedent("""
                                from pyspark.sql.functions import *
                                from pyspark.ml.feature import Bucketizer
                                from pyspark.ml import Pipeline
                                from pyspark.sql.types import StringType

                                result = {}
                                labellist='""" + label_string + """'.split('@')
                                column_list='""" + column_string + """'.split('@')
                                size = len(column_list) 
                                endlist='""" + end_string + """'.split('@')
                                size = len(endlist) 
                                new_list = []
                                for item in column_list:
                                 new_list.append(float(item))   
                                new_list.append(float(endlist[size-1]))    
                                df = Bucketizer(splits=new_list, inputCol='""" + column + """', outputCol='""" + new_column + """').transform(df)
                                label=udf(lambda v: (labellist[int(v)] if v is not None else v))
                                df=df.withColumn('""" + new_column + """',label('""" + new_column + """'))
                                columns = len(df.columns)
                                result["rows"] = rows
                                result["columns"] = columns
                                result["sampling"] = "Random Sampling"
                                result["sampling_size"] = rows 
                                result_data = []
                                temp_data = df.limit(1000).toJSON().collect()
                                for data in temp_data:
                                    result_data.append(json.loads(data))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'DATA BINING', column, constants.bining)
        db.store_values(transformation_id,
                        db.get_list_as_hash(start_list) + constants.seperator + db.get_list_as_hash(
                            end_list) + constants.seperator + db.get_list_as_hash(
                            label_list) + constants.seperator + new_column)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Invalid Data for Bining"
    return response


def get_normal_scaling(project_id,file_id,column,new_column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                from pyspark.sql.functions import *
                                from pyspark.ml.feature import StandardScaler
                                from pyspark.ml.feature import VectorAssembler
                                from pyspark.ml import Pipeline
                                from pyspark.sql.functions import udf
                                from pyspark.sql.types import FloatType
                                result = {}
                                df = df.where(col('""" + column + """').isNotNull())
                                age_assembler = VectorAssembler(inputCols= ['"""+column+"""'], outputCol = "age_feature")
                                scaler = StandardScaler(inputCol="age_feature", outputCol='"""+new_column+"""')
                                age_pipeline = Pipeline(stages=[age_assembler, scaler])
                                scalerModel = age_pipeline.fit(df)
                                df=scalerModel.transform(df)
                                df=df.drop("age_feature") 
                                firstelement=udf(lambda v:float(v[0]),FloatType())
                                df=df.withColumn('"""+new_column+"""',round(firstelement('"""+new_column+"""'),4))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Invalid column for scaling"
    return response


def get_bining_quantile(project_id,file_id,column,new_column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                from pyspark.sql.functions import *
                                from pyspark.ml.feature import QuantileDiscretizer
                                from pyspark.ml import Pipeline
                                result = {}
                                df = QuantileDiscretizer(numBuckets=4, inputCol='"""+column+"""', outputCol='"""+new_column+"""').fit(df).transform(df)
                                columns = len(df.columns)
                                result["rows"] = rows
                                result["columns"] = columns
                                result["sampling"] = "Random Sampling"
                                result["sampling_size"] = rows
                                result_data = []
                                temp_data = df.limit(1000).toJSON().collect()
                                for data in temp_data:
                                    result_data.append(json.loads(data))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'QUANTILE BINING', column,
                                                      constants.quantile)
        db.store_values(transformation_id, new_column)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Invalid column for quantile bining"
    return response



def get_minmax_scaling(project_id,file_id,column,new_column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                from pyspark.sql.functions import *
                                from pyspark.ml.feature import MinMaxScaler
                                from pyspark.ml.feature import VectorAssembler
                                from pyspark.ml import Pipeline
                                from pyspark.sql.functions import udf
                                from pyspark.sql.types import FloatType
                                result = {}
                                df = df.where(col('""" + column + """').isNotNull())
                                age_assembler = VectorAssembler(inputCols= ['"""+column+"""'], outputCol = "age_feature")
                                scaler = MinMaxScaler(inputCol="age_feature", outputCol='"""+new_column+"""')
                                age_pipeline = Pipeline(stages=[age_assembler, scaler])
                                scalerModel = age_pipeline.fit(df)
                                df=scalerModel.transform(df)
                                df=df.drop("age_feature") 
                                firstelement=udf(lambda v:float(v[0]),FloatType())
                                df=df.withColumn('"""+new_column+"""',round(firstelement('"""+new_column+"""'),4))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Invalid column for scaling"
    return response


def get_log_scaling(project_id,file_id,column,new_column):

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
                                      df = df.withColumn('""" + new_column + """',round(log(col('""" + column + """')),4))
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



def feature_sum(project_id,file_id,column_list,new_column):
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
                                        column_list='"""+column_string+"""'.split('^')
                                        size = len(column_list)
                                        totalSum = df[column_list[0]]
                                        for i in range(1, size): 
                                          totalSum = totalSum + df[column_list[i]]
                                        df = df.withColumn('"""+new_column+"""', totalSum)  
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
            logger.error(json.loads(resp['output']['data']['text/plain']))
            return (json.loads(resp['output']['data']['text/plain']))
        else:
            response["message"] = "success"
            response["response"] = json.loads(resp['output']['data']['text/plain'])
            logger.error(json.loads(resp['output']['data']['text/plain']))
            return response
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response


def feature_sub(project_id,file_id,column_list,new_column):
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
                                        column_list='"""+column_string+"""'.split('^')
                                        size = len(column_list)
                                        total = df[column_list[0]]
                                        for i in range(1, size): 
                                          total = total - df[column_list[i]]
                                        df = df.withColumn('"""+new_column+"""', total)  
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
            return response
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def feature_mul(project_id,file_id,column_list,new_column):
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
                                        column_list='"""+column_string+"""'.split('^')
                                        size = len(column_list)
                                        total = df[column_list[0]]
                                        for i in range(1, size): 
                                          total = total * df[column_list[i]]
                                        df = df.withColumn('"""+new_column+"""', total)  
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
            return response
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response


def feature_div(project_id,file_id,column_list,new_column):
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
                                        column_list='"""+column_string+"""'.split('^')
                                        size = len(column_list)
                                        total = df[column_list[0]]
                                        for i in range(1, size): 
                                          total = total / df[column_list[i]]
                                        df = df.withColumn('"""+new_column+"""', total)  
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
            return response
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def feature_log(project_id,file_id,column,new_column):

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
                                      df = df.withColumn('""" + new_column + """',round(log(col('""" + column + """')),4))
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

def replace_int(project_id,file_id,column,search_int,replace_inte,new_column):

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
                                      if '"""+search_int+"""' == "null":
                                       replacenull = udf((lambda x: (x if x is not None else """+replace_inte+""")))
                                       df = df.withColumn('"""+new_column+"""', replacenull('"""+column+"""'))
                                      else:
                                       replaceint = udf(lambda x: (x if x != int(search_int) else """+replace_inte+"""))
                                       df = df.withColumn('"""+new_column+"""', replaceint('"""+column+"""'))
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
        transformation_id = db.save_rules(project_id, file_id, '','CREATED COLUMN BY replacing int', column,
                                                      constants.replace_int_constant)
        db.store_values(transformation_id, str(search_int) + constants.seperator + str(replace_inte) + constants.seperator + new_column)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response


def get_filter_equal(project_id,file_id,column,value):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                from pyspark.sql.functions import *
                                result = {}
                                value_list = []
                                try:
                                  value_list.append(int('"""+value+"""'))
                                except ValueError:
                                  value_list.append('"""+value+"""')
                                df=df.filter(col('""" + column + """').isin(value_list)) 
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'FILTERED DATA', column,
                                                      constants.filter_equal)
        db.store_values(transformation_id, value)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def get_filter_not_equal(project_id,file_id,column,value):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                    from pyspark.sql.functions import *
                                    result = {}
                                    try:
                                     value= int('"""+value+"""')
                                    except ValueError:
                                     value = '"""+value+"""'                                  
                                    df=df.where(col('""" + column + """')!=value) 
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'FILTERED DATA', column,
                                          constants.filter_not_equal)
        db.store_values(transformation_id, value)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def get_filter_greater(project_id,file_id,column,value):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                    from pyspark.sql.functions import *
                                    result = {}
                                    df = df.filter(col('"""+column+"""') > """+value+""")    
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'FILTERED DATA', column,
                                          constants.filter_greater)
        db.store_values(transformation_id, value)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def get_filter_greater_equal(project_id,file_id,column,value):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                            from pyspark.sql.functions import *
                                            result = {}
                                            df = df.filter(col('""" + column + """') >= """ + value + """)    
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'FILTERED DATA', column,
                                          constants.filter_greater_equal)
        db.store_values(transformation_id, value)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response



def get_filter_lesser(project_id,file_id,column,value):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                    from pyspark.sql.functions import *
                                                    result = {}
                                                    df = df.filter(col('""" + column + """') < """ + value + """)    
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'FILTERED DATA', column,
                                          constants.filter_lesser)
        db.store_values(transformation_id, value)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def get_filter_lesser_equal(project_id,file_id,column,value):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                from pyspark.sql.functions import *
                                                result = {}
                                                df = df.filter(col('""" + column + """') <= """ + value + """)    
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'FILTERED DATA', column,
                                          constants.filter_lesser_equal)
        db.store_values(transformation_id, value)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response




def get_filter_between(project_id,file_id,column,start_value,end_value):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                from pyspark.sql.functions import *
                                                result = {}
                                                df = df.where(col('""" + column + """').between("""+start_value+""","""+end_value+"""))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        logger.error(json.loads(resp['output']['data']['text/plain']))
        transformation_id = db.save_rules(project_id, file_id, '', 'FILTERED DATA', column,
                                          constants.filter_between)
        db.store_values(transformation_id, str(start_value) + constants.seperator + str(end_value))
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def get_filter_null(project_id,file_id,column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                    from pyspark.sql.functions import *
                                                    result = {}
                                                    df = df.where(col('""" + column + """').isNull())
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        db.save_rules(project_id, file_id, '', 'FILTERED DATA', column, constants.filter_null)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def get_filter_not_null(project_id,file_id,column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                        from pyspark.sql.functions import *
                                                        result = {}
                                                        df = df.where(col('""" + column + """').isNotNull())
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        db.save_rules(project_id, file_id, '', 'FILTERED DATA', column, constants.filter_not_null)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def get_filter_contains(project_id,file_id,column,value):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        value = "%" + value + "%"
        data = {
            'code': textwrap.dedent("""
                                                        from pyspark.sql.functions import *
                                                        result = {}
                                                        df=df.filter(col('""" + column + """').like('"""+value+"""')
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'FILTERED DATA', column,
                                          constants.filter_contains)
        db.store_values(transformation_id, value)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def update_null_constant(project_id,file_id,column,value):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                    from pyspark.sql.functions import *
                                                    result = {}
                                                    df=df.withColumn('"""+column+"""',when(df."""+column+""".isNull(),""" + value + """))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'UPDATED NULL VALUE WITH ' + str(value),
                                          column, constants.update_null)
        db.store_values(transformation_id, value)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def update_negative_constant(project_id,file_id,column,value):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                    from pyspark.sql.functions import *
                                                    result = {}
                                                    df=df.withColumn('"""+column+"""',when(df."""+column+""" < 0,""" + value + """))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'UPDATED NEGATIVE VALUE WITH ' + str(value),
                                          column, constants.update_negative)
        db.store_values(transformation_id, str(value))


    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def update_zero_constant(project_id,file_id,column,value):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                    from pyspark.sql.functions import *
                                                    result = {}
                                                    df=df.withColumn('"""+column+"""',when(df."""+column+""" == 0,""" + value + """))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'UPDATED ZERO VALUE WITH ' + str(value),
                                          column, constants.update_zero)
        db.store_values(transformation_id, value)

    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response



def update_negative_mean(project_id,file_id,column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                    from pyspark.sql.functions import *
                                                    result = {}
                                                    value=df.describe('""" + column + """').collect()[1][1]
                                                    df=df.withColumn('"""+column+"""',when(df."""+column+""" < 0,""" + value + """))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'UPDATED NEGATIVE VALUE WITH ' + str(value),
                                          column, constants.update_negative)
        db.store_values(transformation_id, str(value))
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response




def update_negative_median(project_id,file_id,column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                    from pyspark.sql.functions import *
                                                    result = {}
                                                    value=df.describe('""" + column + """').collect()[1][1]
                                                    df=df.withColumn('"""+column+"""',when(df."""+column+""" < 0,""" + value + """))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'UPDATED NEGATIVE VALUE WITH ' + str(value),
                                          column, constants.update_negative)
        db.store_values(transformation_id, str(value))
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response





def update_zero_mean(project_id,file_id,column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                    from pyspark.sql.functions import *
                                                    result = {}
                                                    value=df.describe('""" + column + """').collect()[1][1]
                                                    df=df.withColumn('"""+column+"""',when(df."""+column+""" == 0,""" + value + """))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'UPDATED ZERO VALUE WITH ' + str(value),
                                          column, constants.update_negative)
        db.store_values(transformation_id, str(value))
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response


def update_zero_median(project_id,file_id,column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                    from pyspark.sql.functions import *
                                                    result = {}
                                                    value=df.describe('""" + column + """').collect()[1][1]
                                                    df=df.withColumn('"""+column+"""',when(df."""+column+""" == 0,""" + value + """))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'UPDATED ZERO VALUE WITH ' + str(value),
                                          column, constants.update_negative)
        db.store_values(transformation_id, str(value))
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def update_null_mean(project_id,file_id,column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                    from pyspark.sql.functions import *
                                                    result = {}
                                                    value=df.describe('""" + column + """').collect()[1][1]
                                                    df=df.withColumn('"""+column+"""',when(df."""+column+""".isNull(),""" + value + """))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'UPDATED NULL VALUE WITH ' + str(value),
                                          column, constants.update_negative)
        db.store_values(transformation_id, str(value))
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response

def update_null_median(project_id,file_id,column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                    from pyspark.sql.functions import *
                                    result = {}                                
                                    value=df.describe('""" + column + """').collect()[1][1]
                                    df=df.withColumn('"""+column+"""',when(df."""+column+""".isNull(),""" + value + """))
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        transformation_id = db.save_rules(project_id, file_id, '', 'UPDATED NULL VALUE WITH ' + str(value),
                                          column, constants.update_null)
        db.store_values(transformation_id, value)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response



def delete_negative(project_id,file_id,column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                    from pyspark.sql.functions import *
                                                    result = {}
                                                    df = df.filter(col('""" + column + """') < 0)    
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        db.save_rules(project_id, file_id, '', 'DELETED NEGATIVE', column, constants.delete_negative)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response




def delete_null(project_id,file_id,column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                                        from pyspark.sql.functions import *
                                                        result = {}
                                                        df = df.where(col('""" + column + """').isNotNull())
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        db.save_rules(project_id, file_id, '', 'DELETED NULL', column, constants.delete_null)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response


def delete_zero(project_id,file_id,column):
    response = {}
    try:
        load_key = constants.getKeyForRedis(project_id, file_id, 0)
        session_id = redis.read_key_value(redis_connection, load_key)
        data = {
            'code': textwrap.dedent("""
                                    from pyspark.sql.functions import *
                                    result = {}                                
                                    df=df.where(col('""" + column + """')!=0) 
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
        response['message'] = "success"
        response['response'] = json.loads(resp['output']['data']['text/plain'])
        db.save_rules(project_id, file_id, '', 'DELETED ZERO', column, constants.delete_zero)
    except Exception:
        logger.error(traceback.format_exc())
        response['message'] = "Internal Server Error"
    return response







