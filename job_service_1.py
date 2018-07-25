import services.logger_config as config
import logging.config
import services.constants as constants
import database_service as db
import math
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StringType
from pysparkd.sql.functions import *
from pyspark.ml.feature import Bucketizer
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf

logging.config.dictConfig(config.logger_config)
logger = logging.getLogger()

def perform_operation(df,transformation_id,operation_id,column):
    if operation_id == constants.upper_case:
        return perform_uppercase(df,column)
    elif operation_id == constants.upper_case_feature:
        return perform_uppercase_feature(df,column,transformation_id)
    elif operation_id == constants.lower_case_feature:
        return perform_lowercase_feature(df,column,transformation_id)
    elif operation_id == constants.feature_concat:
        return perform_concat_feature(df,column,transformation_id)
    elif operation_id == constants.feature_substr:
        return perform_substring_feature(df,column,transformation_id)
    elif operation_id == constants.lower_case:
        return perform_lowercase(df,column)
    elif operation_id == constants.ascending:
        return perform_ascending(df,column)
    elif operation_id == constants.descending:
        return perform_descending(df,column)
    elif operation_id == constants.change_date:
        return perform_change_date(df,column,transformation_id)
    elif operation_id == constants.trailing_space:
        return  perform_trailspace(df,column)
    elif operation_id == constants.to_string:
        return perform_tostring(df,column)
    elif operation_id == constants.to_integer:
        return  perform_tointeger(df,column)
    elif operation_id == constants.delete:
        return  perform_delete_column(df,column)
    elif operation_id == constants.rename:
        return  perform_rename(df,column,transformation_id)
    elif operation_id == constants.replace_string:
        return perform_replace_string(df,column,transformation_id)
    elif operation_id == constants.replace_string_constant:
        return  perform_replace_string_feature(df,column,transformation_id)
    elif operation_id == constants.categorical:
        return perform_categorical(df,column,transformation_id)
    elif operation_id == constants.split:
        return perform_split(df,column,transformation_id)
    elif operation_id == constants.bining:
        return perform_bining(df,column,transformation_id)
    elif operation_id == constants.per_bining:
        return perform_bining_percent(df,column,transformation_id)
    elif operation_id == constants.replace_int_constant:
        return perform_replace_int(df,column,transformation_id)
    elif operation_id == constants.feature_log:
        return perform_feature_log(df,column,transformation_id)
    elif operation_id == constants.feature_sum:
        return perform_feature_sum(df,column,transformation_id)
    elif operation_id == constants.feature_sub:
        return perform_feature_sub(df,column,transformation_id)
    elif operation_id == constants.feature_mul:
        return perform_feature_mul(df,column,transformation_id)
    elif operation_id == constants.feature_div:
        return perform_feature_div(df,column,transformation_id)
    elif operation_id == constants.quantile:
        return perform_quantile(df,column,transformation_id)
    elif operation_id == constants.scaling_normal:
        return perform_normal_scaling(df,column,transformation_id)
    elif operation_id == constants.scaling_minmax:
        return perform_minmax_scaling(df,column,transformation_id)
    elif operation_id == constants.scaling_robust:
        return perform_robust_scaling(df,column,transformation_id)
    elif operation_id == constants.scaling_log:
        return perform_log_scaling(df,column,transformation_id)
    elif operation_id == constants.update_null:
        return perform_update_null(df,column,transformation_id)
    elif operation_id == constants.update_negative:
        return perform_update_negative(df,column,transformation_id)
    elif operation_id == constants.update_zero:
        return perform_update_zero(df,column,transformation_id)
    elif operation_id == constants.delete_negative:
        return perform_delete_negative(df,column)
    elif operation_id == constants.delete_zero:
        return perform_delete_zero(df, column)
    elif operation_id == constants.delete_null:
        return perform_delete_null(df, column)
    elif operation_id == constants.filter_equal:
        return perform_filter_equal(df,column,transformation_id)
    elif operation_id == constants.filter_not_equal:
        return perform_filter_not_equal(df,column,transformation_id)
    elif operation_id == constants.filter_greater:
        return perform_filter_greater(df,column,transformation_id)
    elif operation_id == constants.filter_greater_equal:
        return  perform_filter_greater_equal(df,column,transformation_id)
    elif operation_id == constants.filter_between:
        return  perform_filter_between(df,column,transformation_id)
    elif operation_id == constants.filter_null:
        return perform_filter_null(df,column)
    elif operation_id == constants.filter_not_null:
        return perform_filter_not_null(df,column)
    elif operation_id == constants.filter_contains:
        return perform_filter_contains(df,column,transformation_id)
    elif operation_id == constants.delete_mul:
        return perform_multiple_delete(df,transformation_id)

def perform_multiple_delete(df,transformation_id):
    try:
        column_list = db.get_values(transformation_id)
        column_list = column_list.split('##')
        for colname in column_list:
            df = df.drop(colname)
    except Exception:
        print
    return df

def perform_uppercase(df,column):
    try:
        df = df.withColumn(column, upper(col(column)))
    except Exception:
        print
    return df

def perform_uppercase_feature(df,column,transformation_id):
    try:
        value = db.get_values(transformation_id)
        df = df.withColumn(value, upper(col(column)))
    except Exception:
        print
    return df

def perform_lowercase(df,column):
    try:
        df = df.withColumn(column, lower(col(column)))
    except Exception:
        print
    return df

def perform_normal_scaling(df,column,transformation_id):
    try:
        new_column = db.get_values(transformation_id)
        df = df.where(col(column).isNotNull())
        age_assembler = VectorAssembler(inputCols=[column], outputCol="age_feature")
        scaler = StandardScaler(inputCol="age_feature", outputCol=new_column)
        age_pipeline = Pipeline(stages=[age_assembler, scaler])
        scalerModel = age_pipeline.fit(df)
        df = scalerModel.transform(df)
        df = df.drop("age_feature")
        firstelement = udf(lambda v: float(v[0]), FloatType())
        df = df.withColumn(new_column, round(firstelement(new_column), 4))
    except Exception:
        print
    return df

def perform_lowercase_feature(df,column,transformation_id):
    try:
        value = db.get_values(transformation_id)
        df = df.withColumn(value, lower(col(column)))
    except Exception:
        print
    return df

def perform_concat_feature(df,transformation_id):
    try:
        value_list = db.get_values(transformation_id)
        value_list = value_list.split(constants.seperator)
        column_list = value_list[0]
        column_list = column_list.split('##')
        new_column = value_list[1]
        size = len(column_list)
        mergedString = (df[column_list[0]]).cast("string")
        for i in range(1, size):
            mergedString = concat(mergedString, (df[column_list[i]]).cast("string"))
        df = df.withColumn(new_column, mergedString)
    except Exception:
        print
    return df

def perform_substring_feature(df,column,transformation_id):
    try:
        value_list = db.get_values(transformation_id)
        value_list = value_list.split(constants.seperator)
        start = value_list[0]
        length = value_list[1]
        new_column = value_list[2]
        df = df.withColumn(new_column,
                           substring(col(column), start, length))

    except Exception:
        print
    return df

def perform_ascending(df,column):
    try:
        df = df.sort(asc(column))
    except Exception:
        print
    return df

def perform_descending(df,column):
    try:
        df = df.sort(desc(column))
    except Exception:
        print
    return df


def perform_trailspace(df,column):
    try:
        df = df.withColumn(column, trim(col(column)))
    except Exception:
        print
    return df

def perform_tostring(df,column):
    try:
        df = df.withColumn(column, col(column).cast("string"))
    except Exception:
        print
    return df

def perform_tointeger(df,column):
    try:
        df = df.withColumn(column, col(column).cast(IntegerType()))
    except Exception:
        print
    return df

def perform_delete_column(df,column):
    try:
        df = df.drop(column)
    except Exception:
        print
    return df

def perform_rename(df,column,transformation_id):
    try:
        new_column = db.get_values(transformation_id)
        df = df.withColumnRenamed(column,new_column)
    except Exception:
        print
    return df

def perform_replace_string(df,column,transformation_id):
    try:
        value_list = db.get_values(transformation_id)
        value_list = value_list.split(constants.seperator)
        search_text = value_list[0]
        replace_text = value_list[1]
        df = df.withColumn(column,
                           regexp_replace(column, search_text, replace_text))

    except Exception:
        print
    return df

def perform_replace_string_feature(df,column,transformation_id):
    try:
        value_list = db.get_values(transformation_id)
        value_list = value_list.split(constants.seperator)
        search_text = value_list[0]
        replace_text = value_list[1]
        new_column = value_list[2]
        df = df.withColumn(new_column,
                           regexp_replace(column, search_text, replace_text))

    except Exception:
        print
    return df

def perform_merge_column(df,column,transformation_id):
    try:
        value_list = db.get_values(transformation_id)
        value_list = value_list.split(constants.seperator)
        column_list = value_list[0]
        column_list = column_list.split('##')
        new_column = value_list[1]
        seperator = value_list[2]
        size = len(column_list)
        mergedString = (df[column_list[0]]).cast("string")
        for i in range(1, size):
            mergedString = concat(mergedString, lit(seperator), (df[column_list[i]]).cast("string"))
        df = df.withColumn(new_column, mergedString)
    except Exception:
        print
    return df

def perform_split(df,column,transformation_id):
    try:
        delimiter = db.get_values(transformation_id)
        maxcolSize = (df.withColumn("colsize", (size(split(column, delimiter)))).select(
            max("colsize"))).collect()[0][0]
        for i in range(0, maxcolSize):
            df = df.withColumn("column_" + str(i), split(column, delimiter).getItem(i))
    except Exception:
        print
    return df

def perform_bining(df,column,transformation_id):
    try:
        data_list = db.get_values(transformation_id)
        data_list = data_list.split(constants.seperator)
        start_list = data_list[0]
        end_list = data_list[1]
        label_list = data_list[2]
        new_column = data_list[3]
        startlist = start_list.split('##')
        endlist = end_list.split('##')
        labellist = label_list.split('##')
        size = len(endlist)
        new_list = []
        for item in startlist:
            new_list.append(float(item))
        new_list.append(float(endlist[size - 1]))
        df = Bucketizer(splits=new_list, inputCol=column, outputCol=new_column).transform(df)
        label = udf(lambda v: (labellist[int(v)] if v is not None else v))
        df = df.withColumn(new_column, label(new_column))
    except Exception:
        print
    return df

def perform_bining_percent(df,column,transformation_id):
    try:
        data_list = db.get_values(transformation_id)
        data_list = data_list.split(constants.seperator)
        start_list = data_list[0]
        end_list = data_list[1]
        label_list = data_list[2]
        new_column = data_list[3]
        startlist = start_list.split('##')
        endlist = end_list.split('##')
        labellist = label_list.split('##')
        size = len(endlist)
        new_list = []
        for item in startlist:
            new_list.append(float(item))
        new_list.append(float(endlist[size - 1]))
        df = Bucketizer(splits=new_list, inputCol=column, outputCol=new_column).transform(df)
        label = udf(lambda v: (labellist[int(v)] if v is not None else v))
        df = df.withColumn(new_column, label(new_column))
    except Exception:
        print
    return df

def perform_replace_int(df,column,transformation_id):
    try:
        value_list = db.get_values(transformation_id)
        value_list = value_list.split(constants.seperator)
        search_int = value_list[0]
        replace_int = value_list[1]
        new_column = value_list[2]
        if search_int == "null":
            replacenull = udf((lambda x: (x if x is not None else replace_int)))
            df = df.withColumn(new_column, replacenull(column))
        else:
            replaceint = udf(lambda x: (x if x != int(search_int) else replace_int))
            df = df.withColumn(new_column, replaceint(column))
    except Exception:
        print
    return df

def perform_feature_log(df,column,transformation_id):
    try:
        new_column = db.get_values(transformation_id)
        df = df.withColumn(new_column, round(log(col(column)), 4))
    except Exception:
        print
    return df

def perform_feature_sum(df,column,transformation_id):
    try:
        value_list = db.get_values(transformation_id)
        value_list = value_list.split(constants.seperator)
        column_list = value_list[0]
        column_list = column_list.split('##')
        new_column = value_list[1]
        size = len(column_list)
        totalSum = df[column_list[0]]
        for i in range(1, size):
            totalSum = totalSum + df[column_list[i]]
        df = df.withColumn(new_column, totalSum)
    except Exception:
        print
    return df

def perform_feature_sub(df,column,transformation_id):
    try:
        value_list = db.get_values(transformation_id)
        value_list = value_list.split(constants.seperator)
        column_list = value_list[0]
        column_list = column_list.split('##')
        new_column = value_list[1]
        size = len(column_list)
        total = df[column_list[0]]
        for i in range(1, size):
            total = total - df[column_list[i]]
        df = df.withColumn(new_column, total)
    except Exception:
        print
    return df

def perform_feature_mul(df,column,transformation_id):
    try:
        value_list = db.get_values(transformation_id)
        value_list = value_list.split(constants.seperator)
        column_list = value_list[0]
        column_list = column_list.split('##')
        new_column = value_list[1]
        size = len(column_list)
        total = df[column_list[0]]
        for i in range(1, size):
            total = total * df[column_list[i]]
        df = df.withColumn(new_column, total)
    except Exception:
        print
    return df

def perform_feature_div(df,column,transformation_id):
    try:
        value_list = db.get_values(transformation_id)
        value_list = value_list.split(constants.seperator)
        column_list = value_list[0]
        column_list = column_list.split('##')
        new_column = value_list[1]
        size = len(column_list)
        total = df[column_list[0]]
        for i in range(1, size):
            total = total / df[column_list[i]]
        df = df.withColumn(new_column, total)
    except Exception:
        print
    return df

def perform_quantile(df,column,transformation_id):
    try:
        new_column = db.get_values(transformation_id)
        df = QuantileDiscretizer(numBuckets=4, inputCol=column, outputCol=new_column).fit(df).transform(df)
    except Exception:
        print
    return df

def perform_minmax_scaling(df,column,transformation_id):
    try:
        new_column = db.get_values(transformation_id)
        df = df.where(col(column).isNotNull())
        age_assembler = VectorAssembler(inputCols=[column], outputCol="age_feature")
        scaler = MinMaxScaler(inputCol="age_feature", outputCol=new_column)
        age_pipeline = Pipeline(stages=[age_assembler, scaler])
        scalerModel = age_pipeline.fit(df)
        df = scalerModel.transform(df)
        df = df.drop("age_feature")
        firstelement = udf(lambda v: float(v[0]), FloatType())
        df = df.withColumn(new_column, round(firstelement(new_column), 4))
    except Exception:
        print
    return df

def perform_robust_scaling(df,column,transformation_id):
    try:
        new_column = db.get_values(transformation_id)
        data_list = df[column].tolist()
        data_list = [x if x == x else 0 for x in data_list]
        data_list = np.array(data_list)
        data_list = data_list.reshape(-1, 1)
        data_list = RobustScaler().fit_transform(data_list)
        response = []
        for data1 in data_list:
            response.append(data1[0])
        data_list = [round(elem, 4) for elem in response]
        df[new_column] = data_list
    except Exception:
        print
    return df

def perform_log_scaling(df,column,transformation_id):
    try:
        new_column = db.get_values(transformation_id)
        df = df.withColumn(new_column, round(log(col(column)), 4))
    except Exception:
        print
    return df

def perform_update_null(df,column,transformation_id):
    try:
        value = db.get_values(transformation_id)
        df = df.withColumn(column,when(df[column]
        .isNull(),value))

    except Exception:
        print
    return df

def perform_update_negative(df,column,transformation_id):
    try:
        value = db.get_values(transformation_id)
        df = df.withColumn(column, when(df[column]
         < 0,value))

    except Exception:
        print
    return df

def perform_update_zero(df,column,transformation_id):
    try:
        value = db.get_values(transformation_id)
        df = df.withColumn(column, when(df[column]== 0,value))

    except Exception:
        print
    return df

def perform_delete_negative(df,column):
    try:
        df = df.filter(col(column) < 0)
    except Exception:
        print
    return df

def perform_delete_null(df,column):
    try:
        df = df.where(col(column).isNotNull())
    except Exception:
        print
    return df

def perform_delete_zero(df,column):
    try:
        df = df.where(col(column) != 0)
    except Exception:
        print
    return df

def perform_filter_equal(df,column,transformation_id):
    try:
        value = db.get_values(transformation_id)
        value_list = []
        try:
            value_list.append(int(value))
        except ValueError:
            value_list.append(value)
        df = df.filter(col(column).isin(value_list))
    except Exception:
        print
    return df

def perform_filter_not_equal(df,column,transformation_id):
    try:
        value = db.get_values(transformation_id)
        try:
            value = int(value)
        except ValueError:
            value = value
        df = df.where(col(column) != value)
    except Exception:
        print
    return df

def perform_filter_greater(df,column,transformation_id):
    try:
        value = db.get_values(transformation_id)
        df = df.filter(col(column) > value)

    except Exception:
        print
    return df

def perform_filter_greater_equal(df,column,transformation_id):
    try:
        value = db.get_values(transformation_id)
        df = df.filter(col(column) >= value)

    except Exception:
        print
    return df

def perform_filter_between(df,column,transformation_id):
    try:
        value = db.get_values(transformation_id)
        value = value.split(constants.seperator)
        start_value = value[0]
        end_value = value[1]
        df = df.where(col(column).between(start_value,end_value))
    except Exception:
        print
    return df

def perform_filter_null(df,column):
    try:
        df = df.where(col(column).isNull())
    except Exception:
        print
    return df

def perform_filter_not_null(df,column):
    try:
        df = df.where(col(column).isNotNull())
    except Exception:
        print
    return df

def perform_filter_contains(df,column,transformation_id):
    try:
        value = db.get_values(transformation_id)
        df = df.filter(col(column).like(value))
    except Exception:
        print
    return df

def perform_deduplicate(df,column):
    try:
        df = df.drop_duplicates(subset=[column]).count()
    except Exception:
        print
    return df

def perform_round(df,column,digits):
    try:
        df = df.withColumn(column, round(col(column), digits))
    except Exception:
        print
    return df


def perform_sqrt(df,column):
    try:
        new_column = db.get_values(transformation_id)
        df = df.withColumn(new_column, round(sqrt(col(column)), 4))
    except Exception:
        print
    return df



def perform_square(df,column):
    try:
        def square_float(x):
            return float(x ** 2)

        new_column = db.get_values(transformation_id)
        square_udf_float2 = udf(lambda z: square_float(z), FloatType())
        df = df.withColumn(new_column, square_udf_float2((col(column))))
    except Exception:
        print
    return df


def perform_ceil(df,column):
    try:
        new_column = db.get_values(transformation_id)
        df = df.withColumn(new_column, ceil(col(column)))
    except Exception:
        print
    return df


def perform_floor(df,column):
    try:
        new_column = db.get_values(transformation_id)
        df = df.withColumn(new_column, floor(col(column)))
    except Exception:
        print
    return df

def perform_initcap(df,column):
    try:
        df = df.withColumn(new_column, initcap(col(column)))
    except Exception:
        print
    return df


def perform_startswith(df,column,transformation_id):
    try:
        value = db.get_values(transformation_id)
        df = df.filter(col(column).startswith(value))
    except Exception:
        print
    return df

def perform_endswith(df,column,transformation_id):
    try:
        value = db.get_values(transformation_id)
        df = df.filter(col(column).endswith(value))
    except Exception:
        print
    return df

def perform_extract_year(df,column,transformation_id):
    try:
        new_column = db.get_values(transformation_id)
        df = df.withColumn(new_column,year(col(column)))
    except Exception:
        print
    return df


def perform_extract_month(df,column,transformation_id):
    try:
        new_column = db.get_values(transformation_id)
        df = df.withColumn(new_column,month(col(column)))
    except Exception:
        print
    return df



def perform_extract_day(df,column,transformation_id):
    try:
        new_column = db.get_values(transformation_id)
        df = df.withColumn(new_column, dayofmonth(col(column)))
    except Exception:
        print
    return df


def perform_extract_hour(df,column,transformation_id):
 try:
     new_column=db.get_values(transformation_id)
     df = df.withColumn(
      new_column,hour(col(column)))
 except Exception:
        print
 return df

def perform_extract_minute(df,column,transformation_id):
    try:
        new_column = db.get_values(transformation_id)
        df = df.withColumn(new_column,minute(col(column)))
    except Exception:
        print
    return df


def perform_extract_second(df,column,transformation_id):
    try:
     new_column = db.get_values(transformation_id)
     df = df.withColumn(new_column, second(col(column)))
    except Exception:
        print
    return df


def perform_extract_day_of_week(df,column,transformation_id):
    try:
     new_column = db.get_values(transformation_id)
     df = df.withColumn(new_column, dayofweek(col(column)))
    except Exception:
        print
    return df

def perform_date_diff(df,column,transformation_id):
    try:
        value_list = db.get_values(transformation_id)
        value_list = value_list.split(constants.seperator)
        new_column = value_list[0]
        column1 = value_list[1]
        df = df.withColumn(
            new_column, datediff(col(column), col(column1)))
    except Exception:
        print
    return df



def perform_weekday(df,column,transformation_id):
    try:
        new_column = db.get_values(transformation_id)
        df = df.withColumn(
            new_column, when((date_format(column, 'u') <= 5), True).otherwise(False))
    except Exception:
        print
    return df


def perform_abs(df,column,transformation_id):
    try:
        new_column = db.get_values(transformation_id)
        df = df.withColumn(new_column, abs(col(column)))
    except Exception:
        print
    return df

def perform_leap_year(df,column,transformation_id):
 new_column=db.get_values(transformation_id)
 def leap_year(year):
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
 leap_udf = udf(lambda z: leap_year(z), BooleanType())
 df = df.withColumn(new_column,leap_udf((col(column))))

 return df

def perform_change_date(df,column,transformation_id):
 format = db.get_values(transformation_id)
 df=df.withColumn("sdfd",to_date(col(column),format))
 df.show(4)
 return df


def perform_sortandpickfirstn(df,transformation_id):
    try:
        value_list = db.get_values(transformation_id)
        value_list = value_list.split(constants.seperator)
        column1 = value_list[0]
        column2 = value_list[1]
        n = value_list[2]
        window = Window.partitionBy(df[column1]).orderBy(df[column2].desc())
        df = df.select('*', rank().over(window).alias("rank")).filter(col("rank") <= n)
    except Exception:
        print
    return df

def perform_sortandpicklastn(df,transformation_id):
    try:
        value_list = db.get_values(transformation_id)
        value_list = value_list.split(constants.seperator)
        column1 = value_list[0]
        column2 = value_list[1]
        n = value_list[2]
        window = Window.partitionBy(df[column1]).orderBy(df[column2])
        df = df.select('*', rank().over(window).alias("rank")).filter(col("rank") <= n)
    except Exception:
        print
    return df


def perform_categorial(df,column,transformation_id):
    try:
     new_column = db.get_values(transformation_id)
     unique_list = df.select(column).distinct().rdd.map(lambda r: r[0]).collect()
     keys = {}
     index = 1
     for value in unique_list:
         if value is not None and value == value:
             keys[value] = index
             index = index + 1
     def categorize(x):
       return keys[x]

     categorize_udf= udf(lambda z: categorize(z), IntegerType())
     df = df.withColumn(new_column,categorize_udf((col(column))))
    except Exception:
        print
    return df


def perform_join(df, df2, column, left_column_list, right_column_list, method):
    key_list = []
    for i in range(0, len(left_column_list)):
        if left_column_list[i] == right_column_list[i]:
            key_list.append(left_column_list[i])
        else:
            df2 = df2.withColumnRenamed(right_column_list[i], left_column_list[i])
            key_list.append(right_column_list[i])
    df = df.join(df, on=key_list, how=method)
    return df




