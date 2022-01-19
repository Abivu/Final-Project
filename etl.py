#Import all necessary modules/libraries
import configparser
from email import header
import logging
import os
# from time import monotonic
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, year, month, to_date
from pyspark.sql.types import DateType

# setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Set up AWS Configuration
config = configparser.ConfigParser()
config.read('AWS_Config.cfg', encoding='utf-8-sig') 

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

Source_S3_Bucket = config['S3']['Source_S3_Bucket']
Dest_S3_Bucket = config['S3']['Dest_S3_Bucket']


# Define functions to Use Spark to transform data
def create_spark_session():
    spark = SparkSession.builder\
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
            .enableHiveSupport().getOrCreate()
    return spark

def SAS_to_datetime(date):
    return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')
SAS_to_date_udf = udf(SAS_to_datetime, DateType())

def rename_columns(table, new_columns):
    for original, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(original, new)
    return table


def process_immigration_data(spark, input, output):
    ''' Process I94 Immigration dataset to generate fact table fact_immigration, and 2\
        dim tables: dim_airline and dim_person

        params: spark = spark object,\
                input = source s3 bucket,\
                output = destination s3 bucket
    '''
    logging.info("Start processing Immigration data")

    immi_data = os.path.join(input + 'immigration/18-83510-I94-Data-2016/*.sas7bdat')

    df = spark.read.format('com.github.saurfang.sas.spark').load(immi_data)

    logging.info('Create a table fact_immigration')

    fact_immigration = df.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94mode',\
                                 'i94addr', 'i94visa','arrdate', 'depdate' ).distinct()\
                                     .withColumn("immigration_id", monotonically_increasing_id())


    # Rename columns & Transform 2 cols of date from SAS to datetime 
    new_columns = ['cic_id', 'year', 'month', 'city_code', 'transportation','state_code',\
                     'visa', 'arrival_date', 'departure_date']

    fact_immigration = rename_columns(fact_immigration, new_columns)

    fact_immigration = fact_immigration.withColumn('arrival_date',\
                                        SAS_to_date_udf(col('arrival_date')))

    fact_immigration = fact_immigration.withColumn('departure_date',\
                                        SAS_to_date_udf(col('departure_date')))

    # Write fact_immigration table to parquet files and partitioned by State
    # Could have been done in a better way
    fact_immigration.write.mode("overwrite").partitionBy('state_code')\
                        .parquet(path=output + 'fact_immigration')


    logging.info("Create a table Dim_person")

    dim_person = df.select('cicid', 'i94cit', 'i94res', 'biryear', 'gender').distinct()\
                                    .withColumn("person_id", monotonically_increasing_id())

    # Rename columns 
    new_columns = ['cic_id', 'citizen_country', 'residence_country', 'birth_year', 'gender']
    dim_person = rename_columns(dim_person, new_columns)

    #Write dime_person to parquet files
    dim_person.write.mode("overwrite").parquet(path= output + "dim_person")

    logging.info("Create a table Dim_airline")

    dim_airline = df.select('cicid', 'airline', 'fltno', 'visatype').distinct()\
                                    .withColumn("airflight_id", monotonically_increasing_id())

    new_columns = ['cic_id', 'airline', 'flight_no', 'visa_type']
    dim_airline = rename_columns(dim_airline, new_columns)

    #Write dim_airline to parquet files
    dim_airline.write.mode("overwrite").parquet(path= output + "dim_airline")

def process_demography_data(spark, input, output):
    """ Parsing a csv us cities demographics file in order to create dim_city_demo

        Params: spark object, 
                input: Source S3 bucket - csv file, 
                output: Destination S3 Bucket
    """

    logging.info("Processing US cities demographics file csv format")

    file = os.path.join(input + 'us-cities-demographics.csv')
    df = spark.read.format('csv').options(header= True, delimiter = ';').load(file)

    dim_city_demo = df.select(['City', 'State Code','Median Age', 'Male Population',\
                         'Female Population', 'Foreign-born', 'Average Household Size']).distinct()\
                             .withColumn("demo_id", monotonically_increasing_id())

    new_columns = ['City', 'Code','Median Age', 'Males', 'Females',\
                                     'Foreign-born', 'Avg. Household Size']

    dim_city_demo = rename_columns(dim_city_demo, new_columns)

    dim_city_demo.write.mode("overwrite").parquet(path = output + "dim_city_demo")




def process_label_descriptions(spark, input, output):
    """ Parsing a file I94 SAS Labels description to create dim tables dim_country_code,
        dim_city_code, dim_state_code and dim_transport

        Params: spark, 
                input: Source S3 bucket - Parsing file, 
                output: Destination S3 Bucket
    """
    

    logging.info("Processing SAS Labels Description File")

    file = os.path.join(input + 'I94_SAS_Labels_Descriptions.SAS')
    with open(file) as f:
        contents = f.readlines()
    
    #dim_country_code
    country_code = {}
    for countries in contents[10:298]:
        pair = countries.split('=')
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        country_code[code] = country

    dim_country_code = spark.createDataFrame(country_code.items(), ['code', 'country'])
    dim_country_code.write.mode("overwrite").parquet(path= output + 'dim_country_code')

    #dim_city_code
    city_code = {}
    for cities in contents[303:962]:
        pair = cities.split('=')
        code, city = pair[0].strip().strip("''"), pair[1].strip().strip("' '")
        city_code[code] = city

    dim_city_code = spark.createDataFrame(city_code.items(), ['code', 'city'])
    dim_city_code.write.mode("overwrite").parquet(path= output + 'dim_city_code')

    #dim_state_code
    state_code = {}
    for states in contents[982:1036]:
        pair = states.split('=')
        code, state = pair[0].strip().strip("''"), pair[1].strip().strip("''")
        state_code[code] = state

    dim_state_code =  spark.createDataFrame(state_code.items(), ['code', 'state'])
    dim_state_code.write.mode("overwrite").parquet(path= output + 'dim_state_code')

    #dim_transport
    transport_mode = {}
    for transports in contents[973:976]:
        pair = transports.split('=')
        code, transport = pair[0].strip(), pair[1].strip()
        transport_mode[code] = transport

    dim_transport =  spark.createDataFrame(transport_mode.items(), ['code', 'transport'])
    dim_transport.write.mode("overwrite").parquet(path= output + 'dim_transport')




def main():
    spark = create_spark_session()
    input = Source_S3_Bucket
    output = Dest_S3_Bucket


    process_immigration_data(spark, input, output)
    process_demography_data(spark, input, output)
    process_label_descriptions(spark, input, output)
    logging.info("Data Processing Completed")


if __name__ == "__main__":
    main()
