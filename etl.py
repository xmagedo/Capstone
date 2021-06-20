import pandas as pd 

from pyspark.sql import SparkSession
spark = SparkSession.builder.\
config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
.getOrCreate()

from pyspark.sql.functions import first
from pyspark.sql.functions import upper, col
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType
from pyspark.sql.functions import udf, date_trunc, date_format
import datetime as dt

# Reading dataset file of demographics cities 
sparkUS = spark.read.csv("us-cities-demographics.csv", sep=';',header=True)

sparkUS.printSchema()

sparkUS.count()

# Creating dataset with US cities as demo
USARaceContenint=(sparkUS.select("city","state code","Race","count")
    .groupby(sparkUS.City, "state code")
    .pivot("Race")
    .agg(first("Count")))

cols = ["VetNum", "Count", "Race"]

# Dropping columns to return new data without the duplicates columns

USA = sparkUS.drop(*cols).dropDuplicates()


# storing and joinging SPARKUS dataset
USA = USA.join(USARaceContenint, ['City','state code'])


# changing columns names to avoid conflict and complication in parquet
USA=USA.select('City', col('State Code').alias('State_Code'), 'State', col('Median Age').alias('Median_age'),
     col('Male Population').alias('Male_Pop'), col('Female Population').alias('Fem_Pop'), 
        col('Total Population').alias('Ttl_Pop'), 'Foreign-born', 
          col('Average Household Size').alias('Avg_Household_Size'),
             col('American Indian and Alaska Native').alias('Native_Pop'), 
                 col('Asian').alias('Asian_Pop'), 
                    col('Black or African-American').alias('Black_Pop'), 
                      col('Hispanic or Latino').alias('Latino_Pop'), 
                        col('White').alias('White_Pop'))



# dropping column state
USA = USA.drop("state")


# at this moment we write USA dataset in parquest file processing we do that because Parquet is a columnar format that is supported by many other data processing systems. Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema of the original data.
USA.write.mode('overwrite').parquet("us_cities_demographics.parquet")

I94VisaData = [[1, 'Business'], [2, 'Pleasure'], [3, 'Student']] 

# we convert I94VisaData to spark dataframe in order to read it into parquet file
I94VisaD = spark.createDataFrame(I94VisaData)

# creating parquestfile after converting
I94VisaD.write.mode('overwrite').parquet('./data/I94VisaD.parquet')

#Reading Text File 
I94Res = pd.read_csv('./data/I94rescit_I94RES.txt',sep='=',names=['id','country'])

# We are creating schema for DF and call it I94Res_schema
I94Res_schema = StructType([
    StructField('id', StringType(), True),
    StructField('country', StringType(), True)
])

# Removing white spaces 
I94Res['country']=I94Res['country'].str.replace("'",'').str.strip()

# we are convering pandas to list here so objects will change to string with single qoutes
I94RESData=I94Res.values.tolist()

I94Res=spark.createDataFrame(I94RESData, I94Res_schema)

# creating parquest file 
I94Res.write.mode('overwrite').parquet('./data/i94res.parquet')

# List
I94ModeListData = [[1,'Air'],[2,'Sea'],[3,'Land'],[9,'unreported']]


# we convert I94ModeListData to spark dataframe in order to read it into parquet file

I94Mod = spark.createDataFrame(I94ModeListData)

# we convert I94ModeListData to spark dataframe in order to read it into parquet file
I94Mod.write.mode("overwrite").parquet('./data/I94Mod.parquet')

# Reading I94Port txt file
I94PortDataF = pd.read_csv('./data/I94Port.txt',sep='=',names=['id','port'])

#def toCastType(df, cols):
#    for k,v in cols.items():
#        if k in df.columns:
#            df = df.withColumn(k, df[k].cast(v))
#    return df


#I94ResTxT = toCastType(res, {"Code": IntegerType()})
#I94ResTxT = I94ResTxT.withColumn('resCountry_Lower', lower(res.I94CTRY))


#Removing white spaces
I94PortDataF['id']=I94PortDataF['id'].str.strip().str.replace("'",'')


# Create two columns from i94port string: port_city and port_addr
# also remove whitespaces and single quotes
I94PortDataF['port_city'], I94PortDataF['port_state']=I94PortDataF['port'].str.strip().str.replace("'",'').str.strip().str.split(',',1).str


I94PortDataF['port_state']=I94PortDataF['port_state'].str.strip()

I94PortDataF.drop(columns =['port'], inplace = True)



# Converting Dataframe to list so changes to removing white spaces will be implmented in the list and become string
I94PortData=I94PortDataF.values.tolist()


# Creating sehcma
I94Portschema = StructType([
    StructField('id', StringType(), True),
    StructField('port_city', StringType(), True),
    StructField('port_state', StringType(), True)
])


i94port=spark.createDataFrame(I94PortDataF, I94Portschema)


i94port.write.mode('overwrite').parquet('./data/i94port.parquet')


# Read i94 non-immigration dataset
I94Spark=spark.read.parquet("sas_data")


# Numeric to Longtype and integerType
I94Spark=I94Spark.select(col("i94res").cast(IntegerType()),col("i94port"), 
                           col("arrdate").cast(IntegerType()),
                           col("i94mode").cast(IntegerType()),col("depdate").cast(IntegerType()),
                           col("i94bir").cast(IntegerType()),col("i94visa").cast(IntegerType()), 
                           col("count").cast(IntegerType()),
                              "gender",col("admnum").cast(LongType()))



I94Spark.printSchema()



#Dropping Duplicates
I94Spark=I94Spark.dropDuplicates()



# adding i94port columns
I94Spark=I94Spark.join(i94port, I94Spark.i94port==i94port.id, how='left')


I94Spark=I94Spark.drop("id")


# merging USA with I94spark to i94non_immigrant_port_entry fact table
I94nonImmigrantPortEntry=I94Spark.join(USA, (upper(I94Spark.port_city)==upper(USA.City)) & \
                                           (upper(I94Spark.port_state)==upper(USA.State_Code)), how='left')


I94nonImmigrantPortEntry=I94nonImmigrantPortEntry.drop("City","State_Code")


# Convert SAS arrival date to datetime format
get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
I94nonImmigrantPortEntry = I94nonImmigrantPortEntry.withColumn("arrival_date", get_date(I94nonImmigrantPortEntry.arrdate))


I94Date=I94nonImmigrantPortEntry.select(col('arrdate').alias('arrival_sasdate'),
                                   col('arrival_date').alias('arrival_iso_date'),
                                   date_format('arrival_date','M').alias('arrival_month'),
                                   date_format('arrival_date','E').alias('arrival_dayofweek'), 
                                   date_format('arrival_date', 'y').alias('arrival_year'), 
                                   date_format('arrival_date', 'd').alias('arrival_day'),
                                  date_format('arrival_date','w').alias('arrival_weekofyear')).dropDuplicates()


# Save to parquet file
I94nonImmigrantPortEntry.drop('arrival_date').write.mode("overwrite").parquet('./data/I94nonimmigrantportentry.parquet')


# Here we are adding season to dimention table to group the data in the database when the business creates reports
I94Date.createOrReplaceTempView("I94Date_table")
I94DateSeason=spark.sql('''select arrival_sasdate,
                         arrival_iso_date,
                         arrival_month,
                         arrival_dayofweek,
                         arrival_year,
                         arrival_day,
                         arrival_weekofyear,
                         CASE WHEN arrival_month IN (12, 1, 2) THEN 'winter' 
                                WHEN arrival_month IN (3, 4, 5) THEN 'spring' 
                                WHEN arrival_month IN (6, 7, 8) THEN 'summer' 
                                ELSE 'autumn' 
                         END AS date_season from I94Date_table''')


# Here we are saving I94Date dimension to parquet file 
I94DateSeason.write.mode("overwrite").partitionBy("arrival_year", "arrival_month").parquet('./data/i94date.parquet')

