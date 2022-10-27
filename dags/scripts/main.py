
from pyspark.sql import SparkSession
from decimal import Decimal
from pyspark.sql.types import DecimalType, StructType, StructField, StringType

def get_price(x3):
    try:
        UnitPrice = Decimal(x3[2])
        convert = UnitPrice * Decimal(x3[1])
    except Decimal.InvalidOperation:
           print("Invalid input")
    key = x3[0]
    price = convert

    return (key, price)

if __name__ == '__main__':    
#Create the spark session
    spark = SparkSession \
        .builder \
        .appName("reading csv") \
        .enableHiveSupport() \
        .getOrCreate()


#Read the csv into a dataframe
sales_data_df = spark.read.csv("s3a://vaishali-emr-s3-testing/data/sales-data.csv", header=True, sep=",").cache()
df1 = sales_data_df.select(sales_data_df["Country"],sales_data_df["UnitPrice"], sales_data_df["Quantity"]).repartition(10)

#Get UnitPrice*Quantity and Country wise sales
mapped_rdd = df1.rdd.map(lambda x: get_price(x)).reduceByKey(lambda a, b: (a+b))

#Name the columns 
schema = StructType([StructField("country", StringType()), StructField("total_sales", DecimalType(38,2))])
mapped_rdd.toDF(schema = schema).show(5)

#Save it to a file or files in HDFS
mapped_rdd.saveAsTextFile('s3a://vaishali-emr-s3-testing/output/')








