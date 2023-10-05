import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,FloatType,IntegerType,StringType
from pyspark.sql.functions import from_json,col

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")



#if __name__ == '__main__':

def spark_process():

    spark = SparkSession \
     .builder \
     .appName("SparkStreamingDemotoCassandra") \
     .config("spark.cassandra.connection.host", "cassandra") \
     .config("spark.cassandra.connection.port","9042")\
     .config("spark.cassandra.auth.username", "cassandra") \
     .config("spark.cassandra.auth.password", "cassandra") \
     .getOrCreate() 


    schema =  StructType([
                StructField("name",StringType(),False),
                StructField("company",StringType(),False),
                StructField("remote_ip",StringType(),False),
                StructField("user_agent",StringType(),False),
                StructField("date",StringType(),False)
            ])
    
    raw_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "{your_ip}:9092") \
        .option("subscribe", "fake_data") \
        .option("startingOffsets", "earliest") \
        .load()
    

    df_final = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
                    .select("data.remote_ip","data.name", "data.company","data.user_agent","data.date")

    
    query = df_final.writeStream\
            .format("org.apache.spark.sql.cassandra") \
            .outputMode("append") \
            .option('table','fake_person_table') \
            .option('keyspace','spark_streaming') \
            .option("checkpointLocation", '/opt/bitnami/spark/check_point') \
            .start()

    query.awaitTermination()


#    def write_to_cassandra(df, epoch_id):
#        df.write \
#            .format("org.apache.spark.sql.cassandra") \
#            .mode("append") \
#            .option("keyspace", "spark_streaming") \
#            .option("table", "fake_person_table") \
#            .save()
#
#    query = df_final \
#        .writeStream \
#        .foreachBatch(write_to_cassandra) \
#        .start()
#
#    query.awaitTermination()


if __name__ == '__main__':
    spark_process()

   
