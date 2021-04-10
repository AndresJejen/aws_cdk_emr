import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def transform(bucket, data):

    
    with SparkSession.builder.appName("TrasformingData").getOrCreate() as spark:
        
        df = spark.read.csv(f"s3://{bucket}/data_in/{data}", inferSchema=True, header =True)

        df_2 = df.withColumn('is_police',\
            F.when(\
                F.lower(\
                    F.col('local_site_name'
                )).contains('police'),\
                F.lit(1)
            ).\
            otherwise(F.lit(0))
        ).select('is_police', 'local_site_name')

        print("Una pequeña muestra")
        print(df_2.show())

        df_2.write.csv(f"s3a://{bucket}/data_out/resultado_{data}", mode="overwrite", header=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bucket', type=str,
        help="El bucket donde se lee y almacena el resultado.")
    parser.add_argument(
        '--data_uri', help="The URI where data is saved, typically from an S3 bucket.")
    args = parser.parse_args()

    transform(args.bucket, args.data_uri)
