import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def transform(bucket, data):

    logger.info("Transforming Data.", data)
    
    with SparkSession.builder.appName("TrasformingData").getOrCreate() as spark:
        
        df = spark.read.csv(f"s3://{bucket}/data/input/{data}", inferSchema=True, header =True)

        df_2 = df.withColumn('is_police',\
            F.when(\
                F.lower(\
                    F.col('local_site_name'
                )).contains('police'),\
                F.lit(1)
            ).\
            otherwise(F.lit(0))
        )
        df_2.write.csv(f"s3a://{bucket}/data/output/resultado_{data}", mode="overwrite")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bucket', type=str,
        help="El bucket donde se lee y almacena el resultado.")
    parser.add_argument(
        '--data_uri', help="The URI where data is saved, typically from an S3 bucket.")
    args = parser.parse_args()

    transform(args.bucket, args.data_uri)
