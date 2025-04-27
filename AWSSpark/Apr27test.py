import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['Apr27test'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['Apr27test'], args)
#reading data from csv
df=spark.read.format("csv").option("header", "True").option("path", "s3://first34bucket/test.txt").load()
#writing to csv in output folder
df=spark.write.format("csv").option("header", "True").option("path", "s3://first34bucket/output").save()
job.commit()