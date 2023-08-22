import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark import SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
CATALOG = 'glue_catalog'
WAREHOUSE_PATH = 's3://PATH_TO_YOUR_ICEBERG_TABLE'

conf = (
    SparkConf().set(f'spark.sql.catalog.{CATALOG}', 'org.apache.iceberg.spark.SparkCatalog')
    .set(f'spark.sql.catalog.{CATALOG}.warehouse', f'{WAREHOUSE_PATH}')
    .set(f'spark.sql.catalog.{CATALOG}.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
    .set(f'spark.sql.catalog.{CATALOG}.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    .set('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .set('spark.sql.iceberg.handle-timestamp-without-timezone','true')
    .set('spark.sql.autoBroadcastJoinThreshold','-1')
    )

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# VACUUM operations
# spark.sql("CALL glue_catalog.system.remove_orphan_files(table => 'YOUR_DB.YOUR_TABLE')").show()
# spark.sql("CALL glue_catalog.system.expire_snapshots(table => 'YOUR_DB.YOUR_TABLE',retain_last => 1, max_concurrent_deletes => 20)").show()
# spark.sql("CALL glue_catalog.system.rewrite_manifests('YOUR_DB.YOUR_TABLE')").show()

# OPTIMIZE operations
# spark.sql("CALL glue_catalog.system.rewrite_data_files(table => 'YOUR_DB.YOUR_TABLE', options => map ('max-concurrent-file-group-rewrites','100','partial-progress.enabled','true','rewrite-all','true'))").show()

job.commit()