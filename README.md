### Iceberg adoption journey session:

https://aws-experience.com/emea/tel-aviv/e/bc0a3/apache-iceberg-on-aws---advanced-session

#### Basically the most important steps to perform are:

1. Register the Iceberg connector for AWS Glue
2. Create ETL Job or a Jupyter Notebook
3. Provide the necessary configuration (https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html) to the Spark job as detailed here such as: –datalake-formats and –conf 

NOTE: these actions automatically injects the Iceberg Spark SQL extension: https://iceberg.apache.org/docs/1.3.0/spark-configuration/#sql-extensions

Nice AWS blog (https://aws.amazon.com/blogs/big-data/use-the-aws-glue-connector-to-read-and-write-apache-iceberg-tables-with-acid-transactions-and-perform-time-travel/) and an AWS Glue Developer Guide (https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html) are available

