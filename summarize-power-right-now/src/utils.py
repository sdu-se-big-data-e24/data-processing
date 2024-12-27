import locale
import os
import re
import subprocess
from enum import Enum

from pyspark import SparkConf
from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

locale.getdefaultlocale()
locale.getpreferredencoding()

FS: str = "hdfs://namenode:9000"
# Get the IP address of the host machine.
SPARK_DRIVER_HOST = (
    subprocess.check_output(["hostname", "-i"]).decode(encoding="utf-8").strip()
)

SPARK_DRIVER_HOST = re.sub(rf"\s*127.0.0.1\s*", "", SPARK_DRIVER_HOST)
os.environ["SPARK_LOCAL_IP"] = SPARK_DRIVER_HOST
os.environ["SPARK_USER"] = "K8S job"

class SPARK_ENV(Enum):
    LOCAL = [
        ("spark.master", "local"),
        ("spark.driver.host", SPARK_DRIVER_HOST),
    ]
    K8S = [
        ("spark.master", "spark://spark-master-svc:7077"),
        ("spark.driver.bindAddress", "0.0.0.0"),
        ("spark.driver.host", SPARK_DRIVER_HOST),
        ("spark.driver.port", "7077"),
        ("spark.submit.deployMode", "client"),
        # Disable Hadoop security
        ("spark.kerberos.enabled", "false"),
        ("spark.hadoop.security.authentication", "simple"),
        ("spark.hadoop.security.authorization", "false"),
        # Set HDFS settings
        ("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000"),
        ("spark.hadoop.dfs.client.use.datanode.hostname", "true"),
    ]

def get_spark_context(app_name: str, config: SPARK_ENV) -> SparkSession:
    """Get a Spark context with the given configuration."""
    spark_conf = SparkConf().setAll(config.value).setAppName(app_name)
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    sc = spark.sparkContext

    # sc.setLogLevel("DEBUG")

    # Set the user programmatically
    java_import(sc._gateway.jvm, "org.apache.hadoop.security.UserGroupInformation")
    sc._gateway.jvm.UserGroupInformation.setLoginUser(sc._gateway.jvm.UserGroupInformation.createRemoteUser("root"))

    return spark