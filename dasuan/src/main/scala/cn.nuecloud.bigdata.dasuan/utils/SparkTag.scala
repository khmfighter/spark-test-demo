package cn.nuecloud.bigdata.utils

/**
  * Created by Administrator on 2016/10/10 0010.
  */
object SparkTag {
  val SPARK_MASTER_LOCAL : String ="spark.local";
  val SPARK_MASTER_REMOTE : String ="spark.remote";
  val SPARK_APPNAME : String ="spark.appName";
  val SPARK_DARA_SOURCE : String ="spark.data.source";
  val SPARK_DATE_FORMAT : String ="spark.date.format";
  val SPARK_DATE_INTERVAL : String ="spark.date.inteval";
  val SPARK_DATE_ACCURACY : String ="spark.date.accuracy";
  val ZOOKEEPER_ADDR : String ="spark.zookeeper.addr";
  val HBASE_TABLE_NAME : String ="spark.hbase.tableName";
  val HBASE_COLUMN_NAME : String ="spark.hbase.columnName";
}
