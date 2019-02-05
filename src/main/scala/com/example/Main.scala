package com.example

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Main extends Serializable {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("sample-app")
      .getOrCreate()

    val data = generateData(spark)
    val withRank = new DenseRank(data, "key").generateRank()

    withRank
      .show(false)

    withRank.foreach(println(_))

    spark.close()
  }

  def generateData(spark: SparkSession): DataFrame = {
    val dataInRDD = spark.sparkContext.parallelize(List[Row](
      Row(11, "value1"),
      Row(12, "value21"),
      Row(12, "value22"),
      Row(13, "value3"),
      Row(14, "value4"),
      Row(15, "value51"),
      Row(15, "value52"),
      Row(15, "value53"),
      Row(16, "value6"),
      Row(17, "value7"),
      Row(18, "value8"),
      Row(15, "value8")
    ))

    val schema = StructType(StructField("key", IntegerType) :: StructField("value", StringType) :: Nil)
    spark.createDataFrame(dataInRDD, schema).repartition(5)
  }
}
