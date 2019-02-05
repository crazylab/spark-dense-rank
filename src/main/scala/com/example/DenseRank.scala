package com.example

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, collect_list, spark_partition_id}

class DenseRank(records: DataFrame, keyColumnName: String) extends Serializable {

  private val partitionNumCol = "_part_no"
  private val offsetCol = "_offset"

  private val orderedRecords = records
    .repartitionByRange(records.rdd.getNumPartitions, col(keyColumnName))
    .withColumn(partitionNumCol, spark_partition_id())

  private def calcPartOffset(): Array[Long] = {
    val denseRankOffset = new CountNumberOfElements

    val partitionOffset = orderedRecords
      .groupBy(partitionNumCol)
      .agg(denseRankOffset.distinct(col(keyColumnName)).as(offsetCol))
      .collect()
      .sortBy{case Row(partition: Int, offset: Long) => partition}
      .map { case Row(partition: Int, offset: Long) => offset }
      .scan(1L)(_ + _)

    orderedRecords
      .groupBy(partitionNumCol)
      .agg(denseRankOffset.distinct(col(keyColumnName)).as(offsetCol), collect_list(keyColumnName))
      .show()

    partitionOffset
  }

  def generateRank(): DataFrame = {
    val partitionOffset = calcPartOffset()

    partitionOffset.foreach(println)

    orderedRecords.mapPartitions(rows => {
      val partData = rows.toList.sortBy(_.getAs[Int](keyColumnName))
      val firstRow = partData.head

      val offset = partitionOffset(firstRow.getAs[Int](partitionNumCol))

      partData.tail.scanLeft(DataWithRank(firstRow, offset))((prev: DataWithRank, curr) => {
        val rank = if (prev.data.getAs[Int](keyColumnName) == curr.getAs[Int](keyColumnName)) {
          prev.rank
        } else prev.rank + 1
        DataWithRank(curr, rank)
      }).map(rowWithRank => Row.merge(rowWithRank.data, Row(rowWithRank.rank)))
        .toIterator
    })(Foo().encoder)
  }

}
