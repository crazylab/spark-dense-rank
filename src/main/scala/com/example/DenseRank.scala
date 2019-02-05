package com.example

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, spark_partition_id}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class DenseRank(records: DataFrame, keyColumnName: String) extends Serializable {

  private val partitionNumCol = "_part_no"
  private val offsetCol = "_offset"
  private val rankCol = "_rank"

  private val resultSchema = records.schema.add(StructField(rankCol, LongType))
  private val resultEncoder = RowEncoder(resultSchema)

  private val orderedRecords = records
    .repartitionByRange(records.rdd.getNumPartitions, col(keyColumnName))
    .withColumn(partitionNumCol, spark_partition_id())


  private def calculatePartitionOffset(): Array[Long] = {
    val denseRankOffset = new CountNumberOfElements

    val partitionOffset = orderedRecords
      .groupBy(partitionNumCol)
      .agg(denseRankOffset.distinct(col(keyColumnName)).as(offsetCol))
      .collect()
      .sortBy { case Row(partition: Int, _) => partition }
      .map { case Row(_, offset: Long) => offset }
      .scan(1L)(_ + _)

    partitionOffset
  }

  def generateRank(): DataFrame = {
    val partitionOffset = calculatePartitionOffset()

    orderedRecords.mapPartitions(rows => {
      if (rows.isEmpty) {
        rows
      } else {
        val partData = rows.toList.sortBy(_.getAs[Int](keyColumnName))

        val firstRow = partData.head
        val offset = partitionOffset(firstRow.getAs[Int](partitionNumCol))


        val firstRowWithRank = appendColumn(firstRow, offset)

        val result: Iterator[Row] = partData.tail
          .scan(firstRowWithRank) { case (prev, curr) => {
            val lastRank = prev.getAs[Long](rankCol)
            val rank = if (prev.getAs[Int](keyColumnName) == curr.getAs[Int](keyColumnName)) {
              lastRank
            } else {
              lastRank + 1L
            }
            appendColumn(curr, rank)
          }
          }.toIterator
        result
      }
    })(resultEncoder)
  }

  private def appendColumn(row: Row, value: Any): Row = {
    new GenericRowWithSchema(row.toSeq.toArray.init :+ value, resultSchema)
  }
}
