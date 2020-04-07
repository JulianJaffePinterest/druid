package org.apache.druid.spark.v2

import java.io.File
import java.util

import org.apache.druid.java.util.common.Intervals
import org.apache.druid.timeline.DataSegment
import org.apache.druid.timeline.partition.NumberedShardSpec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, LongType, StringType, StructField, StructType}
import org.joda.time.Interval

import scala.collection.mutable.ArrayBuffer

trait DruidDataSourceV2TestUtils {
  val dataSource: String = "spark_druid_test"
  val interval: Interval = Intervals.of("2020-01-01T00:00:00.000Z/2020-01-02T00:00:00.000Z")
  val secondInterval: Interval = Intervals.of("2020-01-02T00:00:00.000Z/2020-01-03T00:00:00.000Z")
  val version: String = "0"
  val firstSegmentPath: String = new File(
    "src/test/resources/segments/spark_druid_test/2020-01-01T00:00:00.000Z_2020-01-02T00:00:00.000Z/0/0/index.zip")
    .getAbsolutePath
  val secondSegmentPath: String = new File(
    "src/test/resources/segments/spark_druid_test/2020-01-01T00:00:00.000Z_2020-01-02T00:00:00.000Z/0/1/index.zip")
    .getAbsolutePath
  val thirdSegmentPath: String = new File(
    "src/test/resources/segments/spark_druid_test/2020-01-02T00:00:00.000Z_2020-01-03T00:00:00.000Z/0/0/index.zip")
    .getAbsolutePath
  val loadSpec: String => util.Map[String, AnyRef] = (path: String) =>
    Map[String, AnyRef]("type" -> "local", "path" -> path).asJava
  val dimensions: util.List[String] = List("dim1", "dim2", "id1", "id2").asJava
  val metrics: util.List[String] = List(
    "count", "sum_metric1","sum_metric2","sum_metric3","sum_metric4","uniq_id1").asJava
  val binaryVersion: Integer = 9
  val firstSegment: DataSegment = new DataSegment(
    dataSource,
    interval,
    version,
    loadSpec(firstSegmentPath),
    dimensions,
    metrics,
    new NumberedShardSpec(0, 0),
    binaryVersion,
    3278L
  )
  val secondSegment: DataSegment = new DataSegment(
    dataSource,
    interval,
    version,
    loadSpec(secondSegmentPath),
    dimensions,
    metrics,
    new NumberedShardSpec(1, 0),
    binaryVersion,
    3299L
  )
  val thirdSegment: DataSegment = new DataSegment(
    dataSource,
    secondInterval,
    version,
    loadSpec(thirdSegmentPath),
    dimensions,
    metrics,
    new NumberedShardSpec(0, 0),
    binaryVersion,
    3409L
  )

  val schema: StructType = StructType(Seq[StructField](
    StructField("__time", LongType),
    StructField("dim1", ArrayType(StringType, false)),
    StructField("dim2", StringType),
    StructField("id1", StringType),
    StructField("id2", StringType),
    StructField("count", LongType),
    StructField("sum_metric1", LongType),
    StructField("sum_metric2", LongType),
    StructField("sum_metric3", DoubleType),
    StructField("sum_metric4", FloatType)/*,
    StructField("uniq_id1", BinaryType)*/ // TODO: Test ThetaSketch as well. Long term add UDT
  ))


  def partitionReaderToSeq(reader: InputPartitionReader[InternalRow]): Seq[InternalRow] = {
    val res = new ArrayBuffer[InternalRow]()
    // TODO: Wrap this in a custom iterator and call .toSeq to avoid mutable collections
    while (reader.next()) {
      res += reader.get()
    }
    reader.close()
    res
  }

  def compareInternalRows(left: InternalRow, right: InternalRow, schema: StructType): Boolean = {
    left.numFields == right.numFields && !left.toSeq(schema).forall(right.toSeq(schema).contains(_))
  }
}
