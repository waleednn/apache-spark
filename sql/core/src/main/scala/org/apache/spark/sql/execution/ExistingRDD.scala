/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.datasources.parquet.ParquetRelation
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, HadoopFsRelation}
import org.apache.spark.sql.types.DataType

object RDDConversions {
  def productToRowRdd[A <: Product](data: RDD[A], outputTypes: Seq[DataType]): RDD[InternalRow] = {
    data.mapPartitions { iterator =>
      val numColumns = outputTypes.length
      val mutableRow = new GenericMutableRow(numColumns)
      val converters = outputTypes.map(CatalystTypeConverters.createToCatalystConverter)
      iterator.map { r =>
        var i = 0
        while (i < numColumns) {
          mutableRow(i) = converters(i)(r.productElement(i))
          i += 1
        }

        mutableRow
      }
    }
  }

  /**
   * Convert the objects inside Row into the types Catalyst expected.
   */
  def rowToRowRdd(data: RDD[Row], outputTypes: Seq[DataType]): RDD[InternalRow] = {
    data.mapPartitions { iterator =>
      val numColumns = outputTypes.length
      val mutableRow = new GenericMutableRow(numColumns)
      val converters = outputTypes.map(CatalystTypeConverters.createToCatalystConverter)
      iterator.map { r =>
        var i = 0
        while (i < numColumns) {
          mutableRow(i) = converters(i)(r(i))
          i += 1
        }

        mutableRow
      }
    }
  }
}

/** Logical plan node for scanning data from an RDD. */
private[sql] case class LogicalRDD(
    output: Seq[Attribute],
    rdd: RDD[InternalRow])(sqlContext: SQLContext)
  extends LogicalPlan with MultiInstanceRelation {

  override def children: Seq[LogicalPlan] = Nil

  override protected final def otherCopyArgs: Seq[AnyRef] = sqlContext :: Nil

  override def newInstance(): LogicalRDD.this.type =
    LogicalRDD(output.map(_.newInstance()), rdd)(sqlContext).asInstanceOf[this.type]

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case LogicalRDD(_, otherRDD) => rdd.id == otherRDD.id
    case _ => false
  }

  override def producedAttributes: AttributeSet = outputSet

  @transient override lazy val statistics: Statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(sqlContext.conf.defaultSizeInBytes)
  )
}

/** Physical plan node for scanning data from an RDD. */
private[sql] case class PhysicalRDD(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    override val nodeName: String,
    override val metadata: Map[String, String] = Map.empty,
    isUnsafeRow: Boolean = false,
    override val outputPartitioning: Partitioning = UnknownPartitioning(0))
  extends LeafNode with CodegenSupport {

  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val unsafeRow = if (isUnsafeRow) {
      rdd
    } else {
      rdd.mapPartitionsInternal { iter =>
        val proj = UnsafeProjection.create(schema)
        iter.map(proj)
      }
    }

    val numOutputRows = longMetric("numOutputRows")
    unsafeRow.map { r =>
      numOutputRows += 1
      r
    }
  }

  override def simpleString: String = {
    val metadataEntries = for ((key, value) <- metadata.toSeq.sorted) yield s"$key: $value"
    s"Scan $nodeName${output.mkString("[", ",", "]")}${metadataEntries.mkString(" ", ", ", "")}"
  }

  override def upstreams(): Seq[RDD[InternalRow]] = {
    rdd :: Nil
  }

  // Support codegen so that we can avoid the UnsafeRow conversion in all cases. Codegen
  // never requires UnsafeRow as input.
  override protected def doProduce(ctx: CodegenContext): String = {
    val input = ctx.freshName("input")
    // PhysicalRDD always just has one input
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")

    val exprs = output.zipWithIndex.map(x => new BoundReference(x._2, x._1.dataType, true))
    val row = ctx.freshName("row")
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columns = exprs.map(_.gen(ctx))

    // The input RDD can either return (all) ColumnarBatches or InternalRows. We determine this
    // by looking at the first value of the RDD and then calling the function which will process
    // the remaining. It is faster to return batches.
    // TODO: The abstractions between this class and SqlNewHadoopRDD makes it difficult to know
    // here which path to use. Fix this.

    val columnarBatchClz = "org.apache.spark.sql.execution.vectorized.ColumnarBatch"

    val scanBatches = ctx.freshName("processBatches")
    ctx.addNewFunction(scanBatches,
      s"""
      | private void $scanBatches($columnarBatchClz batch) throws java.io.IOException {
      |  while (true) {
      |     int numRows = batch.numRows();
      |     $numOutputRows.add(numRows);
      |     for (int i = 0; i < numRows; i++) {
      |       InternalRow $row = batch.getRow(i);
      |       ${columns.map(_.code).mkString("\n").trim}
      |       ${consume(ctx, columns).trim}
      |     }
      |
      |     if (shouldStop()) return;
      |     if (!$input.hasNext()) break;
      |     batch = ($columnarBatchClz)$input.next();
      |   }
      | }""".stripMargin)

    val scanRows = ctx.freshName("processRows")
    ctx.addNewFunction(scanRows,
      s"""
       | private void $scanRows(InternalRow $row) throws java.io.IOException {
       |   while (true) {
       |     $numOutputRows.add(1);
       |     ${columns.map(_.code).mkString("\n").trim}
       |     ${consume(ctx, columns).trim}
       |     if (shouldStop()) return;
       |     if (!$input.hasNext()) break;
       |     $row = (InternalRow)$input.next();
       |   }
       | }""".stripMargin)

    s"""
       | if ($input.hasNext()) {
       |   Object firstValue = $input.next();
       |   if (firstValue instanceof $columnarBatchClz) {
       |     $scanBatches(($columnarBatchClz)firstValue);
       |   } else {
       |     $scanRows((InternalRow)firstValue);
       |   }
       | }
     """.stripMargin
  }
}

private[sql] object PhysicalRDD {
  // Metadata keys
  val INPUT_PATHS = "InputPaths"
  val PUSHED_FILTERS = "PushedFilters"

  def createFromDataSource(
      output: Seq[Attribute],
      rdd: RDD[InternalRow],
      relation: BaseRelation,
      metadata: Map[String, String] = Map.empty): PhysicalRDD = {
    val outputUnsafeRows = if (relation.isInstanceOf[ParquetRelation]) {
      // The vectorized parquet reader does not produce unsafe rows.
      !SQLContext.getActive().get.conf.getConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED)
    } else {
      // All HadoopFsRelations output UnsafeRows
      relation.isInstanceOf[HadoopFsRelation]
    }

    val bucketSpec = relation match {
      case r: HadoopFsRelation => r.getBucketSpec
      case _ => None
    }

    def toAttribute(colName: String): Attribute = output.find(_.name == colName).getOrElse {
      throw new AnalysisException(s"bucket column $colName not found in existing columns " +
        s"(${output.map(_.name).mkString(", ")})")
    }

    bucketSpec.map { spec =>
      val numBuckets = spec.numBuckets
      val bucketColumns = spec.bucketColumnNames.map(toAttribute)
      val partitioning = HashPartitioning(bucketColumns, numBuckets)
      PhysicalRDD(output, rdd, relation.toString, metadata, outputUnsafeRows, partitioning)
    }.getOrElse {
      PhysicalRDD(output, rdd, relation.toString, metadata, outputUnsafeRows)
    }
  }
}
