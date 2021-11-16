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

package org.apache.spark.sql.execution.arrow

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, OutputStream}
import java.nio.channels.{Channels, ReadableByteChannel}

import scala.collection.JavaConverters._

import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ArrowStreamWriter, ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, IpcOption, MessageSerializer}

import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.{ByteBufferOutputStream, Utils}


/**
 * Writes serialized ArrowRecordBatches to a DataOutputStream in the Arrow stream format.
 */
private[sql] class ArrowBatchStreamWriter(
    schema: StructType,
    out: OutputStream,
    timeZoneId: String) {

  val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
  val writeChannel = new WriteChannel(Channels.newChannel(out))

  // Write the Arrow schema first, before batches
  MessageSerializer.serialize(writeChannel, arrowSchema)

  /**
   * Consume iterator to write each serialized ArrowRecordBatch to the stream.
   */
  def writeBatches(arrowBatchIter: Iterator[Array[Byte]]): Unit = {
    arrowBatchIter.foreach(writeChannel.write)
  }

  /**
   * End the Arrow stream, does not close output stream.
   */
  def end(): Unit = {
    ArrowStreamWriter.writeEndOfStream(writeChannel, new IpcOption)
  }
}

private[sql] object ArrowConverters {

  /**
   * Maps Iterator from InternalRow to ArrowRecordBatche. Limit ArrowRecordBatch size
   * in a batch by setting maxRecordsPerBatch or use 0 to fully consume rowIter.
   */
  private[sql] def toArrowRecordBatchIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Int,
      timeZoneId: String,
      context: TaskContext): Iterator[ArrowRecordBatch] = {

    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("toArrowRecordBatchIterator", 0, Long.MaxValue)

    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val unloader = new VectorUnloader(root)
    val arrowWriter = ArrowWriter.create(root)


    new Iterator[ArrowRecordBatch] {
      private var holdArrowRecordBatch: ArrowRecordBatch = _
      private var holdNext: Boolean = false
      context.addTaskCompletionListener[Unit] { _ =>
        if (holdArrowRecordBatch != null) {
          holdArrowRecordBatch.close()
          holdArrowRecordBatch = null
        }
        root.close()
        allocator.close()
      }

      override def hasNext: Boolean = holdNext || {
        if (holdArrowRecordBatch != null) {
          holdArrowRecordBatch.close()
          holdArrowRecordBatch = null
        }
        if(rowIter.hasNext) {
          holdArrowRecordBatch = nextArrowRecordBatch()
          holdNext = true
          true
        } else {
          root.close()
          allocator.close()
          false
        }
      }

      override def next(): ArrowRecordBatch = {
        holdNext = false
        holdArrowRecordBatch
      }

      private def nextArrowRecordBatch(): ArrowRecordBatch = {
        try {
          var rowCount = 0
          arrowWriter.reset()
          while (rowIter.hasNext && (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
            val row = rowIter.next()
            arrowWriter.write(row)
            rowCount += 1
          }
          arrowWriter.finish()
          unloader.getRecordBatch()
        } catch {
          case t: Throwable =>
            // Purposefully not using NonFatal, because even fatal exceptions
            // we don't want to have our finallyBlock suppress
            arrowWriter.reset()
            throw t
        }
      }
    }
  }

  /**
   * Maps Iterator from ArrowRecordBatche to InternalRow.
   * Path is ArrowRecordBatch -> ColumnarBatch -> InternalRow
   */
  private[sql] def fromArrowRecordBatchIterator(
      arrowBatchIter: Iterator[ArrowRecordBatch],
      schema: StructType,
      timeZoneId: String,
      context: TaskContext): Iterator[InternalRow] = {
    val columnarBatchIter = toColumnarBatchIterator(arrowBatchIter, schema, timeZoneId, context)
    columnarBatchIter.flatMap(_.rowIterator().asScala)
  }

  /**
   * Maps Iterator from InternalRow to serialized ArrowRecordBatches. Limit ArrowRecordBatch size
   * in a batch by setting maxRecordsPerBatch or use 0 to fully consume rowIter.
   */
  private[sql] def toBatchIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Int,
      timeZoneId: String,
      context: TaskContext): Iterator[Array[Byte]] = {

      val arrowRecordBatchIter =
        toArrowRecordBatchIterator(rowIter, schema, maxRecordsPerBatch, timeZoneId, context)
      arrowRecordBatchIter.map(batch => {
        val out = new ByteArrayOutputStream()
        val writeChannel = new WriteChannel(Channels.newChannel(out))
        MessageSerializer.serialize(writeChannel, batch)
        out.toByteArray
      })
    }

  /**
   * Maps iterator from serialized ArrowRecordBatches to InternalRows.
   */
  private[sql] def fromBatchIterator(
      arrowBatchIter: Iterator[Array[Byte]],
      schema: StructType,
      timeZoneId: String,
      context: TaskContext): Iterator[InternalRow] = {
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("fromBatchIterator", 0, Long.MaxValue)
    val arrowRecordBatchIter = arrowBatchIter.map(ArrowConverters.loadBatch(_, allocator))
    fromArrowRecordBatchIterator(arrowRecordBatchIter, schema, timeZoneId, context)
  }

  /**
   * Maps iterator from serialized ArrowRecordBatches to ColumnarBatchs.
   */
  private[sql] def toColumnarBatchIterator(
      arrowBatchIter: Iterator[ArrowRecordBatch],
      schema: StructType,
      timeZoneId: String,
      context: TaskContext): Iterator[ColumnarBatch] = {
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("toColumnarBatchIterator", 0, Long.MaxValue)

    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)

    new Iterator[ColumnarBatch] {
      private var nextColumnarBatch: ColumnarBatch = _
      private var holdNext: Boolean = false

      context.addTaskCompletionListener[Unit] { _ =>
        if (nextColumnarBatch != null) {
          nextColumnarBatch.close()
          nextColumnarBatch = null
        }
        root.close()
        allocator.close()
      }

      override def hasNext: Boolean = holdNext || {
        if (nextColumnarBatch != null) {
          nextColumnarBatch.close()
          nextColumnarBatch = null
        }
        if (arrowBatchIter.hasNext) {
          nextColumnarBatch = nextBatch()
          holdNext = true
          true
        } else {
          root.close()
          allocator.close()
          false
        }
      }

      override def next(): ColumnarBatch = {
        holdNext = false
        nextColumnarBatch
      }

      private def nextBatch(): ColumnarBatch = {
        val arrowRecordBatch = arrowBatchIter.next()
        val vectorLoader = new VectorLoader(root)
        vectorLoader.load(arrowRecordBatch)
        arrowRecordBatch.close()

        val columns = root.getFieldVectors.asScala.map { vector =>
          new ArrowColumnVector(vector).asInstanceOf[ColumnVector]
        }.toArray

        val batch = new ColumnarBatch(columns)
        batch.setNumRows(root.getRowCount)
        batch
      }
    }
  }

  /**
   * Maps iterator from serialized ColumnarBatchs to ArrowRecordBatches.
   */
  private[sql] def fromColumnarBatchIterator(
      columnarBatchIter: Iterator[ColumnarBatch],
      schema: StructType,
      timeZoneId: String,
      context: TaskContext): Iterator[ArrowRecordBatch] = {
    new Iterator[ArrowRecordBatch] {
      private var nextArrowRecordBatch: ArrowRecordBatch = _
      private var holdNext: Boolean = false
      private var root: VectorSchemaRoot = _

      context.addTaskCompletionListener[Unit] { _ =>
        if (nextArrowRecordBatch != null) {
          nextArrowRecordBatch.close()
          nextArrowRecordBatch = null
        }
        if (root != null) root.close()
      }

      override def hasNext: Boolean = holdNext || {
        if (nextArrowRecordBatch != null) {
          nextArrowRecordBatch.close()
          nextArrowRecordBatch = null
        }
        if (columnarBatchIter.hasNext) {
          nextArrowRecordBatch = nextBatch()
          holdNext = true
          true
        } else {
          if (root != null) root.close()
          false
        }
      }

      override def next(): ArrowRecordBatch = {
        holdNext = false
        nextArrowRecordBatch
      }

      private def nextBatch(): ArrowRecordBatch = {
        val columnarBatch = columnarBatchIter.next()
        val arrowVectors = (0 until columnarBatch.numCols).map(i => {
          columnarBatch.column(i).asInstanceOf[ArrowColumnVector].getArrowVector()
        }).map(_.asInstanceOf[FieldVector])
        if (root != null) root.close()
        root = new VectorSchemaRoot(arrowVectors.asJava)
        val unloader = new VectorUnloader(root)
        val arrowRecordBatch = unloader.getRecordBatch()
        columnarBatch.close()
        arrowRecordBatch
      }
    }
  }

  /**
   * Load a serialized ArrowRecordBatch.
   */
  private[sql] def loadBatch(
      batchBytes: Array[Byte],
      allocator: BufferAllocator): ArrowRecordBatch = {
    val in = new ByteArrayInputStream(batchBytes)
    MessageSerializer.deserializeRecordBatch(
      new ReadChannel(Channels.newChannel(in)), allocator)  // throws IOException
  }

  /**
   * Create a DataFrame from an RDD of serialized ArrowRecordBatches.
   */
  private[sql] def toDataFrame(
      arrowBatchRDD: JavaRDD[Array[Byte]],
      schemaString: String,
      sqlContext: SQLContext): DataFrame = {
    val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
    val timeZoneId = sqlContext.sessionState.conf.sessionLocalTimeZone
    val rdd = arrowBatchRDD.rdd.mapPartitions { iter =>
      val context = TaskContext.get()
      ArrowConverters.fromBatchIterator(iter, schema, timeZoneId, context)
    }
    sqlContext.internalCreateDataFrame(rdd.setName("arrow"), schema)
  }

  /**
   * Read a file as an Arrow stream and parallelize as an RDD of serialized ArrowRecordBatches.
   */
  private[sql] def readArrowStreamFromFile(
      sqlContext: SQLContext,
      filename: String): JavaRDD[Array[Byte]] = {
    Utils.tryWithResource(new FileInputStream(filename)) { fileStream =>
      // Create array to consume iterator so that we can safely close the file
      val batches = getBatchesFromStream(fileStream.getChannel).toArray
      // Parallelize the record batches to create an RDD
      JavaRDD.fromRDD(sqlContext.sparkContext.parallelize(batches, batches.length))
    }
  }

  /**
   * Read an Arrow stream input and return an iterator of serialized ArrowRecordBatches.
   */
  private[sql] def getBatchesFromStream(in: ReadableByteChannel): Iterator[Array[Byte]] = {

    // Iterate over the serialized Arrow RecordBatch messages from a stream
    new Iterator[Array[Byte]] {
      var batch: Array[Byte] = readNextBatch()

      override def hasNext: Boolean = batch != null

      override def next(): Array[Byte] = {
        val prevBatch = batch
        batch = readNextBatch()
        prevBatch
      }

      // This gets the next serialized ArrowRecordBatch by reading message metadata to check if it
      // is a RecordBatch message and then returning the complete serialized message which consists
      // of a int32 length, serialized message metadata and a serialized RecordBatch message body
      def readNextBatch(): Array[Byte] = {
        val msgMetadata = MessageSerializer.readMessage(new ReadChannel(in))
        if (msgMetadata == null) {
          return null
        }

        // Get the length of the body, which has not been read at this point
        val bodyLength = msgMetadata.getMessageBodyLength.toInt

        // Only care about RecordBatch messages, skip Schema and unsupported Dictionary messages
        if (msgMetadata.getMessage.headerType() == MessageHeader.RecordBatch) {

          // Buffer backed output large enough to hold 8-byte length + complete serialized message
          val bbout = new ByteBufferOutputStream(8 + msgMetadata.getMessageLength + bodyLength)

          // Write message metadata to ByteBuffer output stream
          MessageSerializer.writeMessageBuffer(
            new WriteChannel(Channels.newChannel(bbout)),
            msgMetadata.getMessageLength,
            msgMetadata.getMessageBuffer)

          // Get a zero-copy ByteBuffer with already contains message metadata, must close first
          bbout.close()
          val bb = bbout.toByteBuffer
          bb.position(bbout.getCount())

          // Read message body directly into the ByteBuffer to avoid copy, return backed byte array
          bb.limit(bb.capacity())
          JavaUtils.readFully(in, bb)
          bb.array()
        } else {
          if (bodyLength > 0) {
            // Skip message body if not a RecordBatch
            Channels.newInputStream(in).skip(bodyLength)
          }

          // Proceed to next message
          readNextBatch()
        }
      }
    }
  }
}
