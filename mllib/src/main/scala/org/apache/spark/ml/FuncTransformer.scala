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

package org.apache.spark.ml

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.reflect.runtime.universe.{typeOf, TypeTag}

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.ml.FuncTransformer.FuncTransformerWriter
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.DataType

/**
 * :: DeveloperApi ::
 * A wrapper to allow easily creation of simple data manipulation for DataFrame.
 * Note that FuncTransformer supports serialization via scala ObjectOutputStream and may not
 * guarantee save/load compatibility between different scala version.
 */
@DeveloperApi
@Since("2.3.0")
class FuncTransformer [IN, OUT: TypeTag] @Since("2.3.0") (
    @Since("2.3.0") override val uid: String,
    @Since("2.3.0") val func: IN => OUT,
    @Since("2.3.0") val outputDataType: DataType
  ) extends UnaryTransformer[IN, OUT, FuncTransformer[IN, OUT]] with DefaultParamsWritable {

  @Since("2.3.0")
  def this(fx: IN => OUT, outputDataType: DataType) =
    this(Identifiable.randomUID("FuncTransformer"), fx, outputDataType)

  @Since("2.3.0")
  def this(fx: IN => OUT) =
    this(Identifiable.randomUID("FuncTransformer"), fx,
      CatalystSqlParser.parseDataType(typeOf[OUT].typeSymbol.name.decodedName.toString))

  setDefault(inputCol -> "input", outputCol -> "output")

  @Since("2.3.0")
  override def createTransformFunc: IN => OUT = func

  @Since("2.3.0")
  override def write: MLWriter = new FuncTransformerWriter(
    this.asInstanceOf[FuncTransformer[Nothing, Nothing]])
}

/**
 * :: DeveloperApi ::
 * Companion object for FuncTransformer with save and load function.
 */
@DeveloperApi
@Since("2.3.0")
object FuncTransformer extends DefaultParamsReadable[FuncTransformer[Nothing, Nothing]] {

  private[FuncTransformer]
  class FuncTransformerWriter(instance: FuncTransformer[Nothing, Nothing]) extends MLWriter {

    private case class Data(func: Array[Byte], dataType: String)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val bo = new ByteArrayOutputStream()
      new ObjectOutputStream(bo).writeObject(instance.func)
      val data = Data(bo.toByteArray, instance.outputDataType.json)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class FuncTransformerReader extends MLReader[FuncTransformer[Nothing, Nothing]] {

    private val className = classOf[FuncTransformer[Nothing, Nothing]].getName

    override def load(path: String): FuncTransformer[Nothing, Nothing] = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(funcBytes: Array[Byte], dataType: String) = data
          .select("func", "dataType")
          .head()
      val func = new ObjectInputStream(new ByteArrayInputStream(funcBytes)).readObject()
      val model = new FuncTransformer(
        metadata.uid, func.asInstanceOf[Function[Any, Any]], DataType.fromJson(dataType))
      DefaultParamsReader.getAndSetParams(model, metadata)
      model.asInstanceOf[FuncTransformer[Nothing, Nothing]]
    }
  }

  @Since("2.3.0")
  override def read: MLReader[FuncTransformer[Nothing, Nothing]] = new FuncTransformerReader

  @Since("2.3.0")
  override def load(path: String): FuncTransformer[Nothing, Nothing] = super.load(path)
}
