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

package org.apache.spark.ml.param.shared

import java.io.PrintWriter

import scala.reflect.ClassTag

/**
 * Code generator for shared params (sharedParams.scala). Run under the Spark folder with
 * {{{
 *   build/sbt "mllib/runMain org.apache.spark.ml.param.shared.SharedParamCodeGen"
 * }}}
 */
private[shared] object SharedParamCodeGen {

  def main(args: Array[String]): Unit = {
    val params = Seq(
      ParamDesc[Double]("regParam", "regularization parameter"),
      ParamDesc[Int]("maxIter", "max number of iterations"),
      ParamDesc[String]("featuresCol", "features column name"),
      ParamDesc[String]("labelCol", "label column name"),
      ParamDesc[String]("predictionCol", "prediction column name"),
      ParamDesc[String]("rawPredictionCol", "raw prediction (a.k.a. confidence) column name"),
      ParamDesc[String](
        "probabilityCol", "column name for predicted class conditional probabilities"),
      ParamDesc[Double]("threshold", "threshold in prediction"),
      ParamDesc[String]("inputCol", "input column name"),
      ParamDesc[String]("outputCol", "output column name"),
      ParamDesc[Int]("checkpointInterval", "checkpoint interval"))

    val code = genSharedParams(params)
    val file = "src/main/scala/org/apache/spark/ml/param/shared/sharedParams.scala"
    val writer = new PrintWriter(file)
    writer.write(code)
    writer.close()
  }

  /** Description of a param. */
  private case class ParamDesc[T: ClassTag](name: String, doc: String) {
    require(name.matches("[a-z][a-zA-Z0-9]*"), s"Param name $name is invalid.")
    require(doc.nonEmpty) // TODO: more rigorous on doc

    def paramTypeName: String = {
      val c = implicitly[ClassTag[T]].runtimeClass
      c match {
        case _ if c == classOf[Int] => "IntParam"
        case _ if c == classOf[Long] => "LongParam"
        case _ if c == classOf[Float] => "FloatParam"
        case _ if c == classOf[Double] => "DoubleParam"
        case _ if c == classOf[Boolean] => "BooleanParam"
        case _ => s"Param[${getTypeString(c)}]"
      }
    }

    def valueTypeName: String = {
      val c = implicitly[ClassTag[T]].runtimeClass
      getTypeString(c)
    }

    private def getTypeString(c: Class[_]): String = {
      c match {
        case _ if c == classOf[Int] => "Int"
        case _ if c == classOf[Long] => "Long"
        case _ if c == classOf[Float] => "Float"
        case _ if c == classOf[Double] => "Double"
        case _ if c == classOf[Boolean] => "Boolean"
        case _ if c == classOf[String] => "String"
        case _ if c.isArray => s"Array[${getTypeString(c.getComponentType)}]"
      }
    }
  }

  /** Generates the HasParam trait code for the input param. */
  private def genHasParamTrait(param: ParamDesc[_]): String = {
    val name = param.name
    val Name = name(0).toUpper +: name.substring(1)
    val Param = param.paramTypeName
    val T = param.valueTypeName
    val doc = param.doc

    s"""
      |/**
      | * :: DeveloperApi ::
      | * Trait for shared param $name.
      | */
      |@DeveloperApi
      |trait Has$Name extends Params {
      |  /**
      |   * Param for $doc.
      |   * @group param
      |   */
      |  final val $name: $Param = new $Param(this, "$name", "$doc")
      |
      |  /** @group getParam */
      |  final def get$Name: $T = get($name)
      |}
    """.stripMargin
  }

  /** Generates Scala source code for the input params with header. */
  private def genSharedParams(params: Seq[ParamDesc[_]]): String = {
    val header =
      """
        |/*
        | * Licensed to the Apache Software Foundation (ASF) under one or more
        | * contributor license agreements.  See the NOTICE file distributed with
        | * this work for additional information regarding copyright ownership.
        | * The ASF licenses this file to You under the Apache License, Version 2.0
        | * (the "License"); you may not use this file except in compliance with
        | * the License.  You may obtain a copy of the License at
        | *
        | *    http://www.apache.org/licenses/LICENSE-2.0
        | *
        | * Unless required by applicable law or agreed to in writing, software
        | * distributed under the License is distributed on an "AS IS" BASIS,
        | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        | * See the License for the specific language governing permissions and
        | * limitations under the License.
        | */
        |
        |package org.apache.spark.ml.param.shared
        |
        |import org.apache.spark.annotation.DeveloperApi
        |import org.apache.spark.ml.param._
        |
        |// DO NOT MODIFY THIS FILE! It was generated by SharedParamCodeGen.
        |
        |// scalastyle:off
      """.stripMargin

    val footer =
      """
        |// scalastyle:on
      """.stripMargin

    val traits = params.map(genHasParamTrait).mkString

    header + traits + footer
  }
}
