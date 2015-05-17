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

package org.apache.spark.mllib.linalg

import org.apache.spark.mllib.util.MLUtils

trait Kernel {
  def compute(vi: Vector, indexi: Long, vj: Vector, indexj: Long): Double
}

/**
 * CosineKernel is the default option for similarity calculation
 * @param rowNorms denominator needs to be normalized by rowNorm
 * @param threshold don't shuffle if similarity is less than the threshold specified by user
 */
case class CosineKernel(rowNorms: Map[Long, Double], threshold: Double) extends Kernel {
  override def compute(vi: Vector, indexi: Long, vj: Vector, indexj: Long): Double = {
    val similarity = BLAS.dot(vi, vj) / rowNorms(indexi) / rowNorms(indexj)
    if (similarity <= threshold) return 0.0
    similarity
  }
}

// For distributed matrix multiplication with user defined normalization
case class ProductKernel() extends Kernel {
  override def compute(vi: Vector, indexi: Long, vj: Vector, indexj: Long): Double = {
    BLAS.dot(vi, vj)
  }
}

case class EuclideanKernel(rowNorms: Map[Long, Double], threshold: Double) extends Kernel {
  override def compute(vi: Vector, indexi: Long, vj: Vector, indexj: Long): Double = {
    MLUtils.fastSquaredDistance(vi, rowNorms(indexi), vj, rowNorms(indexj))
  }
}

// For PowerIterationClustering flows
case class RBFKernel(rowNorms: Map[Long, Double], sigma: Double) extends Kernel {
  val coeff = 1.0 / (math.sqrt(2.0 * math.Pi) * sigma)
  val expCoeff = -1.0 / 2.0 * math.pow(sigma, 2.0)

  override def compute(vi: Vector, indexi: Long, vj: Vector, indexj: Long): Double = {
    val ssquares = MLUtils.fastSquaredDistance(vi, rowNorms(indexi), vj, rowNorms(indexj))
    coeff * math.exp(expCoeff * ssquares)
  }
}

object KernelType extends Enumeration {
  type KernelType = Value
  val COSINE, PRODUCT, EUCLIDEAN, RBF = Value
}
