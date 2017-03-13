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
package org.apache.spark.util.expression.parserTrait

import org.apache.spark.util.expression.quantity.TimeAsSeconds

/**
 * A Trait that will match Time units and expand them into their equivalent number of seconds
 */
private[spark] trait TimeUnitSecParsing extends TimeUnitMSParsing {
  /**
   * An expression of time quantity eg 30 S, 4 Hours etc
   * returns number of bytes
   */
  protected override def timeExpression: Parser[Double] = decimalNumber~timeUnit ^^ {
    case decimalNumber~timeUnit => TimeAsSeconds(decimalNumber.toDouble, timeUnit).toSecs
  }

  /**
   * A Time quantity (eg 'Day') if not parsed as anything else is considered
   * a single unit of the specified quantity
   */
  protected override def standAloneTimeUnit: Parser[Double] = timeUnit ^^ {
    case timeUnit => TimeAsSeconds(1.0,timeUnit).toSecs
  }
}

