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

package org.apache.spark.api.java.function;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;

/**
 * A three-argument function that takes arguments of type T1, T2 and T3 and returns an R.
 */
public abstract class Function3<T1, T2, T3, R> extends WrappedFunction3<T1, T2, T3, R>
        implements Serializable {

    public ClassTag<R> returnType() {
        return (ClassTag<R>) ClassTag$.MODULE$.apply(Object.class);
    }
}

