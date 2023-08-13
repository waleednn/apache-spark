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

package org.apache.spark.sql.catalyst.encoders

import java.lang.ref._
import java.util.Objects
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.util.SparkClassUtils

object OuterScopes {
  private[this] val queue = new ReferenceQueue[AnyRef]
  private class HashableWeakReference(val v: AnyRef) extends WeakReference[AnyRef](v, queue) {
    private[this] val hash = v.hashCode()
    override def hashCode(): Int = hash
    override def equals(obj: Any): Boolean = obj match {
      case ref: HashableWeakReference =>
        (this eq ref) || Objects.equals(get(), ref.get())
    }
  }

  private def classLoaderRef(c: Class[_]): HashableWeakReference = {
    new HashableWeakReference(c.getClassLoader)
  }

  private[this] val outerScopes = {
    new ConcurrentHashMap[HashableWeakReference, ConcurrentHashMap[String, WeakReference[AnyRef]]]
  }

  /**
   * Clean the outer scopes that have been garbage collected.
   */
  private def cleanOuterScopes(): Unit = {
    var entry = queue.poll()
    while (entry != null) {
      outerScopes.remove(entry)
      entry = queue.poll()
    }
  }

  /**
   * Adds a new outer scope to this context that can be used when instantiating an `inner class`
   * during deserialization. Inner classes are created when a case class is defined in the
   * Spark REPL and registering the outer scope that this class was defined in allows us to create
   * new instances on the spark executors.  In normal use, users should not need to call this
   * function.
   *
   * Warning: this function operates on the assumption that there is only ever one instance of any
   * given wrapper class.
   */
  def addOuterScope(outer: AnyRef): Unit = {
    cleanOuterScopes()
    val clz = outer.getClass
    outerScopes
      .computeIfAbsent(classLoaderRef(clz), _ => new ConcurrentHashMap)
      .putIfAbsent(clz.getName, new WeakReference(outer))
  }

  /**
   * Returns a function which can get the outer scope for the given inner class.  By using function
   * as return type, we can delay the process of getting outer pointer to execution time, which is
   * useful for inner class defined in REPL.
   */
  def getOuterScope(innerCls: Class[_]): () => AnyRef = {
    if (!SparkClassUtils.isMemberClass(innerCls)) {
      return null
    }
    val outerClass = innerCls.getDeclaringClass
    val outerClassName = outerClass.getName
    val outer = Option(outerScopes.get(classLoaderRef(outerClass)))
      .flatMap(map => Option(map.get(outerClassName)))
      .map(_.get())
      .orNull
    if (outer == null) {
      outerClassName match {
        case AmmoniteREPLClass(cellClassName) =>
          () => {
            val objClass = SparkClassUtils.classForName(cellClassName)
            val objInstance = objClass.getField("MODULE$").get(null)
            val obj = objClass.getMethod("instance").invoke(objInstance)
            addOuterScope(obj)
            obj
          }
        // If the outer class is generated by REPL, users don't need to register it as it has
        // only one instance and there is a way to retrieve it: get the `$read` object, call the
        // `INSTANCE()` method to get the single instance of class `$read`. Then call `$iw()`
        // method multiply times to get the single instance of the inner most `$iw` class.
        case REPLClass(baseClassName) =>
          () => {
            val objClass = SparkClassUtils.classForName(baseClassName + "$")
            val objInstance = objClass.getField("MODULE$").get(null)
            val baseInstance = objClass.getMethod("INSTANCE").invoke(objInstance)
            val baseClass = SparkClassUtils.classForName(baseClassName)

            var getter = iwGetter(baseClass)
            var obj = baseInstance
            while (getter != null) {
              obj = getter.invoke(obj)
              getter = iwGetter(getter.getReturnType)
            }

            if (obj == null) {
              throw ExecutionErrors.cannotGetOuterPointerForInnerClassError(innerCls)
            }
            addOuterScope(obj)
            obj
          }
        case _ => null
      }
    } else {
      () => outer
    }
  }

  private def iwGetter(cls: Class[_]) = {
    try {
      cls.getMethod("$iw")
    } catch {
      case _: NoSuchMethodException => null
    }
  }

  // The format of REPL generated wrapper class's name, e.g. `$line12.$read$$iw$$iw`
  private[this] val REPLClass = """^(\$line(?:\d+)\.\$read)(?:\$\$iw)+$""".r

  // The format of ammonite REPL generated wrapper class's name,
  // e.g. `ammonite.$sess.cmd8$Helper$Foo` -> `ammonite.$sess.cmd8.instance.Foo`
  private[this] val AmmoniteREPLClass = """^(ammonite\.\$sess\.cmd(?:\d+)\$).*""".r
}
