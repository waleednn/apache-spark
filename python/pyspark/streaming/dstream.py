#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from itertools import chain
import operator
import time
from datetime import datetime

from py4j.protocol import Py4JJavaError

from pyspark import RDD
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.util import rddToFileName, TransformFunction
from pyspark.rdd import portable_hash
from pyspark.resultiterable import ResultIterable

__all__ = ["DStream"]


class DStream(object):
    """
    A Discretized Stream (DStream), the basic abstraction in Spark Streaming,
    is a continuous sequence of RDDs (of the same type) representing a
    continuous stream of data (see L{RDD} in the Spark core documentation
    for more details on RDDs).

    DStreams can either be created from live data (such as, data from TCP
    sockets, Kafka, Flume, etc.) using a L{StreamingContext} or it can be
    generated by transforming existing DStreams using operations such as
    `map`, `window` and `reduceByKeyAndWindow`. While a Spark Streaming
    program is running, each DStream periodically generates a RDD, either
    from live data or by transforming the RDD generated by a parent DStream.

    DStreams internally is characterized by a few basic properties:
     - A list of other DStreams that the DStream depends on
     - A time interval at which the DStream generates an RDD
     - A function that is used to generate an RDD after each time interval
    """
    def __init__(self, jdstream, ssc, jrdd_deserializer):
        self._jdstream = jdstream
        self._ssc = ssc
        self._sc = ssc._sc
        self._jrdd_deserializer = jrdd_deserializer
        self.is_cached = False
        self.is_checkpointed = False

    def context(self):
        """
        Return the StreamingContext associated with this DStream
        """
        return self._ssc

    def count(self):
        """
        Return a new DStream in which each RDD has a single element
        generated by counting each RDD of this DStream.
        """
        return self.mapPartitions(lambda i: [sum(1 for _ in i)]).reduce(operator.add)

    def filter(self, f):
        """
        Return a new DStream containing only the elements that satisfy predicate.
        """
        def func(iterator):
            return filter(f, iterator)
        return self.mapPartitions(func, True)

    def flatMap(self, f, preservesPartitioning=False):
        """
        Return a new DStream by applying a function to all elements of
        this DStream, and then flattening the results
        """
        def func(s, iterator):
            return chain.from_iterable(map(f, iterator))
        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    def map(self, f, preservesPartitioning=False):
        """
        Return a new DStream by applying a function to each element of DStream.
        """
        def func(iterator):
            return map(f, iterator)
        return self.mapPartitions(func, preservesPartitioning)

    def mapPartitions(self, f, preservesPartitioning=False):
        """
        Return a new DStream in which each RDD is generated by applying
        mapPartitions() to each RDDs of this DStream.
        """
        def func(s, iterator):
            return f(iterator)
        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        """
        Return a new DStream in which each RDD is generated by applying
        mapPartitionsWithIndex() to each RDDs of this DStream.
        """
        return self.transform(lambda rdd: rdd.mapPartitionsWithIndex(f, preservesPartitioning))

    def reduce(self, func):
        """
        Return a new DStream in which each RDD has a single element
        generated by reducing each RDD of this DStream.
        """
        return self.map(lambda x: (None, x)).reduceByKey(func, 1).map(lambda x: x[1])

    def reduceByKey(self, func, numPartitions=None):
        """
        Return a new DStream by applying reduceByKey to each RDD.
        """
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism
        return self.combineByKey(lambda x: x, func, func, numPartitions)

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners,
                     numPartitions=None):
        """
        Return a new DStream by applying combineByKey to each RDD.
        """
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism

        def func(rdd):
            return rdd.combineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions)
        return self.transform(func)

    def partitionBy(self, numPartitions, partitionFunc=portable_hash):
        """
        Return a copy of the DStream in which each RDD are partitioned
        using the specified partitioner.
        """
        return self.transform(lambda rdd: rdd.partitionBy(numPartitions, partitionFunc))

    def foreachRDD(self, func):
        """
        Apply a function to each RDD in this DStream.
        """
        if func.__code__.co_argcount == 1:
            old_func = func
            func = lambda t, rdd: old_func(rdd)
        jfunc = TransformFunction(self._sc, func, self._jrdd_deserializer)
        api = self._ssc._jvm.PythonDStream
        api.callForeachRDD(self._jdstream, jfunc)

    def pprint(self, num=10):
        """
        Print the first num elements of each RDD generated in this DStream.

        @param num: the number of elements from the first will be printed.
        """
        def takeAndPrint(time, rdd):
            taken = rdd.take(num + 1)
            print("-------------------------------------------")
            print("Time: %s" % time)
            print("-------------------------------------------")
            for record in taken[:num]:
                print(record)
            if len(taken) > num:
                print("...")
            print()

        self.foreachRDD(takeAndPrint)

    def mapValues(self, f):
        """
        Return a new DStream by applying a map function to the value of
        each key-value pairs in this DStream without changing the key.
        """
        map_values_fn = lambda kv: (kv[0], f(kv[1]))
        return self.map(map_values_fn, preservesPartitioning=True)

    def flatMapValues(self, f):
        """
        Return a new DStream by applying a flatmap function to the value
        of each key-value pairs in this DStream without changing the key.
        """
        flat_map_fn = lambda kv: ((kv[0], x) for x in f(kv[1]))
        return self.flatMap(flat_map_fn, preservesPartitioning=True)

    def glom(self):
        """
        Return a new DStream in which RDD is generated by applying glom()
        to RDD of this DStream.
        """
        def func(iterator):
            yield list(iterator)
        return self.mapPartitions(func)

    def cache(self):
        """
        Persist the RDDs of this DStream with the default storage level
        (C{MEMORY_ONLY_SER}).
        """
        self.is_cached = True
        self.persist(StorageLevel.MEMORY_ONLY_SER)
        return self

    def persist(self, storageLevel):
        """
        Persist the RDDs of this DStream with the given storage level
        """
        self.is_cached = True
        javaStorageLevel = self._sc._getJavaStorageLevel(storageLevel)
        self._jdstream.persist(javaStorageLevel)
        return self

    def checkpoint(self, interval):
        """
        Enable periodic checkpointing of RDDs of this DStream

        @param interval: time in seconds, after each period of that, generated
                         RDD will be checkpointed
        """
        self.is_checkpointed = True
        self._jdstream.checkpoint(self._ssc._jduration(interval))
        return self

    def groupByKey(self, numPartitions=None):
        """
        Return a new DStream by applying groupByKey on each RDD.
        """
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism
        return self.transform(lambda rdd: rdd.groupByKey(numPartitions))

    def countByValue(self):
        """
        Return a new DStream in which each RDD contains the counts of each
        distinct value in each RDD of this DStream.
        """
        return self.map(lambda x: (x, None)).reduceByKey(lambda x, y: None).count()

    def saveAsTextFiles(self, prefix, suffix=None):
        """
        Save each RDD in this DStream as at text file, using string
        representation of elements.
        """
        def saveAsTextFile(t, rdd):
            path = rddToFileName(prefix, suffix, t)
            try:
                rdd.saveAsTextFile(path)
            except Py4JJavaError as e:
                # after recovered from checkpointing, the foreachRDD may
                # be called twice
                if 'FileAlreadyExistsException' not in str(e):
                    raise
        return self.foreachRDD(saveAsTextFile)

    # TODO: uncomment this until we have ssc.pickleFileStream()
    # def saveAsPickleFiles(self, prefix, suffix=None):
    #     """
    #     Save each RDD in this DStream as at binary file, the elements are
    #     serialized by pickle.
    #     """
    #     def saveAsPickleFile(t, rdd):
    #         path = rddToFileName(prefix, suffix, t)
    #         try:
    #             rdd.saveAsPickleFile(path)
    #         except Py4JJavaError as e:
    #             # after recovered from checkpointing, the foreachRDD may
    #             # be called twice
    #             if 'FileAlreadyExistsException' not in str(e):
    #                 raise
    #     return self.foreachRDD(saveAsPickleFile)

    def transform(self, func):
        """
        Return a new DStream in which each RDD is generated by applying a function
        on each RDD of this DStream.

        `func` can have one argument of `rdd`, or have two arguments of
        (`time`, `rdd`)
        """
        if func.__code__.co_argcount == 1:
            oldfunc = func
            func = lambda t, rdd: oldfunc(rdd)
        assert func.__code__.co_argcount == 2, "func should take one or two arguments"
        return TransformedDStream(self, func)

    def transformWith(self, func, other, keepSerializer=False):
        """
        Return a new DStream in which each RDD is generated by applying a function
        on each RDD of this DStream and 'other' DStream.

        `func` can have two arguments of (`rdd_a`, `rdd_b`) or have three
        arguments of (`time`, `rdd_a`, `rdd_b`)
        """
        if func.__code__.co_argcount == 2:
            oldfunc = func
            func = lambda t, a, b: oldfunc(a, b)
        assert func.__code__.co_argcount == 3, "func should take two or three arguments"
        jfunc = TransformFunction(self._sc, func, self._jrdd_deserializer, other._jrdd_deserializer)
        dstream = self._sc._jvm.PythonTransformed2DStream(self._jdstream.dstream(),
                                                          other._jdstream.dstream(), jfunc)
        jrdd_serializer = self._jrdd_deserializer if keepSerializer else self._sc.serializer
        return DStream(dstream.asJavaDStream(), self._ssc, jrdd_serializer)

    def repartition(self, numPartitions):
        """
        Return a new DStream with an increased or decreased level of parallelism.
        """
        return self.transform(lambda rdd: rdd.repartition(numPartitions))

    @property
    def _slideDuration(self):
        """
        Return the slideDuration in seconds of this DStream
        """
        return self._jdstream.dstream().slideDuration().milliseconds() / 1000.0

    def union(self, other):
        """
        Return a new DStream by unifying data of another DStream with this DStream.

        @param other: Another DStream having the same interval (i.e., slideDuration)
                     as this DStream.
        """
        if self._slideDuration != other._slideDuration:
            raise ValueError("the two DStream should have same slide duration")
        return self.transformWith(lambda a, b: a.union(b), other, True)

    def cogroup(self, other, numPartitions=None):
        """
        Return a new DStream by applying 'cogroup' between RDDs of this
        DStream and `other` DStream.

        Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
        """
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism
        return self.transformWith(lambda a, b: a.cogroup(b, numPartitions), other)

    def join(self, other, numPartitions=None):
        """
        Return a new DStream by applying 'join' between RDDs of this DStream and
        `other` DStream.

        Hash partitioning is used to generate the RDDs with `numPartitions`
        partitions.
        """
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism
        return self.transformWith(lambda a, b: a.join(b, numPartitions), other)

    def leftOuterJoin(self, other, numPartitions=None):
        """
        Return a new DStream by applying 'left outer join' between RDDs of this DStream and
        `other` DStream.

        Hash partitioning is used to generate the RDDs with `numPartitions`
        partitions.
        """
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism
        return self.transformWith(lambda a, b: a.leftOuterJoin(b, numPartitions), other)

    def rightOuterJoin(self, other, numPartitions=None):
        """
        Return a new DStream by applying 'right outer join' between RDDs of this DStream and
        `other` DStream.

        Hash partitioning is used to generate the RDDs with `numPartitions`
        partitions.
        """
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism
        return self.transformWith(lambda a, b: a.rightOuterJoin(b, numPartitions), other)

    def fullOuterJoin(self, other, numPartitions=None):
        """
        Return a new DStream by applying 'full outer join' between RDDs of this DStream and
        `other` DStream.

        Hash partitioning is used to generate the RDDs with `numPartitions`
        partitions.
        """
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism
        return self.transformWith(lambda a, b: a.fullOuterJoin(b, numPartitions), other)

    def _jtime(self, timestamp):
        """ Convert datetime or unix_timestamp into Time
        """
        if isinstance(timestamp, datetime):
            timestamp = time.mktime(timestamp.timetuple())
        return self._sc._jvm.Time(long(timestamp * 1000))

    def slice(self, begin, end):
        """
        Return all the RDDs between 'begin' to 'end' (both included)

        `begin`, `end` could be datetime.datetime() or unix_timestamp
        """
        jrdds = self._jdstream.slice(self._jtime(begin), self._jtime(end))
        return [RDD(jrdd, self._sc, self._jrdd_deserializer) for jrdd in jrdds]

    def _validate_window_param(self, window, slide):
        duration = self._jdstream.dstream().slideDuration().milliseconds()
        if int(window * 1000) % duration != 0:
            raise ValueError("windowDuration must be multiple of the slide duration (%d ms)"
                             % duration)
        if slide and int(slide * 1000) % duration != 0:
            raise ValueError("slideDuration must be multiple of the slide duration (%d ms)"
                             % duration)

    def window(self, windowDuration, slideDuration=None):
        """
        Return a new DStream in which each RDD contains all the elements in seen in a
        sliding window of time over this DStream.

        @param windowDuration: width of the window; must be a multiple of this DStream's
                              batching interval
        @param slideDuration:  sliding interval of the window (i.e., the interval after which
                              the new DStream will generate RDDs); must be a multiple of this
                              DStream's batching interval
        """
        self._validate_window_param(windowDuration, slideDuration)
        d = self._ssc._jduration(windowDuration)
        if slideDuration is None:
            return DStream(self._jdstream.window(d), self._ssc, self._jrdd_deserializer)
        s = self._ssc._jduration(slideDuration)
        return DStream(self._jdstream.window(d, s), self._ssc, self._jrdd_deserializer)

    def reduceByWindow(self, reduceFunc, invReduceFunc, windowDuration, slideDuration):
        """
        Return a new DStream in which each RDD has a single element generated by reducing all
        elements in a sliding window over this DStream.

        if `invReduceFunc` is not None, the reduction is done incrementally
        using the old window's reduced value :

        1. reduce the new values that entered the window (e.g., adding new counts)

        2. "inverse reduce" the old values that left the window (e.g., subtracting old counts)
        This is more efficient than `invReduceFunc` is None.

        @param reduceFunc:     associative reduce function
        @param invReduceFunc:  inverse reduce function of `reduceFunc`
        @param windowDuration: width of the window; must be a multiple of this DStream's
                               batching interval
        @param slideDuration:  sliding interval of the window (i.e., the interval after which
                               the new DStream will generate RDDs); must be a multiple of this
                               DStream's batching interval
        """
        keyed = self.map(lambda x: (1, x))
        reduced = keyed.reduceByKeyAndWindow(reduceFunc, invReduceFunc,
                                             windowDuration, slideDuration, 1)
        return reduced.map(lambda kv: kv[1])

    def countByWindow(self, windowDuration, slideDuration):
        """
        Return a new DStream in which each RDD has a single element generated
        by counting the number of elements in a window over this DStream.
        windowDuration and slideDuration are as defined in the window() operation.

        This is equivalent to window(windowDuration, slideDuration).count(),
        but will be more efficient if window is large.
        """
        return self.map(lambda x: 1).reduceByWindow(operator.add, operator.sub,
                                                    windowDuration, slideDuration)

    def countByValueAndWindow(self, windowDuration, slideDuration, numPartitions=None):
        """
        Return a new DStream in which each RDD contains the count of distinct elements in
        RDDs in a sliding window over this DStream.

        @param windowDuration: width of the window; must be a multiple of this DStream's
                              batching interval
        @param slideDuration:  sliding interval of the window (i.e., the interval after which
                              the new DStream will generate RDDs); must be a multiple of this
                              DStream's batching interval
        @param numPartitions:  number of partitions of each RDD in the new DStream.
        """
        keyed = self.map(lambda x: (x, 1))
        counted = keyed.reduceByKeyAndWindow(operator.add, operator.sub,
                                             windowDuration, slideDuration, numPartitions)
        return counted.filter(lambda kv: kv[1] > 0).count()

    def groupByKeyAndWindow(self, windowDuration, slideDuration, numPartitions=None):
        """
        Return a new DStream by applying `groupByKey` over a sliding window.
        Similar to `DStream.groupByKey()`, but applies it over a sliding window.

        @param windowDuration: width of the window; must be a multiple of this DStream's
                              batching interval
        @param slideDuration:  sliding interval of the window (i.e., the interval after which
                              the new DStream will generate RDDs); must be a multiple of this
                              DStream's batching interval
        @param numPartitions:  Number of partitions of each RDD in the new DStream.
        """
        ls = self.mapValues(lambda x: [x])
        grouped = ls.reduceByKeyAndWindow(lambda a, b: a.extend(b) or a, lambda a, b: a[len(b):],
                                          windowDuration, slideDuration, numPartitions)
        return grouped.mapValues(ResultIterable)

    def reduceByKeyAndWindow(self, func, invFunc, windowDuration, slideDuration=None,
                             numPartitions=None, filterFunc=None):
        """
        Return a new DStream by applying incremental `reduceByKey` over a sliding window.

        The reduced value of over a new window is calculated using the old window's reduce value :
         1. reduce the new values that entered the window (e.g., adding new counts)
         2. "inverse reduce" the old values that left the window (e.g., subtracting old counts)

        `invFunc` can be None, then it will reduce all the RDDs in window, could be slower
        than having `invFunc`.

        @param reduceFunc:     associative reduce function
        @param invReduceFunc:  inverse function of `reduceFunc`
        @param windowDuration: width of the window; must be a multiple of this DStream's
                              batching interval
        @param slideDuration:  sliding interval of the window (i.e., the interval after which
                              the new DStream will generate RDDs); must be a multiple of this
                              DStream's batching interval
        @param numPartitions:  number of partitions of each RDD in the new DStream.
        @param filterFunc:     function to filter expired key-value pairs;
                              only pairs that satisfy the function are retained
                              set this to null if you do not want to filter
        """
        self._validate_window_param(windowDuration, slideDuration)
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism

        reduced = self.reduceByKey(func, numPartitions)

        def reduceFunc(t, a, b):
            b = b.reduceByKey(func, numPartitions)
            r = a.union(b).reduceByKey(func, numPartitions) if a else b
            if filterFunc:
                r = r.filter(filterFunc)
            return r

        def invReduceFunc(t, a, b):
            b = b.reduceByKey(func, numPartitions)
            joined = a.leftOuterJoin(b, numPartitions)
            return joined.mapValues(lambda kv: invFunc(kv[0], kv[1])
                                    if kv[1] is not None else kv[0])

        jreduceFunc = TransformFunction(self._sc, reduceFunc, reduced._jrdd_deserializer)
        if invReduceFunc:
            jinvReduceFunc = TransformFunction(self._sc, invReduceFunc, reduced._jrdd_deserializer)
        else:
            jinvReduceFunc = None
        if slideDuration is None:
            slideDuration = self._slideDuration
        dstream = self._sc._jvm.PythonReducedWindowedDStream(reduced._jdstream.dstream(),
                                                             jreduceFunc, jinvReduceFunc,
                                                             self._ssc._jduration(windowDuration),
                                                             self._ssc._jduration(slideDuration))
        return DStream(dstream.asJavaDStream(), self._ssc, self._sc.serializer)

    def updateStateByKey(self, updateFunc, numPartitions=None):
        """
        Return a new "state" DStream where the state for each key is updated by applying
        the given function on the previous state of the key and the new values of the key.

        @param updateFunc: State update function. If this function returns None, then
                           corresponding state key-value pair will be eliminated.
        """
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism

        def reduceFunc(t, a, b):
            if a is None:
                g = b.groupByKey(numPartitions).mapValues(lambda vs: (list(vs), None))
            else:
                g = a.cogroup(b.partitionBy(numPartitions), numPartitions)
                g = g.mapValues(lambda ab: (list(ab[1]), list(ab[0])[0] if len(ab[0]) else None))
            state = g.mapValues(lambda vs_s: updateFunc(vs_s[0], vs_s[1]))
            return state.filter(lambda k_v: k_v[1] is not None)

        jreduceFunc = TransformFunction(self._sc, reduceFunc,
                                        self._sc.serializer, self._jrdd_deserializer)
        dstream = self._sc._jvm.PythonStateDStream(self._jdstream.dstream(), jreduceFunc)
        return DStream(dstream.asJavaDStream(), self._ssc, self._sc.serializer)


class TransformedDStream(DStream):
    """
    TransformedDStream is an DStream generated by an Python function
    transforming each RDD of an DStream to another RDDs.

    Multiple continuous transformations of DStream can be combined into
    one transformation.
    """
    def __init__(self, prev, func):
        self._ssc = prev._ssc
        self._sc = self._ssc._sc
        self._jrdd_deserializer = self._sc.serializer
        self.is_cached = False
        self.is_checkpointed = False
        self._jdstream_val = None

        if (isinstance(prev, TransformedDStream) and
                not prev.is_cached and not prev.is_checkpointed):
            prev_func = prev.func
            self.func = lambda t, rdd: func(t, prev_func(t, rdd))
            self.prev = prev.prev
        else:
            self.prev = prev
            self.func = func

    @property
    def _jdstream(self):
        if self._jdstream_val is not None:
            return self._jdstream_val

        jfunc = TransformFunction(self._sc, self.func, self.prev._jrdd_deserializer)
        dstream = self._sc._jvm.PythonTransformedDStream(self.prev._jdstream.dstream(), jfunc)
        self._jdstream_val = dstream.asJavaDStream()
        return self._jdstream_val
