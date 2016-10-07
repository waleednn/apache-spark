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

package org.apache.spark.util.collection.unsafe.sort;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.UUID;

import scala.Tuple2;
import scala.Tuple2$;
import scala.runtime.AbstractFunction1;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.memory.TestMemoryManager;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.Utils;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.*;
import static org.mockito.Answers.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.*;

public class UnsafeExternalSorterSuite {

  final LinkedList<File> spillFilesCreated = new LinkedList<>();
  final TestMemoryManager memoryManager =
    new TestMemoryManager(new SparkConf().set("spark.memory.offHeap.enabled", "false"));
  final TaskMemoryManager taskMemoryManager = new TaskMemoryManager(memoryManager, 0);
  final SerializerManager serializerManager = new SerializerManager(
    new JavaSerializer(new SparkConf()),
    new SparkConf().set("spark.shuffle.spill.compress", "false"));
  // Use integer comparison for comparing prefixes (which are partition ids, in this case)
  final PrefixComparator prefixComparator = PrefixComparators.LONG;
  // Since the key fits within the 8-byte prefix, we don't need to do any record comparison, so
  // use a dummy comparator
  final RecordComparator recordComparator = new RecordComparator() {
    @Override
    public int compare(
      Object leftBaseObject,
      long leftBaseOffset,
      Object rightBaseObject,
      long rightBaseOffset) {
      return 0;
    }
  };

  File tempDir;
  @Mock(answer = RETURNS_SMART_NULLS) BlockManager blockManager;
  @Mock(answer = RETURNS_SMART_NULLS) DiskBlockManager diskBlockManager;
  @Mock(answer = RETURNS_SMART_NULLS) TaskContext taskContext;

  protected boolean shouldUseRadixSort() { return false; }

  private final long pageSizeBytes = new SparkConf().getSizeAsBytes("spark.buffer.pageSize", "4m");

  private static final class WrapStream extends AbstractFunction1<OutputStream, OutputStream> {
    @Override
    public OutputStream apply(OutputStream stream) {
      return stream;
    }
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "unsafe-test");
    spillFilesCreated.clear();
    taskContext = mock(TaskContext.class);
    when(taskContext.taskMetrics()).thenReturn(new TaskMetrics());
    when(blockManager.diskBlockManager()).thenReturn(diskBlockManager);
    when(diskBlockManager.createTempLocalBlock()).thenAnswer(
        new Answer<Tuple2<TempLocalBlockId, File>>() {
      @Override
      public Tuple2<TempLocalBlockId, File> answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        TempLocalBlockId blockId = new TempLocalBlockId(UUID.randomUUID());
        File file = File.createTempFile("spillFile", ".spill", tempDir);
        spillFilesCreated.add(file);
        return Tuple2$.MODULE$.apply(blockId, file);
      }
    });
    when(blockManager.getDiskWriter(
      any(BlockId.class),
      any(File.class),
      any(SerializerInstance.class),
      anyInt(),
      any(ShuffleWriteMetrics.class))).thenAnswer(new Answer<DiskBlockObjectWriter>() {
      @Override
      public DiskBlockObjectWriter answer(InvocationOnMock invocationOnMock) throws Throwable {
        Object[] args = invocationOnMock.getArguments();

        return new DiskBlockObjectWriter(
          (File) args[1],
          (SerializerInstance) args[2],
          (Integer) args[3],
          new WrapStream(),
          false,
          (ShuffleWriteMetrics) args[4],
          (BlockId) args[0]
        );
      }
    });
  }

  @After
  public void tearDown() {
    try {
      assertEquals(0L, taskMemoryManager.cleanUpAllAllocatedMemory());
    } finally {
      Utils.deleteRecursively(tempDir);
      tempDir = null;
    }
  }

  private void assertSpillFilesWereCleanedUp() {
    for (File spillFile : spillFilesCreated) {
      assertFalse("Spill file " + spillFile.getPath() + " was not cleaned up",
        spillFile.exists());
    }
  }

  private static void insertNumber(UnsafeExternalSorter sorter, int value) throws Exception {
    final int[] arr = new int[]{ value };
    sorter.insertRecord(arr, Platform.INT_ARRAY_OFFSET, 4, value, false);
  }

  private static void insertRecord(
      UnsafeExternalSorter sorter,
      int[] record,
      long prefix) throws IOException {
    sorter.insertRecord(record, Platform.INT_ARRAY_OFFSET, record.length * 4, prefix, false);
  }

  private UnsafeExternalSorter newSorter() throws IOException {
    return UnsafeExternalSorter.create(
      taskMemoryManager,
      blockManager,
      serializerManager,
      taskContext,
      recordComparator,
      prefixComparator,
      /* initialSize */ 1024,
      pageSizeBytes,
      UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD,
      shouldUseRadixSort());
  }

  @Test
  public void testSortingOnlyByPrefix() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    insertNumber(sorter, 5);
    insertNumber(sorter, 1);
    insertNumber(sorter, 3);
    sorter.spill();
    insertNumber(sorter, 4);
    sorter.spill();
    insertNumber(sorter, 2);

    UnsafeSorterIterator iter = sorter.getSortedIterator();

    for (int i = 1; i <= 5; i++) {
      iter.loadNext();
      assertEquals(i, iter.getKeyPrefix());
      assertEquals(4, iter.getRecordLength());
      assertEquals(i, Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));
    }

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testSortingEmptyArrays() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    sorter.insertRecord(null, 0, 0, 0, false);
    sorter.insertRecord(null, 0, 0, 0, false);
    sorter.spill();
    sorter.insertRecord(null, 0, 0, 0, false);
    sorter.spill();
    sorter.insertRecord(null, 0, 0, 0, false);
    sorter.insertRecord(null, 0, 0, 0, false);

    UnsafeSorterIterator iter = sorter.getSortedIterator();

    for (int i = 1; i <= 5; i++) {
      iter.loadNext();
      assertEquals(0, iter.getKeyPrefix());
      assertEquals(0, iter.getRecordLength());
    }

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testSortTimeMetric() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    long prevSortTime = sorter.getSortTimeNanos();
    assertEquals(prevSortTime, 0);

    sorter.insertRecord(null, 0, 0, 0, false);
    sorter.spill();
    assertThat(sorter.getSortTimeNanos(), greaterThan(prevSortTime));
    prevSortTime = sorter.getSortTimeNanos();

    sorter.spill();  // no sort needed
    assertEquals(sorter.getSortTimeNanos(), prevSortTime);

    sorter.insertRecord(null, 0, 0, 0, false);
    UnsafeSorterIterator iter = sorter.getSortedIterator();
    assertThat(sorter.getSortTimeNanos(), greaterThan(prevSortTime));
  }

  @Test
  public void spillingOccursInResponseToMemoryPressure() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    // This should be enough records to completely fill up a data page:
    final int numRecords = (int) (pageSizeBytes / (4 + 4));
    for (int i = 0; i < numRecords; i++) {
      insertNumber(sorter, numRecords - i);
    }
    assertEquals(1, sorter.getNumberOfAllocatedPages());
    memoryManager.markExecutionAsOutOfMemoryOnce();
    // The insertion of this record should trigger a spill:
    insertNumber(sorter, 0);
    // Ensure that spill files were created
    assertThat(tempDir.listFiles().length, greaterThanOrEqualTo(1));
    // Read back the sorted data:
    UnsafeSorterIterator iter = sorter.getSortedIterator();

    int i = 0;
    while (iter.hasNext()) {
      iter.loadNext();
      assertEquals(i, iter.getKeyPrefix());
      assertEquals(4, iter.getRecordLength());
      assertEquals(i, Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));
      i++;
    }
    assertEquals(numRecords + 1, i);
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testFillingPage() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    byte[] record = new byte[16];
    while (sorter.getNumberOfAllocatedPages() < 2) {
      sorter.insertRecord(record, Platform.BYTE_ARRAY_OFFSET, record.length, 0, false);
    }
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void sortingRecordsThatExceedPageSize() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    final int[] largeRecord = new int[(int) pageSizeBytes + 16];
    Arrays.fill(largeRecord, 456);
    final int[] smallRecord = new int[100];
    Arrays.fill(smallRecord, 123);

    insertRecord(sorter, largeRecord, 456);
    sorter.spill();
    insertRecord(sorter, smallRecord, 123);
    sorter.spill();
    insertRecord(sorter, smallRecord, 123);
    insertRecord(sorter, largeRecord, 456);

    UnsafeSorterIterator iter = sorter.getSortedIterator();
    // Small record
    assertTrue(iter.hasNext());
    iter.loadNext();
    assertEquals(123, iter.getKeyPrefix());
    assertEquals(smallRecord.length * 4, iter.getRecordLength());
    assertEquals(123, Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));
    // Small record
    assertTrue(iter.hasNext());
    iter.loadNext();
    assertEquals(123, iter.getKeyPrefix());
    assertEquals(smallRecord.length * 4, iter.getRecordLength());
    assertEquals(123, Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));
    // Large record
    assertTrue(iter.hasNext());
    iter.loadNext();
    assertEquals(456, iter.getKeyPrefix());
    assertEquals(largeRecord.length * 4, iter.getRecordLength());
    assertEquals(456, Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));
    // Large record
    assertTrue(iter.hasNext());
    iter.loadNext();
    assertEquals(456, iter.getKeyPrefix());
    assertEquals(largeRecord.length * 4, iter.getRecordLength());
    assertEquals(456, Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));

    assertFalse(iter.hasNext());
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void forcedSpillingWithReadIterator() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    long[] record = new long[100];
    int recordSize = record.length * 8;
    int n = (int) pageSizeBytes / recordSize * 3;
    for (int i = 0; i < n; i++) {
      record[0] = (long) i;
      sorter.insertRecord(record, Platform.LONG_ARRAY_OFFSET, recordSize, 0, false);
    }
    assertTrue(sorter.getNumberOfAllocatedPages() >= 2);
    UnsafeExternalSorter.SpillableIterator iter =
      (UnsafeExternalSorter.SpillableIterator) sorter.getSortedIterator();
    int lastv = 0;
    for (int i = 0; i < n / 3; i++) {
      iter.hasNext();
      iter.loadNext();
      assertTrue(Platform.getLong(iter.getBaseObject(), iter.getBaseOffset()) == i);
      lastv = i;
    }
    assertTrue(iter.spill() > 0);
    assertEquals(0, iter.spill());
    assertTrue(Platform.getLong(iter.getBaseObject(), iter.getBaseOffset()) == lastv);
    for (int i = n / 3; i < n; i++) {
      iter.hasNext();
      iter.loadNext();
      assertEquals(i, Platform.getLong(iter.getBaseObject(), iter.getBaseOffset()));
    }
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void forcedSpillingWithNotReadIterator() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    long[] record = new long[100];
    int recordSize = record.length * 8;
    int n = (int) pageSizeBytes / recordSize * 3;
    for (int i = 0; i < n; i++) {
      record[0] = (long) i;
      sorter.insertRecord(record, Platform.LONG_ARRAY_OFFSET, recordSize, 0, false);
    }
    assertTrue(sorter.getNumberOfAllocatedPages() >= 2);
    UnsafeExternalSorter.SpillableIterator iter =
      (UnsafeExternalSorter.SpillableIterator) sorter.getSortedIterator();
    assertTrue(iter.spill() > 0);
    assertEquals(0, iter.spill());
    for (int i = 0; i < n; i++) {
      iter.hasNext();
      iter.loadNext();
      assertEquals(i, Platform.getLong(iter.getBaseObject(), iter.getBaseOffset()));
    }
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void forcedSpillingWithoutComparator() throws Exception {
    final UnsafeExternalSorter sorter = UnsafeExternalSorter.create(
      taskMemoryManager,
      blockManager,
      serializerManager,
      taskContext,
      null,
      null,
      /* initialSize */ 1024,
      pageSizeBytes,
      UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD,
      shouldUseRadixSort());
    long[] record = new long[100];
    int recordSize = record.length * 8;
    int n = (int) pageSizeBytes / recordSize * 3;
    int batch = n / 4;
    for (int i = 0; i < n; i++) {
      record[0] = (long) i;
      sorter.insertRecord(record, Platform.LONG_ARRAY_OFFSET, recordSize, 0, false);
      if (i % batch == batch - 1) {
        sorter.spill();
      }
    }
    UnsafeSorterIterator iter = sorter.getIterator();
    for (int i = 0; i < n; i++) {
      iter.hasNext();
      iter.loadNext();
      assertEquals(i, Platform.getLong(iter.getBaseObject(), iter.getBaseOffset()));
    }
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testPeakMemoryUsed() throws Exception {
    final long recordLengthBytes = 8;
    final long pageSizeBytes = 256;
    final long numRecordsPerPage = pageSizeBytes / recordLengthBytes;
    final UnsafeExternalSorter sorter = UnsafeExternalSorter.create(
      taskMemoryManager,
      blockManager,
      serializerManager,
      taskContext,
      recordComparator,
      prefixComparator,
      1024,
      pageSizeBytes,
      UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD,
      shouldUseRadixSort());

    // Peak memory should be monotonically increasing. More specifically, every time
    // we allocate a new page it should increase by exactly the size of the page.
    long previousPeakMemory = sorter.getPeakMemoryUsedBytes();
    long newPeakMemory;
    try {
      for (int i = 0; i < numRecordsPerPage * 10; i++) {
        insertNumber(sorter, i);
        newPeakMemory = sorter.getPeakMemoryUsedBytes();
        if (i % numRecordsPerPage == 0) {
          // We allocated a new page for this record, so peak memory should change
          assertEquals(previousPeakMemory + pageSizeBytes, newPeakMemory);
        } else {
          assertEquals(previousPeakMemory, newPeakMemory);
        }
        previousPeakMemory = newPeakMemory;
      }

      // Spilling should not change peak memory
      sorter.spill();
      newPeakMemory = sorter.getPeakMemoryUsedBytes();
      assertEquals(previousPeakMemory, newPeakMemory);
      for (int i = 0; i < numRecordsPerPage; i++) {
        insertNumber(sorter, i);
      }
      newPeakMemory = sorter.getPeakMemoryUsedBytes();
      assertEquals(previousPeakMemory, newPeakMemory);
    } finally {
      sorter.cleanupResources();
      assertSpillFilesWereCleanedUp();
    }
  }

}

