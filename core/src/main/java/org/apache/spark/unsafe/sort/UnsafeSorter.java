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

package org.apache.spark.unsafe.sort;

import java.util.Comparator;

import org.apache.spark.util.collection.Sorter;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import static org.apache.spark.unsafe.sort.UnsafeSortDataFormat.RecordPointerAndKeyPrefix;

/**
 * Sorts records using an AlphaSort-style key-prefix sort. This sort stores pointers to records
 * alongside a user-defined prefix of the record's sorting key. When the underlying sort algorithm
 * compares records, it will first compare the stored key prefixes; if the prefixes are not equal,
 * then we do not need to traverse the record pointers to compare the actual records. Avoiding these
 * random memory accesses improves cache hit rates.
 */
public final class UnsafeSorter {

  private final TaskMemoryManager memoryManager;
  private final Sorter<RecordPointerAndKeyPrefix, long[]> sorter;
  private final Comparator<RecordPointerAndKeyPrefix> sortComparator;

  /**
   * Within this buffer, position {@code 2 * i} holds a pointer pointer to the record at
   * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
   */
  private long[] sortBuffer;

  /**
   * The position in the sort buffer where new records can be inserted.
   */
  private int sortBufferInsertPosition = 0;

  public void expandSortBuffer() {
    final long[] oldBuffer = sortBuffer;
    sortBuffer = new long[oldBuffer.length * 2];
    System.arraycopy(oldBuffer, 0, sortBuffer, 0, oldBuffer.length);
  }

  public UnsafeSorter(
      final TaskMemoryManager memoryManager,
      final RecordComparator recordComparator,
      final PrefixComparator prefixComparator,
      int initialSize) {
    assert (initialSize > 0);
    this.sortBuffer = new long[initialSize * 2];
    this.memoryManager = memoryManager;
    this.sorter =
      new Sorter<RecordPointerAndKeyPrefix, long[]>(UnsafeSortDataFormat.INSTANCE);
    this.sortComparator = new Comparator<RecordPointerAndKeyPrefix>() {
      @Override
      public int compare(RecordPointerAndKeyPrefix left, RecordPointerAndKeyPrefix right) {
        final int prefixComparisonResult =
          prefixComparator.compare(left.keyPrefix, right.keyPrefix);
        if (prefixComparisonResult == 0) {
          final Object leftBaseObject = memoryManager.getPage(left.recordPointer);
          final long leftBaseOffset = memoryManager.getOffsetInPage(left.recordPointer);
          final Object rightBaseObject = memoryManager.getPage(right.recordPointer);
          final long rightBaseOffset = memoryManager.getOffsetInPage(right.recordPointer);
          return recordComparator.compare(
            leftBaseObject, leftBaseOffset, rightBaseObject, rightBaseOffset);
        } else {
          return prefixComparisonResult;
        }
      }
    };
  }

  public long getMemoryUsage() {
    return sortBuffer.length * 8L;
  }

  public boolean hasSpaceForAnotherRecord() {
    return sortBufferInsertPosition + 2 < sortBuffer.length;
  }

  /**
   * Insert a record into the sort buffer.
   *
   * @param objectAddress pointer to a record in a data page, encoded by {@link TaskMemoryManager}.
   */
  public void insertRecord(long objectAddress, long keyPrefix) {
    if (!hasSpaceForAnotherRecord()) {
      expandSortBuffer();
    }
    sortBuffer[sortBufferInsertPosition] = objectAddress;
    sortBufferInsertPosition++;
    sortBuffer[sortBufferInsertPosition] = keyPrefix;
    sortBufferInsertPosition++;
  }

  /**
   * Return an iterator over record pointers in sorted order. For efficiency, all calls to
   * {@code next()} will return the same mutable object.
   */
  public UnsafeSorterIterator getSortedIterator() {
    sorter.sort(sortBuffer, 0, sortBufferInsertPosition / 2, sortComparator);
    return new UnsafeSorterIterator() {

      private int position = 0;
      private Object baseObject;
      private long baseOffset;
      private long keyPrefix;
      private int recordLength;

      @Override
      public boolean hasNext() {
        return position < sortBufferInsertPosition;
      }

      @Override
      public void loadNext() {
        final long recordPointer = sortBuffer[position];
        baseObject = memoryManager.getPage(recordPointer);
        baseOffset = memoryManager.getOffsetInPage(recordPointer);
        keyPrefix = sortBuffer[position + 1];
        position += 2;
      }

      @Override
      public Object getBaseObject() { return baseObject; }

      @Override
      public long getBaseOffset() { return baseOffset; }

      @Override
      public int getRecordLength() { return recordLength; }

      @Override
      public long getKeyPrefix() { return keyPrefix; }
    };
  }
}
