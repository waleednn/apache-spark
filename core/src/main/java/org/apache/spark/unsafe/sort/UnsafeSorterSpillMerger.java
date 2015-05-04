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

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

final class UnsafeSorterSpillMerger {

  private final PriorityQueue<UnsafeSorterIterator> priorityQueue;

  public UnsafeSorterSpillMerger(
    final RecordComparator recordComparator,
    final PrefixComparator prefixComparator) {
    final Comparator<UnsafeSorterIterator> comparator = new Comparator<UnsafeSorterIterator>() {

      @Override
      public int compare(UnsafeSorterIterator left, UnsafeSorterIterator right) {
        final int prefixComparisonResult =
          prefixComparator.compare(left.getKeyPrefix(), right.getKeyPrefix());
        if (prefixComparisonResult == 0) {
          return recordComparator.compare(
            left.getBaseObject(), left.getBaseOffset(),
            right.getBaseObject(), right.getBaseOffset());
        } else {
          return prefixComparisonResult;
        }
      }
    };
    // TODO: the size is often known; incorporate size hints here.
    priorityQueue = new PriorityQueue<UnsafeSorterIterator>(10, comparator);
  }

  public void addSpill(UnsafeSorterIterator spillReader) throws IOException {
    if (spillReader.hasNext()) {
      spillReader.loadNext();
    }
    priorityQueue.add(spillReader);
  }

  public UnsafeSorterIterator getSortedIterator() throws IOException {
    return new UnsafeSorterIterator() {

      private UnsafeSorterIterator spillReader;

      @Override
      public boolean hasNext() {
        return !priorityQueue.isEmpty() || (spillReader != null && spillReader.hasNext());
      }

      @Override
      public void loadNext() throws IOException {
        if (spillReader != null) {
          if (spillReader.hasNext()) {
            spillReader.loadNext();
            priorityQueue.add(spillReader);
          }
        }
        spillReader = priorityQueue.remove();
      }

      @Override
      public Object getBaseObject() { return spillReader.getBaseObject(); }

      @Override
      public long getBaseOffset() { return spillReader.getBaseOffset(); }

      @Override
      public int getRecordLength() { return spillReader.getRecordLength(); }

      @Override
      public long getKeyPrefix() { return spillReader.getKeyPrefix(); }
    };
  }
}
