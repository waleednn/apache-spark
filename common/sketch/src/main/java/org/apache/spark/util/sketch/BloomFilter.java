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

package org.apache.spark.util.sketch;

/**
 * A Bloom filter is a space-efficient probabilistic data structure, that is used to test whether
 * an element is a member of a set. It returns false when the element is definitely not in the
 * set, returns true when the element is probably in the set.
 *
 * Internally a Bloom filter is initialized with 2 information: how many space to use(number of
 * bits) and how many hash values to calculate for each record.  To get as lower false positive
 * probability as possible, user should call {@link BloomFilter#create} to automatically pick a
 * best combination of these 2 parameters.
 *
 * Currently the following data types are supported:
 * <ul>
 *   <li>{@link Byte}</li>
 *   <li>{@link Short}</li>
 *   <li>{@link Integer}</li>
 *   <li>{@link Long}</li>
 *   <li>{@link String}</li>
 * </ul>
 *
 * The implementation is largely based on the {@code BloomFilter} class from guava.
 */
public abstract class BloomFilter {
  /**
   * Returns the probability that {@linkplain #mightContain(Object)} will erroneously return
   * {@code true} for an object that has not actually been put in the {@code BloomFilter}.
   *
   * <p>Ideally, this number should be close to the {@code fpp} parameter
   * passed in to create this bloom filter, or smaller. If it is
   * significantly higher, it is usually the case that too many elements (more than
   * expected) have been put in the {@code BloomFilter}, degenerating it.
   */
  public abstract double expectedFpp();

  /**
   * Returns the number of bits in the underlying bit array.
   */
  public abstract long bitSize();

  /**
   * Puts an element into this {@code BloomFilter}. Ensures that subsequent invocations of
   * {@link #mightContain(Object)} with the same element will always return {@code true}.
   *
   * @return true if the bloom filter's bits changed as a result of this operation. If the bits
   *     changed, this is <i>definitely</i> the first time {@code object} has been added to the
   *     filter. If the bits haven't changed, this <i>might</i> be the first time {@code object}
   *     has been added to the filter. Note that {@code put(t)} always returns the
   *     <i>opposite</i> result to what {@code mightContain(t)} would have returned at the time
   *     it is called."
   */
  public abstract boolean put(Object item);

  /**
   * Determines whether a given bloom filter is compatible with this bloom filter. For two
   * bloom filters to be compatible, they must have the same bit size.
   *
   * @param other The bloom filter to check for compatibility.
   */
  public abstract boolean isCompatible(BloomFilter other);

  /**
   * Combines this bloom filter with another bloom filter by performing a bitwise OR of the
   * underlying data. The mutations happen to <b>this</b> instance. Callers must ensure the
   * bloom filters are appropriately sized to avoid saturating them.
   *
   * @param other The bloom filter to combine this bloom filter with. It is not mutated.
   * @throws IllegalArgumentException if {@code isCompatible(that) == false}
   */
  public abstract BloomFilter mergeInPlace(BloomFilter other);

  /**
   * Returns {@code true} if the element <i>might</i> have been put in this Bloom filter,
   * {@code false} if this is <i>definitely</i> not the case.
   */
  public abstract boolean mightContain(Object item);

  /**
   * Computes the optimal k (number of hashes per element inserted in Bloom filter), given the
   * expected insertions and total number of bits in the Bloom filter.
   *
   * See http://en.wikipedia.org/wiki/File:Bloom_filter_fp_probability.svg for the formula.
   *
   * @param n expected insertions (must be positive)
   * @param m total number of bits in Bloom filter (must be positive)
   */
  private static int optimalNumOfHashFunctions(long n, long m) {
    // (m / n) * log(2), but avoid truncation due to division!
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }

  /**
   * Computes m (total bits of Bloom filter) which is expected to achieve, for the specified
   * expected insertions, the required false positive probability.
   *
   * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for the formula.
   *
   * @param n expected insertions (must be positive)
   * @param p false positive rate (must be 0 < p < 1)
   */
  private static long optimalNumOfBits(long n, double p) {
    if (p == 0) {
      p = Double.MIN_VALUE;
    }
    return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  public static BloomFilter create(long expectedInsertions) {
    return create(expectedInsertions, 0.03);
  }

  public static BloomFilter create(long expectedInsertions, double fpp) {
    assert fpp > 0.0 : "False positive probability must be > 0.0";
    assert fpp < 1.0 : "False positive probability must be < 1.0";
    long numBits = optimalNumOfBits(expectedInsertions, fpp);
    return create(expectedInsertions, numBits);
  }

  public static BloomFilter create(long expectedInsertions, long numBits) {
    assert expectedInsertions > 0 : "Expected insertions must be > 0";
    assert numBits > 0 : "number of bits must be > 0";
    int numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits);
    return new DefaultBloomFilter(numHashFunctions, numBits);
  }
}
