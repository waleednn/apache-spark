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
package org.apache.spark.sql.catalyst.util;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

import com.ibm.icu.util.ULocale;
import com.ibm.icu.text.Collator;

import org.apache.spark.SparkException;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Static entry point for collation aware string functions.
 * Provides functionality to the UTF8String object which respects defined collation settings.
 */
public final class CollationFactory {
  /**
   * Entry encapsulating all information about a collation.
   */
  public static class Collation {
    public final String collationName;
    public final Collator collator;
    public final Comparator<UTF8String> comparator;

    /**
     * Version of the collation. This is the version of the ICU library used to create the collator.
     * For non-ICU collations (e.g. UTF8 Binary) the version is set to "1.0".
     */
    public final String version;

    /**
     * Collation sensitive hash function. Output for two UTF8Strings will be the same if they are
     * equal according to the collation.
     */
    public final ToLongFunction<UTF8String> hashFunction;

    /**
     * Potentially faster way than using comparator to compare two UTF8Strings for equality.
     * Falls back to binary comparison if the collation is binary.
     */
    public final BiFunction<UTF8String, UTF8String, Boolean> equalsFunction;

    /**
     * Binary collation implies that UTF8Strings are considered equal only if they are
     * byte for byte equal. All accent or case-insensitive collations are considered non-binary.
     */
    public final boolean isBinaryCollation;

    public Collation(
      String collationName,
      Collator collator,
      Comparator<UTF8String> comparator,
      String version,
      ToLongFunction<UTF8String> hashFunction,
      boolean isBinaryCollation) {
      this.collationName = collationName;
      this.collator = collator;
      this.comparator = comparator;
      this.version = version;
      this.hashFunction = hashFunction;
      this.isBinaryCollation = isBinaryCollation;

      if (isBinaryCollation) {
        this.equalsFunction = UTF8String::equals;
      } else {
        this.equalsFunction = (s1, s2) -> this.comparator.compare(s1, s2) == 0;
      }
    }

    /**
     * Constructor with comparators that are inherited from the given collator.
     */
    public Collation(
      String collationName, Collator collator, String version, boolean isBinaryCollation) {
      this(
        collationName,
        collator,
        (s1, s2) -> collator.compare(s1.toString(), s2.toString()),
        version,
        s -> (long)collator.getCollationKey(s.toString()).hashCode(),
        isBinaryCollation);
    }
  }

  private final Collation[] collatorTable;
  private final HashMap<String, Integer> collationNameToIdMap = new HashMap<>();

  private CollationFactory() {
    collatorTable = new Collation[4];

    // Binary comparison. This is the default collation.
    // No custom comparators will be used for this collation.
    // Instead, we rely on byte for byte comparison.
    collatorTable[0] = new Collation(
      "UCS_BASIC",
      null,
      UTF8String::compareTo,
      "1.0",
      s -> (long)s.hashCode(),
      true);

    // Case-insensitive UTF8 binary collation.
    // TODO: Do in place comparisons instead of creating new strings.
    collatorTable[1] = new Collation(
      "UCS_BASIC_LCASE",
      null,
            Comparator.comparing(UTF8String::toLowerCase), "1.0",
      (s) -> (long)s.toLowerCase().hashCode(),
      false);

    // UNICODE case sensitive comparison (ROOT locale, in ICU).
    collatorTable[2] = new Collation(
      "UNICODE", Collator.getInstance(ULocale.ROOT), "153.120.0.0", true);
    collatorTable[2].collator.setStrength(Collator.TERTIARY);


    // UNICODE case-insensitive comparison (ROOT locale, in ICU + Secondary strength).
    collatorTable[3] = new Collation(
            "UNICODE_CI", Collator.getInstance(ULocale.ROOT), "153.120.0.0", false);
    collatorTable[3].collator.setStrength(Collator.SECONDARY);

    for (int i = 0; i < collatorTable.length; i++) {
      this.collationNameToIdMap.put(collatorTable[i].collationName, i);
    }
  }

  private static final CollationFactory instance = new CollationFactory();

  /**
   * Returns the collation id for the given collation name.
   */
  public int collationNameToId(String collationName) throws SparkException {
    String normalizedName = collationName.toUpperCase();
    if (collationNameToIdMap.containsKey(normalizedName)) {
      return collationNameToIdMap.get(normalizedName);
    } else {
      throw new SparkException(
       "COLLATION_INVALID_NAME",
        SparkException.constructMessageParams(
          Collections.singletonMap("collationName", collationName)), null);
    }
  }

  public Collation fetchCollation(int collationId) {
    return collatorTable[collationId];
  }

  public Collation fetchCollation(String collationName) throws SparkException {
    int collationId = collationNameToId(collationName);
    return collatorTable[collationId];
  }

  public static CollationFactory getInstance() {
    return instance;
  }
}
