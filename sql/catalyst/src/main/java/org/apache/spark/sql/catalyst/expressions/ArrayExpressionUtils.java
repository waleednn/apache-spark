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
package org.apache.spark.sql.catalyst.expressions;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.SQLOrderingUtil;
import org.apache.spark.sql.types.ByteType$;
import org.apache.spark.sql.types.BooleanType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.ShortType$;

public class ArrayExpressionUtils {

  private static final Comparator<Object> booleanComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    boolean c1 = (Boolean) o1, c2 = (Boolean) o2;
    return c1 == c2 ? 0 : (c1 ? 1 : -1);
  };

  private static final Comparator<Object> byteComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    byte c1 = (Byte) o1, c2 = (Byte) o2;
    return Byte.compare(c1, c2);
  };

  private static final Comparator<Object> shortComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    short c1 = (Short) o1, c2 = (Short) o2;
    return Short.compare(c1, c2);
  };

  private static final Comparator<Object> integerComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    int c1 = (Integer) o1, c2 = (Integer) o2;
    return Integer.compare(c1, c2);
  };

  private static final Comparator<Object> longComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    long c1 = (Long) o1, c2 = (Long) o2;
    return Long.compare(c1, c2);
  };

  private static final Comparator<Object> floatComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    float c1 = (Float) o1, c2 = (Float) o2;
    return SQLOrderingUtil.compareFloats(c1, c2);
  };

  private static final Comparator<Object> doubleComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    double c1 = (Double) o1, c2 = (Double) o2;
    return SQLOrderingUtil.compareDoubles(c1, c2);
  };

  // boolean
  // foldable optimize
  public static int binarySearchNullSafe(Boolean[] data, Boolean value) {
    return Arrays.binarySearch(data, value, booleanComp);
  }

  public static int binarySearchNullSafe(ArrayData data, Boolean value) {
    return Arrays.binarySearch(data.toObjectArray(BooleanType$.MODULE$), value, booleanComp);
  }

  // byte
  // foldable optimize
  public static int binarySearch(byte[] data, byte value) {
    return Arrays.binarySearch(data, value);
  }

  public static int binarySearch(ArrayData data, byte value) {
    return Arrays.binarySearch(data.toByteArray(), value);
  }

  // foldable optimize
  public static int binarySearchNullSafe(Byte[] data, Byte value) {
    return Arrays.binarySearch(data, value, byteComp);
  }

  public static int binarySearchNullSafe(ArrayData data, Byte value) {
    return Arrays.binarySearch(data.toObjectArray(ByteType$.MODULE$), value, byteComp);
  }

  // short
  // foldable optimize
  public static int binarySearch(short[] data, short value) {
    return Arrays.binarySearch(data, value);
  }

  public static int binarySearch(ArrayData data, short value) {
    return Arrays.binarySearch(data.toShortArray(), value);
  }

  // foldable optimize
  public static int binarySearchNullSafe(Short[] data, Short value) {
    return Arrays.binarySearch(data, value, shortComp);
  }

  public static int binarySearchNullSafe(ArrayData data, Short value) {
    return Arrays.binarySearch(data.toObjectArray(ShortType$.MODULE$), value, shortComp);
  }

  // int
  // foldable optimize
  public static int binarySearch(int[] data, int value) {
    return Arrays.binarySearch(data, value);
  }

  public static int binarySearch(ArrayData data, int value) {
    return Arrays.binarySearch(data.toIntArray(), value);
  }

  // foldable optimize
  public static int binarySearchNullSafe(Integer[] data, Integer value) {
    return Arrays.binarySearch(data, value, integerComp);
  }

  public static int binarySearchNullSafe(ArrayData data, Integer value) {
    return Arrays.binarySearch(data.toObjectArray(IntegerType$.MODULE$), value, integerComp);
  }

  // long
  // foldable optimize
  public static int binarySearch(long[] data, long value) {
    return Arrays.binarySearch(data, value);
  }

  public static int binarySearch(ArrayData data, long value) {
    return Arrays.binarySearch(data.toLongArray(), value);
  }

  // foldable optimize
  public static int binarySearchNullSafe(Long[] data, Long value) {
    return Arrays.binarySearch(data, value, longComp);
  }

  public static int binarySearchNullSafe(ArrayData data, Long value) {
    return Arrays.binarySearch(data.toObjectArray(LongType$.MODULE$), value, longComp);
  }

  // float
  // foldable optimize
  public static int binarySearch(float[] data, float value) {
    return Arrays.binarySearch(data, value);
  }

  public static int binarySearch(ArrayData data, float value) {
    return Arrays.binarySearch(data.toFloatArray(), value);
  }

  // foldable optimize
  public static int binarySearchNullSafe(Float[] data, Float value) {
    return Arrays.binarySearch(data, value, floatComp);
  }

  public static int binarySearchNullSafe(ArrayData data, Float value) {
    return Arrays.binarySearch(data.toObjectArray(FloatType$.MODULE$), value, floatComp);
  }

  // double
  // foldable optimize
  public static int binarySearch(double[] data, double value) {
    return Arrays.binarySearch(data, value);
  }

  public static int binarySearch(ArrayData data, double value) {
    return Arrays.binarySearch(data.toDoubleArray(), value);
  }

  // foldable optimize
  public static int binarySearchNullSafe(Double[] data, Double value) {
    return Arrays.binarySearch(data, value, doubleComp);
  }

  public static int binarySearchNullSafe(ArrayData data, Double value) {
    return Arrays.binarySearch(data.toObjectArray(DoubleType$.MODULE$), value, doubleComp);
  }

  // Object
  public static int binarySearch(
    DataType elementType, Comparator<Object> comp, ArrayData data, Object value) {
    Object[] array = data.toObjectArray(elementType);
    return Arrays.binarySearch(array, value, comp);
  }
}
