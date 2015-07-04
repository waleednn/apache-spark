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

package org.apache.spark.unsafe.types;

import java.io.UnsupportedEncodingException;

import org.junit.Test;

import static junit.framework.Assert.*;

import static org.apache.spark.unsafe.types.UTF8String.*;

public class UTF8StringSuite {

  private void checkBasic(String str, int len) throws UnsupportedEncodingException {
    UTF8String s1 = fromString(str);
    UTF8String s2 = fromBytes(str.getBytes("utf8"));
    assertEquals(s1.numChars(), len);
    assertEquals(s2.numChars(), len);

    assertEquals(s1.toString(), str);
    assertEquals(s2.toString(), str);
    assertEquals(s1, s2);

    assertEquals(s1.hashCode(), s2.hashCode());

    assertEquals(s1.compare(s2), 0);
    assertEquals(s1.compareTo(s2), 0);

    assertEquals(s1.contains(s2), true);
    assertEquals(s2.contains(s1), true);
    assertEquals(s1.startsWith(s1), true);
    assertEquals(s1.endsWith(s1), true);
  }

  @Test
  public void basicTest() throws UnsupportedEncodingException {
    checkBasic("hello", 5);
    checkBasic("大 千 世 界", 7);
  }

  @Test
  public void compare() {
    assertTrue(fromString("abc").compare(fromString("ABC")) > 0);
    assertTrue(fromString("abc0").compare(fromString("abc")) > 0);
    assertTrue(fromString("abcabcabc").compare(fromString("abcabcabc")) == 0);
    assertTrue(fromString("aBcabcabc").compare(fromString("Abcabcabc")) > 0);
    assertTrue(fromString("Abcabcabc").compare(fromString("abcabcabC")) < 0);
    assertTrue(fromString("abcabcabc").compare(fromString("abcabcabC")) > 0);

    assertTrue(fromString("abc").compare(fromString("世界")) < 0);
    assertTrue(fromString("你好").compare(fromString("世界")) > 0);
    assertTrue(fromString("你好123").compare(fromString("你好122")) > 0);
  }

  @Test
  public void contains() {
    assertTrue(fromString("hello").contains(fromString("ello")));
    assertFalse(fromString("hello").contains(fromString("vello")));
    assertFalse(fromString("hello").contains(fromString("hellooo")));
    assertTrue(fromString("大千世界").contains(fromString("千世界")));
    assertFalse(fromString("大千世界").contains(fromString("世千")));
    assertFalse(fromString("大千世界").contains(fromString("大千世界好")));
  }

  @Test
  public void startsWith() {
    assertTrue(fromString("hello").startsWith(fromString("hell")));
    assertFalse(fromString("hello").startsWith(fromString("ell")));
    assertFalse(fromString("hello").startsWith(fromString("hellooo")));
    assertTrue(fromString("数据砖头").startsWith(fromString("数据")));
    assertFalse(fromString("大千世界").startsWith(fromString("千")));
    assertFalse(fromString("大千世界").startsWith(fromString("大千世界好")));
  }

  @Test
  public void endsWith() {
    assertTrue(fromString("hello").endsWith(fromString("ello")));
    assertFalse(fromString("hello").endsWith(fromString("ellov")));
    assertFalse(fromString("hello").endsWith(fromString("hhhello")));
    assertTrue(fromString("大千世界").endsWith(fromString("世界")));
    assertFalse(fromString("大千世界").endsWith(fromString("世")));
    assertFalse(fromString("数据砖头").endsWith(fromString("我的数据砖头")));
  }

  @Test
  public void substring() {
    assertEquals(fromString("hello").substring(0, 0), fromString(""));
    assertEquals(fromString("hello").substring(1, 3), fromString("el"));
    assertEquals(fromString("数据砖头").substring(0, 1), fromString("数"));
    assertEquals(fromString("数据砖头").substring(1, 3), fromString("据砖"));
    assertEquals(fromString("数据砖头").substring(3, 5), fromString("头"));
  }
}
