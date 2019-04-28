/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionTest {

  @Test
  public void versionParsing() {
    Version version;

    version = new Version("1");
    assertEquals(1, version.getMajor());
    assertEquals(0, version.getMinor());

    version = new Version("1.2-SNAPSHOT");
    assertEquals(1, version.getMajor());
    assertEquals(2, version.getMinor());
    assertEquals(0, version.getBugfix());

    version = new Version("1.2.3-RC1");
    assertEquals(1, version.getMajor());
    assertEquals(2, version.getMinor());
    assertEquals(3, version.getBugfix());
  }

  @Test
  public void isTheSame() {
    assertTrue(new Version("1.1").isEqual("1.1.0"));
    assertTrue(new Version("1.1").isEqual("1.1.0-SNAPSHOT"));
    assertTrue(new Version("1.1.0").isEqual("1.1"));
    assertTrue(new Version("1.1.0").isEqual("1.1-SNAPSHOT"));
    assertTrue(new Version("1.1.0").isEqual("1.1.0.0.0"));
    assertTrue(new Version("1.1.0-RC").isEqual("1.1.0.0.0-SNAPSHOT"));

    assertFalse(new Version("1").isEqual("2"));
    assertFalse(new Version("1.1.0-RC").isEqual("1.1.0.0.2-SNAPSHOT"));
  }

  @Test
  public void isGreaterOrEqualTo() {
    // Equal
    assertTrue(new Version("1.1").isGreaterOrEqualTo("1.1.0"));
    assertTrue(new Version("1.1").isGreaterOrEqualTo("1.1"));
    assertTrue(new Version("1.1.0").isGreaterOrEqualTo("1.1"));

    // Greater
    assertTrue(new Version("1.1").isGreaterOrEqualTo("1.0"));
    assertTrue(new Version("1.1").isGreaterOrEqualTo("1.0.0.0.0.0.0.1"));
    assertTrue(new Version("1.1").isGreaterOrEqualTo("0.9"));

    // Negative cases
    assertFalse(new Version("1.1").isGreaterOrEqualTo("1.1.0.0.0.0.1"));
    assertFalse(new Version("1.1").isGreaterOrEqualTo("2"));
  }

  @Test
  public void isGreaterThen() {
    // Equal
    assertFalse(new Version("1.1").isGreaterThan("1.1.0"));
    assertFalse(new Version("1.1").isGreaterThan("1.1"));
    assertFalse(new Version("1.1.0").isGreaterThan("1.1"));

    // Greater
    assertTrue(new Version("1.1").isGreaterThan("1.0"));
    assertTrue(new Version("1.1").isGreaterThan("1.0.0.0.0.0.0.1"));
    assertTrue(new Version("1.1").isGreaterThan("0.9"));

    // Negative cases
    assertFalse(new Version("1.1").isGreaterThan("1.1.0.0.0.0.1"));
    assertFalse(new Version("1.1").isGreaterThan("2"));
  }

  @Test
  public void isLessOrEqualToA() {
    // Equal
    assertTrue(new Version("1.1.0").isLessOrEqualTo("1.1"));
    assertTrue(new Version("1.1").isLessOrEqualTo("1.1"));
    assertTrue(new Version("1.1").isLessOrEqualTo("1.1.0"));

    // Lesser
    assertTrue(new Version("1.1").isLessOrEqualTo("1.2"));
    assertTrue(new Version("1.1").isLessOrEqualTo("1.1.0.0.0.1"));
    assertTrue(new Version("1.1").isLessOrEqualTo("2"));

    // Negative cases
    assertFalse(new Version("1.1").isLessOrEqualTo("1.0"));
    assertFalse(new Version("1.1").isLessOrEqualTo("1.0.0.0.0.0.0.1"));
    assertFalse(new Version("1.1").isLessOrEqualTo("0.9"));
  }

  @Test
  public void isLessThen() {
    // Equal
    assertFalse(new Version("1.1.0").isLessThan("1.1"));
    assertFalse(new Version("1.1").isLessThan("1.1"));
    assertFalse(new Version("1.1").isLessThan("1.1.0"));

    // Lesser
    assertTrue(new Version("1.1").isLessThan("1.2"));
    assertTrue(new Version("1.1").isLessThan("1.1.0.0.0.1"));
    assertTrue(new Version("1.1").isLessThan("2"));

    // Negative cases
    assertFalse(new Version("1.1").isLessThan("1.0"));
    assertFalse(new Version("1.1").isLessThan("1.0.0.0.0.0.0.1"));
    assertFalse(new Version("1.1").isLessThan("0.9"));
  }
}
