/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.classpath;

import org.junit.Test;

import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClasspathValidatorTest {

  @Test
  public void testSimpleValid() throws Exception {
    ClasspathValidatorResult result = ClasspathValidator.newValidator("test")
      .withURL(new URL("file:///test-0.1.jar"))
      .validate();

    assertTrue(result.isValid());
    assertEquals(0, result.getUnparseablePaths().size());
    assertEquals(0, result.getVersionCollisons().size());
  }

  @Test
  public void testUnparseableLibrary() throws Exception {
    ClasspathValidatorResult result = ClasspathValidator.newValidator("test")
      .withURL(new URL("file:///obviously-not-a-jar-name.txt"))
      .validate();

    assertFalse(result.isValid());
    assertEquals(1, result.getUnparseablePaths().size());
    assertEquals(0, result.getVersionCollisons().size());

    assertTrue(result.getUnparseablePaths().contains("file:/obviously-not-a-jar-name.txt"));
  }

  @Test
  public void testVersionCollision() throws Exception {
    ClasspathValidatorResult result = ClasspathValidator.newValidator("test")
      .withURL(new URL("file:///test-0.1.jar"))
      .withURL(new URL("file:///test-0.2.jar"))
      .validate();

    assertFalse(result.isValid());
    assertEquals(0, result.getUnparseablePaths().size());
    assertEquals(1, result.getVersionCollisons().size());

    assertTrue(result.getVersionCollisons().containsKey("test"));
    Map<String, List<Dependency>> collisions = result.getVersionCollisons().get("test");
    assertEquals(2, collisions.size());
    assertTrue(collisions.containsKey("0.1"));
    assertTrue(collisions.containsKey("0.2"));
  }
}
