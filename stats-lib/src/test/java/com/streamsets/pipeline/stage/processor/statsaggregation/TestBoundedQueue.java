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
package com.streamsets.pipeline.stage.processor.statsaggregation;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestBoundedQueue {


  @Test
  public void testBoundedQueueDuplicate() {
    BoundedDeque<String> boundedDeque = new BoundedDeque<>(5);
    Assert.assertTrue(boundedDeque.offerLast("a"));
    Assert.assertFalse(boundedDeque.offerLast("a"));
    Assert.assertTrue(boundedDeque.offerLast("b"));
    Assert.assertFalse(boundedDeque.offerLast("b"));
    Assert.assertTrue(boundedDeque.offerLast("a"));
    Assert.assertTrue(boundedDeque.offerLast("b"));
    Assert.assertTrue(boundedDeque.offerLast("a"));

    List<String> result = new ArrayList<>(5);
    for (String s : boundedDeque) {
      result.add(s);
    }

    Assert.assertEquals(5, result.size());

    Assert.assertEquals("a", result.get(0));
    Assert.assertEquals("b", result.get(1));
    Assert.assertEquals("a", result.get(2));
    Assert.assertEquals("b", result.get(3));
    Assert.assertEquals("a", result.get(4));
  }

  @Test
  public void testBoundedQueueDiscard() {
    BoundedDeque<String> boundedDeque = new BoundedDeque<>(5);
    Assert.assertTrue(boundedDeque.offerLast("a"));
    Assert.assertTrue(boundedDeque.offerLast("b"));
    Assert.assertTrue(boundedDeque.offerLast("c"));
    Assert.assertTrue(boundedDeque.offerLast("d"));
    Assert.assertTrue(boundedDeque.offerLast("e"));
    Assert.assertTrue(boundedDeque.offerLast("f"));
    Assert.assertTrue(boundedDeque.offerLast("g"));


    List<String> result = new ArrayList<>(5);
    for (String s : boundedDeque) {
      result.add(s);
    }

    Assert.assertEquals(5, result.size());

    Assert.assertEquals("c", result.get(0));
    Assert.assertEquals("d", result.get(1));
    Assert.assertEquals("e", result.get(2));
    Assert.assertEquals("f", result.get(3));
    Assert.assertEquals("g", result.get(4));
  }


  @Test
  public void testBoundedQueue() {
    BoundedDeque<String> boundedDeque = new BoundedDeque<>(5);
    Assert.assertTrue(boundedDeque.offerLast("a"));
    Assert.assertFalse(boundedDeque.offerLast("a"));
    Assert.assertTrue(boundedDeque.offerLast("b"));
    Assert.assertFalse(boundedDeque.offerLast("b"));
    Assert.assertTrue(boundedDeque.offerLast("c"));
    Assert.assertTrue(boundedDeque.offerLast("d"));
    Assert.assertFalse(boundedDeque.offerLast("d"));
    Assert.assertFalse(boundedDeque.offerLast("d"));
    Assert.assertFalse(boundedDeque.offerLast("d"));
    Assert.assertFalse(boundedDeque.offerLast("d"));
    Assert.assertTrue(boundedDeque.offerLast("e"));
    Assert.assertFalse(boundedDeque.offerLast("e"));
    Assert.assertFalse(boundedDeque.offerLast("e"));
    Assert.assertFalse(boundedDeque.offerLast("e"));

    List<String> result = new ArrayList<>(5);

    for (String s : boundedDeque) {
      result.add(s);
    }

    Assert.assertEquals("a", result.get(0));
    Assert.assertEquals("b", result.get(1));
    Assert.assertEquals("c", result.get(2));
    Assert.assertEquals("d", result.get(3));
    Assert.assertEquals("e", result.get(4));

    Assert.assertTrue(boundedDeque.offerLast("f"));
    Assert.assertTrue(boundedDeque.offerLast("g"));

    result.clear();
    for (String s : boundedDeque) {
      result.add(s);
    }

    Assert.assertEquals("c", result.get(0));
    Assert.assertEquals("d", result.get(1));
    Assert.assertEquals("e", result.get(2));
    Assert.assertEquals("f", result.get(3));
    Assert.assertEquals("g", result.get(4));

  }
}
