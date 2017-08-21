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
package com.streamsets.pipeline.stage.origin.elasticsearch;

import org.junit.Test;

import static org.junit.Assert.*;

public class ElasticsearchSourceOffsetTest {
  private static ElasticsearchSourceOffset nullOffset = new ElasticsearchSourceOffset(null, null);
  private static ElasticsearchSourceOffset nullOffset2 = new ElasticsearchSourceOffset(null, null);
  private static ElasticsearchSourceOffset nonNullOffset = new ElasticsearchSourceOffset("abcdef", "12345");
  private static ElasticsearchSourceOffset nonNullOffset2 = new ElasticsearchSourceOffset("abcdef", "12346");
  private static ElasticsearchSourceOffset nonNullOffset3 = new ElasticsearchSourceOffset("abcdef", "12345");
  private static ElasticsearchSourceOffset partialNullOffset = new ElasticsearchSourceOffset(null, "12345");
  private static ElasticsearchSourceOffset partialNullOffset2 = new ElasticsearchSourceOffset("12345", null);

  @Test
  public void testCompareTo() throws Exception {

    assertEquals(0, nullOffset.compareTo(nullOffset2));

    // CompareTo should treat offsets with same timeOffset as equal, regardless of scrollId;
    assertEquals(0, nonNullOffset.compareTo(partialNullOffset));
    assertTrue(nonNullOffset.compareTo(nonNullOffset2) < 0);
    assertTrue(partialNullOffset.compareTo(partialNullOffset2) > 0);
    assertTrue(partialNullOffset2.compareTo(partialNullOffset) < 0);

  }

  @Test
  public void testEquals() throws Exception {
    assertEquals(nullOffset, nullOffset2);
    assertNotEquals(nullOffset, nonNullOffset);
    assertNotEquals(nonNullOffset, nonNullOffset2);
    assertNotEquals(nonNullOffset, partialNullOffset);
    assertNotEquals(nonNullOffset, "abcdef");
  }

  @Test
  public void testHashCode() throws Exception {
    assertEquals(nullOffset.hashCode(), nullOffset2.hashCode());
    assertEquals(nonNullOffset.hashCode(), nonNullOffset3.hashCode());
  }

}
