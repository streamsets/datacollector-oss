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
package com.streamsets.pipeline.stage.processor.kv.local;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestLocalStore {

  @Test
  public void testGetSingleKey() throws Exception {
    final String expected = "value1";
    LocalLookupConfig conf = new LocalLookupConfig();
    conf.values = ImmutableMap.of("key1", "value1", "key2", "value2", "key3", "value3");
    LocalStore localStore = new LocalStore(conf);
    Optional<String> value = localStore.get("key1");
    localStore.close();
    assertTrue(value.isPresent());
    assertEquals(expected, value.get());
  }

  @Test
  public void testGetMultipleKeys() throws Exception {
    List<String> keys = ImmutableList.of("key1", "key2", "key3", "key4");
    Map<String, Optional<?>> expected = ImmutableMap.of(
      "key1", Optional.of("value1"),
      "key2", Optional.of("value2"),
      "key3", Optional.of("value3"),
      "key4", Optional.absent()
    );
    LocalLookupConfig conf = new LocalLookupConfig();
    conf.values = ImmutableMap.of("key1", "value1", "key2", "value2", "key3", "value3");
    LocalStore localStore = new LocalStore(conf);
    Map<String, Optional<String>> values = localStore.get(keys);
    localStore.close();
    assertEquals(keys.size(), values.size());
    assertEquals(expected.toString(), values.toString());
  }

  @Test
  public void testPutSingleKey() throws Exception{
    final String expected = "putSingleValue";
    LocalLookupConfig conf = new LocalLookupConfig();
    LocalStore localStore = new LocalStore(conf);
    localStore.put("putSingleKey", expected);
    Optional<String> value = localStore.get("putSingleKey");
    localStore.close();
    assertTrue(value.isPresent());
    assertEquals(expected, value.get());
  }

  @Test
  public void testPutMultipleKeys() throws Exception {
    List<String> keys = ImmutableList.of("putMultipleKey1", "putMultipleKey2", "putMultipleKey3", "putMultipleKey4");
    Map<String, String> toAdd = ImmutableMap.of(
        "putMultipleKey1", "value1",
        "putMultipleKey2", "value2",
        "putMultipleKey3", "value3"
    );
    Map<String, Optional<?>> expected = ImmutableMap.of(
      "putMultipleKey1", Optional.of("value1"),
      "putMultipleKey2", Optional.of("value2"),
      "putMultipleKey3", Optional.of("value3"),
      "putMultipleKey4", Optional.absent()
    );
    LocalLookupConfig conf = new LocalLookupConfig();
    LocalStore localStore = new LocalStore(conf);
    localStore.putAll(toAdd);
    Map<String, Optional<String>> values = localStore.get(keys);
    localStore.close();
    assertEquals(keys.size(), values.size());
    assertEquals(expected.toString(), values.toString());
  }
}
