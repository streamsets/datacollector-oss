/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.kudulookup;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.stage.processor.kv.CacheConfig;
import com.streamsets.pipeline.stage.processor.kv.EvictionPolicyType;
import com.streamsets.pipeline.stage.processor.kv.LookupUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestKuduLookupProcessorLocalCaching {
  @Test
  public void testTwoInstancesOfOneKeyCanBeUsedToLookUpACachedElement() throws Exception {
    // given
    CacheConfig cacheConf = new CacheConfig();
    cacheConf.enabled = true;
    cacheConf.maxSize = -1;
    cacheConf.evictionPolicyType = EvictionPolicyType.EXPIRE_AFTER_WRITE;
    cacheConf.expirationTime = 24;
    cacheConf.timeUnit = TimeUnit.HOURS;

    Map<String, Field> keyList = new HashMap<>();
    keyList.put("c1", Field.create(1));

    KuduLookupKey lk1 = new KuduLookupKey("t1", keyList);
    KuduLookupKey lk2 = new KuduLookupKey("t1", keyList);

    CacheLoader<KuduLookupKey, List<Map<String, Field>>> store = mock(CacheLoader.class);

    Map<String, Field> value = new HashMap<>();
    value.put("c2", Field.create("value"));

    List<Map<String, Field>> values1 = Collections.singletonList(value);
    List<Map<String, Field>> values2 = Collections.singletonList(value);

    when(store.load(any())).thenReturn(values1).thenReturn(values2);

    LoadingCache<KuduLookupKey, List<Map<String, Field>>> cache = LookupUtils.buildCache(store, cacheConf);

    // when
    List<Map<String, Field>> r1 = cache.get(lk1);
    List<Map<String, Field>> r2 = cache.get(lk2);

    // then
    Assert.assertSame(r1, values1);
    Assert.assertSame(r2, values1);

    verify(store).load(any());
  }
}
