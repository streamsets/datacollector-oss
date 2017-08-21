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
package com.streamsets.lib.security.http;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestPrincipalCache {

  @Test
  public void testCache() throws Exception {
    PrincipalCache cache = new PrincipalCache(50, 100);
    SSOPrincipal principal = Mockito.mock(SSOPrincipal.class);

    // not present

    Assert.assertNull(cache.get("token"));
    Assert.assertFalse(cache.isInvalid("token"));

    // caching

    Assert.assertTrue(cache.put("token", principal));
    Assert.assertFalse(cache.put("token", principal));

    Assert.assertEquals(principal, cache.get("token"));

    // expiring principal cache, no invalidation

    Thread.sleep(51);
    Assert.assertNull(cache.get("token"));
    Assert.assertFalse(cache.isInvalid("token"));

    // invalidating known

    Assert.assertTrue(cache.put("token", principal));
    Assert.assertTrue(cache.invalidate("token"));
    Assert.assertNull(cache.get("token"));
    Assert.assertTrue(cache.isInvalid("token"));

    // expiring invalid token cache

    Thread.sleep(101);
    Assert.assertFalse(cache.isInvalid("token"));

    // invalidating unknown

    Assert.assertTrue(cache.invalidate("token"));
    Assert.assertTrue(cache.isInvalid("token"));

    // cache clear
    cache.clear();

    Assert.assertTrue(cache.put("token", principal));
    cache.clear();
    Assert.assertNull(cache.get("token"));

    Assert.assertTrue(cache.invalidate("token"));
    Assert.assertTrue(cache.isInvalid("token"));
    cache.clear();
    Assert.assertNull(cache.get("token"));
    Assert.assertFalse(cache.isInvalid("token"));

  }

}
