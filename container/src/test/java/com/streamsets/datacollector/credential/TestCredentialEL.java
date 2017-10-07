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
package com.streamsets.datacollector.credential;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestCredentialEL {

  @After
  public void setup() {
    CredentialEL.setCredentialStores(null);
  }

  @Test(expected = RuntimeException.class)
  public void testCredentialELUndefinedStoreId() throws Exception {
    CredentialEL.setCredentialStores(ImmutableMap.of());

    CredentialEL.get("id", "group", "name");
  }

  @Test
  public void testCredentialELGet() throws Exception {
    CredentialStore store = Mockito.mock(CredentialStore.class);
    Mockito.when(store.get(Mockito.eq("group"), Mockito.eq("name"), Mockito.eq(""))).thenReturn(() -> "foo");
    CredentialEL.setCredentialStores(ImmutableMap.of("id", store));

    Assert.assertEquals("foo", CredentialEL.get("id", "group", "name").get());
    Assert.assertNull(CredentialEL.get("id", "group", "nothere"));
  }

  @Test
  public void testCredentialELGetWithOptions() throws Exception {
    CredentialStore store = Mockito.mock(CredentialStore.class);
    Mockito.when(store.get(Mockito.eq("group"), Mockito.eq("name"), Mockito.eq("options"))).thenReturn(() -> "foo");
    CredentialEL.setCredentialStores(ImmutableMap.of("id", store));

    Assert.assertEquals("foo", CredentialEL.getWithOptions("id", "group", "name", "options").get());
    Assert.assertNull(CredentialEL.getWithOptions("id", "group", "nothere", "options"));
  }

  @Test(expected = StageException.class)
  public void testCredentialELGetStageException() throws Exception {
    CredentialStore store = Mockito.mock(CredentialStore.class);
    Mockito.when(store.get(Mockito.eq("group"), Mockito.eq("name"), Mockito.eq("")))
           .thenThrow(Mockito.mock(StageException.class));
    CredentialEL.setCredentialStores(ImmutableMap.of("id", store));

    CredentialEL.get("id", "group", "name");
  }

}
