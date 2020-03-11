/*
 * Copyright 2017 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.credential;

import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.security.GroupsInScope;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.credential.ManagedCredentialStore;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestGroupEnforcerCredentialStore {

  @Test(expected = RuntimeException.class)
  public void testNoContext() throws StageException {
    CredentialStore store = Mockito.mock(CredentialStore.class);
    store = new GroupEnforcerCredentialStore<>(store);
    store.get("g", "n", "o");
  }

  private static final CredentialValue CREDENTIAL_VALUE = new CredentialValue() {
    @Override
    public String get() throws StageException {
      return "c";
    }
  };


  @Test
  public void testNotEnforced() throws Exception {
    CredentialStore store = Mockito.mock(CredentialStore.class);
    Mockito.when(store.get(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq("o"))).thenReturn(CREDENTIAL_VALUE);
    GroupEnforcerCredentialStore enforcerStore = new GroupEnforcerCredentialStore<>(store);

    CredentialValue value = GroupsInScope.executeIgnoreGroups(() -> enforcerStore.get("g", "n", "o"));
    Assert.assertEquals(CREDENTIAL_VALUE, value);
  }

  @Test
  public void testEnforcedOk() throws Exception {
    CredentialStore store = Mockito.mock(CredentialStore.class);
    Mockito.when(store.get(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq("o"))).thenReturn(CREDENTIAL_VALUE);
    GroupEnforcerCredentialStore enforcerStore = new GroupEnforcerCredentialStore<>(store);

    CredentialValue value = GroupsInScope.execute(ImmutableSet.of("g"), () -> enforcerStore.get("g", "n", "o"));
    Assert.assertEquals(CREDENTIAL_VALUE, value);
  }

  @Test(expected = StageException.class)
  public void testEnforcedFail() throws Throwable {
    CredentialStore store = Mockito.mock(CredentialStore.class);
    Mockito.when(store.get(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq("o"))).thenReturn(CREDENTIAL_VALUE);
    GroupEnforcerCredentialStore enforcerStore = new GroupEnforcerCredentialStore<>(store);

    GroupsInScope.execute(ImmutableSet.of("h"), () -> enforcerStore.get("g", "n", "o"));
  }

  @Test
  public void testEnforcedPassForAllGroup() throws Throwable {
    CredentialStore store = Mockito.mock(CredentialStore.class);
    Mockito.when(store.get(Mockito.eq("all"), Mockito.eq("n"), Mockito.eq("o"))).thenReturn(CREDENTIAL_VALUE);
    GroupEnforcerCredentialStore enforcerStore = new GroupEnforcerCredentialStore<>(store);
    GroupsInScope.execute(ImmutableSet.of("h"), () -> enforcerStore.get("all", "n", "o"));
  }

  @Test
  public void testNotManagedStore() {
    CredentialStore store = Mockito.mock(CredentialStore.class);
    Mockito.when(store.get(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq("o"))).thenReturn(CREDENTIAL_VALUE);
    GroupEnforcerCredentialStore enforcerStore = new GroupEnforcerCredentialStore<>(store);
    try {
      enforcerStore.store(CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST, "n", "v");
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalStateException);
    }

    try {
      enforcerStore.delete("n");
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalStateException);
    }

    try {
      enforcerStore.getNames();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalStateException);
    }
  }
}
