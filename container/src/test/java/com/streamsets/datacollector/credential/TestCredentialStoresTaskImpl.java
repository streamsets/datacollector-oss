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
import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.definition.CredentialStoreDefinitionExtractor;
import com.streamsets.datacollector.security.GroupsInScope;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialStoreDef;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestCredentialStoresTaskImpl {

  @CredentialStoreDef(label = "label", description = "desc") public static class MyCredentialStore
      implements CredentialStore {
    @Override
    public List<ConfigIssue> init(Context context) {
      List<ConfigIssue> issues = new ArrayList<>();
      if (!context.getId().equals("id")) {
        issues.add(context.createConfigIssue(Errors.CREDENTIAL_STORE_000, "no ID"));
      }
      if (!context.getConfig("foo").equals("bar")) {
        issues.add(context.createConfigIssue(Errors.CREDENTIAL_STORE_000, "no config"));
      }
      return issues;
    }

    @Override
    public CredentialValue get(String group, String name, String credentialStoreOptions) throws StageException {
      return () -> group + ":" + name + ":" + credentialStoreOptions;
    }

    @Override
    public void destroy() {

    }
  }

  @Test
  public void testLifecycle() {
    Configuration conf = new Configuration();
    StageLibraryTask libraryTask = Mockito.mock(StageLibraryTask.class);
    CredentialStoresTaskImpl storeTask = new CredentialStoresTaskImpl(conf, libraryTask);
    Map<String, CredentialStore> stores = storeTask.getStores();
    Assert.assertNotNull(stores);
    Assert.assertTrue(stores.isEmpty());

    storeTask = Mockito.spy(storeTask);

    Mockito.doReturn(Collections.emptyList()).when(storeTask).loadAndInitStores();

    // init
    storeTask.initTask();
    Mockito.verify(storeTask, Mockito.times(1)).loadAndInitStores();
    Assert.assertEquals(stores, CredentialEL.getCredentialStores());

    // stop
    CredentialStore store = Mockito.mock(CredentialStore.class);
    stores.put("id", store);
    storeTask.stopTask();
    Mockito.verify(store, Mockito.times(1)).destroy();
  }

  @Test
  public void testLoadAndInitStoreGetDestroy() throws Exception {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getName()).thenReturn("lib");
    CredentialStoreDefinition storeDef =
        CredentialStoreDefinitionExtractor.get().extract(libraryDef, MyCredentialStore.class);


    Configuration conf = new Configuration();
    conf.set("credentialStores", "id");
    conf.set("credentialStore.id.def", libraryDef.getName() + "::" + storeDef.getName());
    conf.set("credentialStore.id.config.foo", "bar");
    StageLibraryTask libraryTask = Mockito.mock(StageLibraryTask.class);
    Mockito.when(libraryTask.getCredentialStoreDefinitions()).thenReturn(ImmutableList.of(storeDef));
    CredentialStoresTaskImpl storeTask = new CredentialStoresTaskImpl(conf, libraryTask);

    storeTask.initTask();

    CredentialStore store = storeTask.getStores().get("id");
    Assert.assertTrue(store instanceof ClassloaderInContextCredentialStore);

    GroupsInScope.execute(ImmutableSet.of("g"), () -> store.get("g", "n", "o"));

    // enforcing Fail
    try {
      GroupsInScope.execute(ImmutableSet.of("g"), () -> store.get("h", "n", "o"));
      Assert.fail();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof StageException);
    }

    // not enforcing
    GroupsInScope.executeIgnoreGroups(() -> store.get("g", "n", "o"));

    storeTask.stopTask();
  }

  @Test
  public void testCreateContext() {
    Configuration conf = new Configuration();
    conf.set("credentialStore.id.config.foo", "bar");
    StageLibraryTask libraryTask = Mockito.mock(StageLibraryTask.class);
    CredentialStoresTaskImpl storeTask = new CredentialStoresTaskImpl(conf, libraryTask);

    CredentialStore.Context context = storeTask.createContext("id", conf);
    Assert.assertEquals("id", context.getId());
    Assert.assertEquals("bar", context.getConfig("foo"));

    CredentialStore.ConfigIssue issue = context.createConfigIssue(Errors.CREDENTIAL_STORE_000, "MESSAGE");
    Assert.assertTrue(issue.toString().contains("CREDENTIAL_STORE_000"));
    Assert.assertTrue(issue.toString().contains("MESSAGE"));
  }

  @Test
  public void testVaultELCredentialStoreRegistration() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getName()).thenReturn("lib");
    CredentialStoreDefinition storeDef =
        CredentialStoreDefinitionExtractor.get().extract(libraryDef, MyCredentialStore.class);


    Configuration conf = new Configuration();
    conf.set("credentialStores", "id");
    conf.set("credentialStore.id.def", libraryDef.getName() + "::" + storeDef.getName());
    conf.set("credentialStore.id.config.foo", "bar");
    StageLibraryTask libraryTask = Mockito.mock(StageLibraryTask.class);
    Mockito.when(libraryTask.getCredentialStoreDefinitions()).thenReturn(ImmutableList.of(storeDef));

    // testing no Vault EL impl registered
    CredentialStoresTaskImpl storeTask = new CredentialStoresTaskImpl(conf, libraryTask);
    storeTask.initTask();
    Assert.assertNull(DataCollectorServices.instance().get(CredentialStoresTaskImpl.VAULT_CREDENTIAL_STORE_KEY));
    storeTask.stopTask();

    // testing Vault EL impl registered
    conf.set("vaultEL.credentialStore.id", "id");
    storeTask = new CredentialStoresTaskImpl(conf, libraryTask);
    storeTask.initTask();
    CredentialStore store = DataCollectorServices.instance().get(CredentialStoresTaskImpl.VAULT_CREDENTIAL_STORE_KEY);
    Assert.assertNotNull(store);
    storeTask.stopTask();
  }

}
