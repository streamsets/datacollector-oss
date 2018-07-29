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

import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;

public class TestClassloaderInContextCredentialStore {

  public static class MyCredentialStore implements CredentialStore {
    private final ClassLoader expectedClassLoader;

    public MyCredentialStore(ClassLoader expectedClassLoader) {
      this.expectedClassLoader = expectedClassLoader;
    }

    @Override
    public List<ConfigIssue> init(Context context) {
      Assert.assertEquals(expectedClassLoader, Thread.currentThread().getContextClassLoader());
      return Collections.emptyList();
    }

    @Override
    public CredentialValue get(String group, String name, String credentialStoreOptions) throws StageException {
      Assert.assertEquals(expectedClassLoader, Thread.currentThread().getContextClassLoader());
     return () -> "credential";
    }

    @Override
    public void destroy() {
      Assert.assertEquals(expectedClassLoader, Thread.currentThread().getContextClassLoader());
    }
  }

  private ClassLoader currentClassLoader;

  @Before
  public void setup() {
    currentClassLoader = Thread.currentThread().getContextClassLoader();
  }

  @After
  public void cleanup() {
    Thread.currentThread().setContextClassLoader(currentClassLoader);
  }

  @Test
  public void testStore() throws StageException {
    ClassLoader cl = new URLClassLoader(new URL[0], currentClassLoader);

    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(cl);
    CredentialStoreDefinition storeDef = Mockito.mock(CredentialStoreDefinition.class);
    Mockito.when(storeDef.getStageLibraryDefinition()).thenReturn(libraryDef);

    CredentialStore store = new MyCredentialStore(cl);
    store = Mockito.spy(store);

    CredentialStore proxyStore = new ClassloaderInContextCredentialStore(storeDef, store);

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Assert.assertEquals(Collections.emptyList(), proxyStore.init(context));
    Mockito.verify(store, Mockito.times(1)).init(Mockito.eq(context));
    Assert.assertEquals(currentClassLoader, Thread.currentThread().getContextClassLoader());

    Assert.assertEquals("credential", proxyStore.get("group", "name", "options").get());
    Mockito.verify(store, Mockito.times(1)).get(Mockito.eq("group"), Mockito.eq("name"), Mockito.eq("options"));
    Assert.assertEquals(currentClassLoader, Thread.currentThread().getContextClassLoader());

    proxyStore.destroy();
    Mockito.verify(store, Mockito.times(1)).destroy();
    Assert.assertEquals(currentClassLoader, Thread.currentThread().getContextClassLoader());
  }

}
