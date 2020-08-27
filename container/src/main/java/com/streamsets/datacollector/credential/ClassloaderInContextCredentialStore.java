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
import com.streamsets.datacollector.util.LambdaUtil;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.credential.ManagedCredentialStore;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

/**
 * CredentialStore proxy that ensures the CredentialStore is always invoked in the context of its corresponding
 * classloader.
 */
public class ClassloaderInContextCredentialStore<T extends CredentialStore> implements ManagedCredentialStore {
  private final ClassLoader storeClassLoader;
  private final T store;

  public ClassloaderInContextCredentialStore(CredentialStoreDefinition definition, T store) {
    Utils.checkNotNull(definition, "definition");
    Utils.checkNotNull(store, "store");
    this.storeClassLoader = definition.getStageLibraryDefinition().getClassLoader();
    this.store = store;
  }

  @Override
  public List<ConfigIssue> init(Context context) {
    return LambdaUtil.withClassLoader(storeClassLoader,() -> store.init(context));
  }

  private static class ClassloaderCredentialValue implements CredentialValue {
    private final ClassLoader classLoader;
    private final LambdaUtil.ExceptionSupplier<String> credentialValue;
    public ClassloaderCredentialValue(ClassLoader classLoader, LambdaUtil.ExceptionSupplier<String> credentialValue) {
      this.classLoader = classLoader;
      this.credentialValue = credentialValue;
    }
    @Override
    public String get() throws StageException {
      return LambdaUtil.withClassLoader(classLoader, StageException.class, credentialValue);
    }
  }

  @Override
  public CredentialValue get(String group, String name, String credentialStoreOptions) throws StageException {
    return new ClassloaderCredentialValue(storeClassLoader, () -> store.get(group, name, credentialStoreOptions).get());
  }

  @Override
  public void store(List<String> groups, String name, String credentialValue) throws StageException {
    CredentialStoresTask.checkManagedState(store);
    LambdaUtil.withClassLoader(
        storeClassLoader,
        StageException.class,
        () -> {
          ((ManagedCredentialStore) store).store(groups, name, credentialValue);
          return null;
        }
    );
  }

  @Override
  public void delete(String name) throws StageException {
    CredentialStoresTask.checkManagedState(store);
    LambdaUtil.withClassLoader(
        storeClassLoader,
        StageException.class,
        () -> {
          ((ManagedCredentialStore) store).delete(name);
          return null;
        }
    );
  }

  @Override
  public List<String> getNames() throws StageException {
    CredentialStoresTask.checkManagedState(store);
    return LambdaUtil.withClassLoader(
        storeClassLoader,
        StageException.class,
        ((ManagedCredentialStore) store)::getNames
    );
  }

  @Override
  public void destroy() {
    LambdaUtil.withClassLoader(storeClassLoader, () -> {store.destroy();return null;});
  }

}
