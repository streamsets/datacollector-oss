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
package com.streamsets.datacollector.credential.vault;

import com.streamsets.datacollector.vault.Vault;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestVaultCredentialStore {

  @Test
  public void testStore() throws StageException {
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    VaultCredentialStore store = new VaultCredentialStore();
    store = Mockito.spy(store);
    Vault vault = Mockito.mock(Vault.class);
    Mockito.doReturn(vault).when(store).createVault(Mockito.eq(context));

    Assert.assertTrue(store.init(context).isEmpty());
    Mockito.verify(vault, Mockito.times(1)).init();

    Mockito.when(vault.read(Mockito.eq("path"), Mockito.eq("key"), Mockito.eq(1L))).thenReturn("secret");

    CredentialValue value = store.get("g", "path&key", "delay=1");
    Assert.assertNotNull(value);
    Mockito.verify(vault, Mockito.times(1)).read(Mockito.eq("path"), Mockito.eq("key"), Mockito.eq(1L));
    Assert.assertEquals("secret", value.get());
    Mockito.verify(vault, Mockito.times(2)).read(Mockito.eq("path"), Mockito.eq("key"), Mockito.eq(1L));

    Mockito.when(vault.read(Mockito.eq("path"), Mockito.eq("key"), Mockito.eq(0L))).thenReturn("secret");
    value = store.get("g", "path&key", "");
    Mockito.verify(vault, Mockito.times(1)).read(Mockito.eq("path"), Mockito.eq("key"), Mockito.eq(0L));
    Assert.assertEquals("secret", value.get());

    value = store.get("g", "path@key", "separator=@");
    Mockito.verify(vault, Mockito.times(3)).read(Mockito.eq("path"), Mockito.eq("key"), Mockito.eq(0L));
    Assert.assertEquals("secret", value.get());

    store.destroy();
    Mockito.verify(vault, Mockito.times(1)).destroy();
  }

}
