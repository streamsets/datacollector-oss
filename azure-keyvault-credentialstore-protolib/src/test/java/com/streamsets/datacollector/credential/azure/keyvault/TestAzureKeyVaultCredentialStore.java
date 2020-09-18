/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.datacollector.credential.azure.keyvault;

import com.microsoft.azure.keyvault.KeyVaultClient;
import com.microsoft.azure.keyvault.models.SecretBundle;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest({KeyVaultClient.class})
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestAzureKeyVaultCredentialStore {

  @Test
  public void testInit_emptyConfigs() {
    AzureKeyVaultCredentialStore store = new AzureKeyVaultCredentialStore();
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    store = Mockito.spy(store);

    KeyVaultClient keyVaultClient = PowerMockito.mock(KeyVaultClient.class);
    Mockito.doReturn(keyVaultClient).when(store).createClient();

    Mockito.when(context.getConfig(Mockito.any())).thenReturn("");

    Assert.assertEquals(3, store.init(context).size());
  }

  @Test
  public void testInit_nullConfigs() {
    AzureKeyVaultCredentialStore store = new AzureKeyVaultCredentialStore();
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    store = Mockito.spy(store);

    KeyVaultClient keyVaultClient = PowerMockito.mock(KeyVaultClient.class);
    Mockito.doReturn(keyVaultClient).when(store).createClient();

    Mockito.when(context.getConfig(Mockito.any())).thenReturn(null);

    Assert.assertEquals(3, store.init(context).size());
  }

  @Test
  public void testInit_noIssues() {
    AzureKeyVaultCredentialStore store = new AzureKeyVaultCredentialStore();
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    store = Mockito.spy(store);

    KeyVaultClient keyVaultClient = PowerMockito.mock(KeyVaultClient.class);
    Mockito.doReturn(keyVaultClient).when(store).createClient();
    Mockito.when(keyVaultClient.getSecret(Mockito.any(), Mockito.any())).thenReturn(new SecretBundle());

    Mockito.when(context.getConfig(Mockito.any())).thenReturn("test");

    Configuration configuration = Mockito.mock(Configuration.class);
    Mockito.doReturn(configuration).when(store).getConfiguration();
    Mockito.when(configuration.get(AzureKeyVaultCredentialStore.CREDENTIAL_REFRESH_PROP,
        AzureKeyVaultCredentialStore.CREDENTIAL_REFRESH_DEFAULT
    ))
           .thenReturn(AzureKeyVaultCredentialStore.CREDENTIAL_REFRESH_DEFAULT);

    Mockito.when(configuration.get(AzureKeyVaultCredentialStore.CREDENTIAL_RETRY_PROP,
        AzureKeyVaultCredentialStore.CREDENTIAL_RETRY_DEFAULT
    ))
           .thenReturn(AzureKeyVaultCredentialStore.CREDENTIAL_RETRY_DEFAULT);

    Mockito.when(context.getConfig(store.CACHE_EXPIRATION_PROP)).thenReturn(null);

    Assert.assertEquals(0, store.init(context).size());
  }

  @Test
  public void testEncodeDecode() {
    AzureKeyVaultCredentialStore store = new AzureKeyVaultCredentialStore();
    store = Mockito.spy(store);
    Mockito.doReturn(Mockito.mock(CredentialStore.Context.class)).when(store).getContext();

    Assert.assertEquals("g" +
        AzureKeyVaultCredentialStore.DELIMITER_FOR_CACHE_KEY +
        "n" +
        AzureKeyVaultCredentialStore.DELIMITER_FOR_CACHE_KEY +
        "o", store.encode("g", "n", "o"));
    Assert.assertArrayEquals(new String[]{"g", "n", "o"},
        store.decode("g" +
            AzureKeyVaultCredentialStore.DELIMITER_FOR_CACHE_KEY +
            "n" +
            AzureKeyVaultCredentialStore.DELIMITER_FOR_CACHE_KEY +
            "o")
    );
  }

  @Test
  public void testCacheEncodeDecode() {
    AzureKeyVaultCredentialStore store = new AzureKeyVaultCredentialStore();
    store = Mockito.spy(store);
    Mockito.doReturn(Mockito.mock(CredentialStore.Context.class)).when(store).getContext();

    String encoded = store.encode("g", "n", "o");
    Assert.assertArrayEquals(new String[]{"g", "n", "o"}, store.decode(encoded));
    encoded = store.encode("g", "n", "");
    Assert.assertArrayEquals(new String[]{"g", "n", ""}, store.decode(encoded));
  }

  @Test
  public void testAzureKeyVaultCredentialValueOptions() throws StageException {
    AzureKeyVaultCredentialStore store = new AzureKeyVaultCredentialStore();
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    store = Mockito.spy(store);

    KeyVaultClient keyVaultClient = PowerMockito.mock(KeyVaultClient.class);
    Mockito.doReturn(keyVaultClient).when(store).createClient();
    Mockito.when(keyVaultClient.getSecret(Mockito.any(), Mockito.any())).thenReturn(new SecretBundle());

    Mockito.when(context.getConfig(Mockito.any())).thenReturn("test");

    Configuration configuration = Mockito.mock(Configuration.class);
    Mockito.doReturn(configuration).when(store).getConfiguration();
    Mockito.when(configuration.get(AzureKeyVaultCredentialStore.CREDENTIAL_REFRESH_PROP,
        AzureKeyVaultCredentialStore.CREDENTIAL_REFRESH_DEFAULT
    ))
           .thenReturn(AzureKeyVaultCredentialStore.CREDENTIAL_REFRESH_DEFAULT);

    Mockito.when(configuration.get(AzureKeyVaultCredentialStore.CREDENTIAL_RETRY_PROP,
        AzureKeyVaultCredentialStore.CREDENTIAL_RETRY_DEFAULT
    ))
           .thenReturn(AzureKeyVaultCredentialStore.CREDENTIAL_RETRY_DEFAULT);

    Mockito.when(context.getConfig(store.CACHE_EXPIRATION_PROP)).thenReturn(null);

    Assert.assertTrue(store.init(context).isEmpty());

    CredentialValue c = store.get("g", "n", "refresh=1,retry=2");
    Assert.assertNotNull(c);
    AzureKeyVaultCredentialStore.AzureKeyVaultCredentialValue
        cc
        = (AzureKeyVaultCredentialStore.AzureKeyVaultCredentialValue) c;
    Assert.assertEquals(1L, cc.getRefreshMillis());
    Assert.assertEquals(2L, cc.getRetryMillis());

    store.destroy();
  }

  @Test
  public void testCache() throws StageException, InterruptedException {
    AzureKeyVaultCredentialStore store = new AzureKeyVaultCredentialStore();
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    store = Mockito.spy(store);

    KeyVaultClient keyVaultClient = PowerMockito.mock(KeyVaultClient.class);
    Mockito.doReturn(keyVaultClient).when(store).createClient();
    SecretBundle secretBundle = Mockito.mock(SecretBundle.class);
    Mockito.when(secretBundle.value()).thenReturn("secret");
    Mockito.when(keyVaultClient.getSecret(Mockito.any(), Mockito.any())).thenReturn(secretBundle);

    Mockito.when(context.getConfig(Mockito.any())).thenReturn("test");

    Configuration configuration = Mockito.mock(Configuration.class);
    Mockito.doReturn(configuration).when(store).getConfiguration();
    Mockito.when(configuration.get(AzureKeyVaultCredentialStore.CREDENTIAL_REFRESH_PROP,
        AzureKeyVaultCredentialStore.CREDENTIAL_REFRESH_DEFAULT
    ))
           .thenReturn(AzureKeyVaultCredentialStore.CREDENTIAL_REFRESH_DEFAULT);

    Mockito.when(configuration.get(AzureKeyVaultCredentialStore.CREDENTIAL_RETRY_PROP,
        AzureKeyVaultCredentialStore.CREDENTIAL_RETRY_DEFAULT
    ))
           .thenReturn(AzureKeyVaultCredentialStore.CREDENTIAL_RETRY_DEFAULT);

    Mockito.when(context.getConfig(store.CACHE_EXPIRATION_PROP)).thenReturn(null);

    Assert.assertTrue(store.init(context).isEmpty());
    CredentialValue credential1 = store.get("g", "n", "a=A,b=B");
    Assert.assertNotNull(credential1);
    Assert.assertEquals("secret", credential1.get());

    //within cache time
    CredentialValue credential2 = store.get("g", "n", "a=A,b=B");
    Assert.assertEquals(((AzureKeyVaultCredentialStore.AzureKeyVaultCredentialValue) credential1).getName(),
        ((AzureKeyVaultCredentialStore.AzureKeyVaultCredentialValue) credential2).getName()
    );
    Assert.assertEquals(((AzureKeyVaultCredentialStore.AzureKeyVaultCredentialValue) credential1).getOptions(),
        ((AzureKeyVaultCredentialStore.AzureKeyVaultCredentialValue) credential2).getOptions()
    );
    Assert.assertEquals(((AzureKeyVaultCredentialStore.AzureKeyVaultCredentialValue) credential1).getGroup(),
        ((AzureKeyVaultCredentialStore.AzureKeyVaultCredentialValue) credential2).getGroup()
    );

    Thread.sleep(201);
    //outside cache time.
    CredentialValue credential3 = store.get("g", "n", "a=A,b=B");
    Assert.assertNotSame(credential1, credential3);

    store.destroy();
  }
}
