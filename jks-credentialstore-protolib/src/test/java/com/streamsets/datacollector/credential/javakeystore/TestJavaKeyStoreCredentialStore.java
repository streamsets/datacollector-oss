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
package com.streamsets.datacollector.credential.javakeystore;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.security.KeyStore;
import java.util.Properties;
import java.util.UUID;

public class TestJavaKeyStoreCredentialStore {
  private File testDir;

  @Before
  public void setup() throws Exception {
    testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    Properties properties = new Properties();
    properties.setProperty("credentialStore.id.keystore.type", "PKCS12");
    properties.setProperty("credentialStore.id.keystore.file", "credentialStore");
    properties.setProperty("credentialStore.id.keystore.storePassword", "1234567890A");
    try (OutputStream os = new FileOutputStream(new File(testDir, "sdc.properties"))) {
      properties.store(os, "");
    }
    System.setProperty("sdc.conf.dir", testDir.getAbsolutePath());
  }

  @After
  public void cleanup() {
    System.getProperties().remove("sdc.conf.dir");
  }

  @Test
  public void testInitOk() {
    JavaKeyStoreCredentialStore store = new JavaKeyStoreCredentialStore();
    store = Mockito.spy(store);

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("JCEKS");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("file");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");

    KeyStore keyStore = Mockito.mock(KeyStore.class);
    Mockito.doReturn(keyStore).when(store).loadKeyStore();

    Assert.assertTrue(store.init(context).isEmpty());
    Assert.assertEquals("JCEKS", store.getKeystoreType());
    Assert.assertEquals("file", store.getKeyStoreFile().getName());
    Assert.assertEquals("password", store.getKeystorePassword());
    Assert.assertEquals(context, store.getContext());
    Assert.assertEquals(keyStore, store.getKeyStore());
  }

  @Test
  public void testInitOkAbsolutePath() {
    JavaKeyStoreCredentialStore store = new JavaKeyStoreCredentialStore();
    store = Mockito.spy(store);

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("JCEKS");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("/file");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");

    KeyStore keyStore = Mockito.mock(KeyStore.class);
    Mockito.doReturn(keyStore).when(store).loadKeyStore();

    Assert.assertTrue(store.init(context).isEmpty());
    Assert.assertEquals(new File( "/file"), store.getKeyStoreFile());
  }

  @Test
  public void testInitOkRelativePath() {
    JavaKeyStoreCredentialStore store = new JavaKeyStoreCredentialStore();
    store = Mockito.spy(store);

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("PKCS12");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("file");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");

    KeyStore keyStore = Mockito.mock(KeyStore.class);
    Mockito.doReturn(keyStore).when(store).loadKeyStore();

    Assert.assertTrue(store.init(context).isEmpty());
    Assert.assertEquals(new File( testDir,"file"), store.getKeyStoreFile());
  }

  @Test
  public void testInitInvalidKeystoreType() {
    JavaKeyStoreCredentialStore store = new JavaKeyStoreCredentialStore();
    store = Mockito.spy(store);

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("JKS");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("file");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");

    Assert.assertEquals(1, store.init(context).size());
  }

  @Test
  public void testStoreGetDeleteList() throws StageException {
    JavaKeyStoreCredentialStore store = new JavaKeyStoreCredentialStore();
    store = Mockito.spy(store);

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("PKCS12");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("file");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");

    //forcing a reload not to use the cached keystore for this test
    Mockito.doReturn(true).when(store).needsToReloadKeyStore();

    Assert.assertTrue(store.init(context).isEmpty());

    try {
      store.get("group", "name", "");
      Assert.fail();
    } catch (StageException ex) {
    }

    Assert.assertTrue(store.getAliases().isEmpty());
    store.storeCredential("name", "password");
    Assert.assertEquals("password", store.get("group", "name", "").get());
    Assert.assertEquals(1, store.getAliases().size());
    Assert.assertEquals("name", store.getAliases().get(0));
    store.deleteCredential("name");

    try {
      store.get("group", "name", "");
      Assert.fail();
    } catch (StageException ex) {
    }

    Assert.assertTrue(store.getAliases().isEmpty());
  }

  @Test
  public void testNeedsToReloadKeyStore() {
    JavaKeyStoreCredentialStore store = new JavaKeyStoreCredentialStore();
    store = Mockito.spy(store);

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("PKCS12");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("file");
    Mockito.when(context.getConfig(Mockito.eq(JavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");

    Assert.assertTrue(store.init(context).isEmpty());

    //null keystore
    Assert.assertTrue(store.needsToReloadKeyStore());

    //not null keystore file older than loading time
    Mockito.doReturn(Mockito.mock(KeyStore.class)).when(store).getKeyStore();
    Mockito.doReturn(10000L).when(store).getKeystoreTimestamp();
    File file = Mockito.mock(File.class);
    Mockito.when(file.lastModified()).thenReturn(9999L);
    Mockito.doReturn(file).when(store).getKeyStoreFile();
    Mockito.doReturn(20000L).when(store).now();
    Assert.assertFalse(store.needsToReloadKeyStore());

    //not null keystore file equal than loading time but not over 10secs
    Mockito.doReturn(Mockito.mock(KeyStore.class)).when(store).getKeyStore();
    Mockito.doReturn(10000L).when(store).getKeystoreTimestamp();
    file = Mockito.mock(File.class);
    Mockito.when(file.lastModified()).thenReturn(10000L);
    Mockito.doReturn(file).when(store).getKeyStoreFile();
    Mockito.doReturn(20000L).when(store).now();
    Assert.assertFalse(store.needsToReloadKeyStore());

    //not null keystore file greater than loading time but not over 10secs
    Mockito.doReturn(Mockito.mock(KeyStore.class)).when(store).getKeyStore();
    Mockito.doReturn(10000L).when(store).getKeystoreTimestamp();
    file = Mockito.mock(File.class);
    Mockito.when(file.lastModified()).thenReturn(10001L);
    Mockito.doReturn(file).when(store).getKeyStoreFile();
    Mockito.doReturn(20001L).when(store).now();
    Assert.assertFalse(store.needsToReloadKeyStore());

    //not null keystore file greater than loading time  over 10secs
    Mockito.doReturn(Mockito.mock(KeyStore.class)).when(store).getKeyStore();
    Mockito.doReturn(10000L).when(store).getKeystoreTimestamp();
    file = Mockito.mock(File.class);
    Mockito.when(file.lastModified()).thenReturn(10001L);
    Mockito.doReturn(file).when(store).getKeyStoreFile();
    Mockito.doReturn(20002L).when(store).now();
    Assert.assertTrue(store.needsToReloadKeyStore());
  }

}
