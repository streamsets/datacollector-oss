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
import com.streamsets.pipeline.api.impl.Utils;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public abstract class TestAbstractJavaKeyStoreCredentialStore<T extends AbstractJavaKeyStoreCredentialStore> {
  private File testDir;
  protected T store;
  protected AbstractJavaKeyStoreCredentialStore.JKSManager jksManager;

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
  public void testInitOk() throws Exception {
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("JCEKS");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("file");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");

    KeyStore keyStore = Mockito.mock(KeyStore.class);
    Mockito.doReturn(keyStore).when(jksManager).loadKeyStore();

    Assert.assertTrue(store.init(context).isEmpty());
    Assert.assertEquals("JCEKS", store.getKeystoreType());
    Assert.assertEquals("file", store.getKeyStoreFile().getName());
    Assert.assertEquals("password", store.getKeystorePassword());
    Assert.assertEquals(context, store.getContext());
    Assert.assertEquals(keyStore, jksManager.getKeyStore());
  }

  @Test
  public void testInitOkAbsolutePath() throws Exception {
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("JCEKS");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("/file");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");

    KeyStore keyStore = Mockito.mock(KeyStore.class);
    Mockito.doReturn(keyStore).when(jksManager).loadKeyStore();

    Assert.assertTrue(store.init(context).isEmpty());
    Assert.assertEquals(new File( "/file"), store.getKeyStoreFile());
  }

  @Test
  public void testInitOkRelativePath() throws Exception {
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("PKCS12");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("file");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");
    Mockito.when(context.getStreamSetsConfigDir()).thenReturn(testDir.getAbsolutePath());

    KeyStore keyStore = Mockito.mock(KeyStore.class);
    Mockito.doReturn(keyStore).when(jksManager).loadKeyStore();

    Assert.assertTrue(store.init(context).isEmpty());
    Assert.assertEquals(new File( testDir,"file"), store.getKeyStoreFile());
  }

  @Test
  public void testInitInvalidKeystoreType() {
    AbstractJavaKeyStoreCredentialStore.JKSManager jksManager = Mockito.spy(store.new JKSManager());
    Whitebox.setInternalState(store, "manager", jksManager);
    Mockito.doReturn(jksManager).when(store).createManager();

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("JKS");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("file");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");

    Assert.assertEquals(1, store.init(context).size());
  }

  @Test
  public void testStoreGetDeleteList() throws Exception {
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("PKCS12");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("file");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");

    //forcing a reload not to use the cached keystore for this test
    Mockito.doReturn(true).when(jksManager).needsToReloadKeyStore();

    Assert.assertTrue(store.init(context).isEmpty());

    try {
      store.get("group", "name", "");
      Assert.fail();
    } catch (StageException ex) {
      //Expected
    }

    Assert.assertTrue(store.getNames().isEmpty());
    store.store(AbstractJavaKeyStoreCredentialStore.DEFAULT_SDC_GROUP_AS_LIST, "name", "password");
    Assert.assertEquals("password", store.get("group", "name", "").get());
    Assert.assertEquals(1, store.getNames().size());
    Assert.assertEquals("name", store.getNames().get(0));
    store.delete("name");

    try {
      store.get("group", "name", "");
      Assert.fail();
    } catch (StageException ex) {
    }

    Assert.assertTrue(store.getNames().isEmpty());
  }

  @Test
  public void testNeedsToReloadKeyStore() throws Exception {
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("PKCS12");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("file");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");

    Assert.assertTrue(store.init(context).isEmpty());

    //not null keystore file older than loading time
    Mockito.doReturn(Mockito.mock(KeyStore.class)).when(jksManager).getKeyStore();
    Mockito.doReturn(10000L).when(jksManager).getKeystoreTimestamp();
    File file = Mockito.mock(File.class);
    Mockito.when(file.lastModified()).thenReturn(9999L);
    Mockito.doReturn(file).when(store).getKeyStoreFile();
    Mockito.doReturn(20000L).when(store).now();
    Assert.assertFalse(jksManager.needsToReloadKeyStore());

    //not null keystore file equal than loading time but not over 10secs
    Mockito.doReturn(Mockito.mock(KeyStore.class)).when(jksManager).getKeyStore();
    Mockito.doReturn(10000L).when(jksManager).getKeystoreTimestamp();
    file = Mockito.mock(File.class);
    Mockito.when(file.lastModified()).thenReturn(10000L);
    Mockito.doReturn(file).when(store).getKeyStoreFile();
    Mockito.doReturn(20000L).when(store).now();
    Assert.assertFalse(jksManager.needsToReloadKeyStore());

    //not null keystore file greater than loading time but not over 10secs
    Mockito.doReturn(Mockito.mock(KeyStore.class)).when(jksManager).getKeyStore();
    Mockito.doReturn(10000L).when(jksManager).getKeystoreTimestamp();
    file = Mockito.mock(File.class);
    Mockito.when(file.lastModified()).thenReturn(10001L);
    Mockito.doReturn(file).when(store).getKeyStoreFile();
    Mockito.doReturn(20001L).when(store).now();
    Assert.assertFalse(jksManager.needsToReloadKeyStore());

    //not null keystore file greater than loading time  over 10secs
    Mockito.doReturn(Mockito.mock(KeyStore.class)).when(jksManager).getKeyStore();
    Mockito.doReturn(10000L).when(jksManager).getKeystoreTimestamp();
    file = Mockito.mock(File.class);
    Mockito.when(file.lastModified()).thenReturn(10001L);
    Mockito.doReturn(20002L).when(store).now();
    Assert.assertTrue(jksManager.needsToReloadKeyStore());
  }

  @Test
  public void testKeystoreRefreshMillisNotConfigured() throws Exception {
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("PKCS12");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("file");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");

    Assert.assertTrue(store.init(context).isEmpty());
    Assert.assertNotNull(Whitebox.getInternalState(store, "keystoreRefreshWaitMillis"));
    Assert.assertEquals(AbstractJavaKeyStoreCredentialStore.KEYSTORE_REFRESH_WAIT_MILLIS_DEFAULT, Whitebox.getInternalState(store, "keystoreRefreshWaitMillis"));

  }

  @Test
  public void testKeystoreRefreshMillisConfigured() throws Exception {
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getId()).thenReturn("id");
    Mockito.when(context.getStreamSetsConfigDir()).thenReturn(System.getProperty("sdc.conf.dir"));
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_TYPE_KEY))).thenReturn("PKCS12");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_FILE_KEY))).thenReturn("file");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_PASSWORD_KEY))).thenReturn("password");
    Mockito.when(context.getConfig(Mockito.eq(AbstractJavaKeyStoreCredentialStore.KEYSTORE_REFRESH_WAIT_MILLIS))).thenReturn("5000");

    Assert.assertTrue(store.init(context).isEmpty());
    Assert.assertEquals(5000L, Whitebox.getInternalState(store, "keystoreRefreshWaitMillis"));
    store.store(Collections.singletonList(AbstractJavaKeyStoreCredentialStore.DEFAULT_SDC_GROUP), "abc", "def");

    File keyStoreFile =  store.getKeyStoreFile();

    AbstractJavaKeyStoreCredentialStore.JKSManager manager =
        (AbstractJavaKeyStoreCredentialStore.JKSManager) Whitebox.getInternalState(store, "manager");

    //Check 1 secret is in the store
    Assert.assertEquals(1, store.getNames().size());
    Assert.assertEquals("def", store.get(AbstractJavaKeyStoreCredentialStore.DEFAULT_SDC_GROUP, "abc", "").get());

    // Write a new secret outside of the Credential Store implementation
    KeyStore ks = null;

    try (InputStream is = new FileInputStream(keyStoreFile)) {
      ks = KeyStore.getInstance(store.getKeystoreType());
      ks.load(is, store.getKeystorePassword().toCharArray());
    } catch (Exception ex) {
      Assert.fail(Utils.format("Failed to load keystore '{}': {}", keyStoreFile, ex));
    }

    ks.setEntry(
        "ghi",
        new KeyStore.SecretKeyEntry(new SecretKeySpec("jkl".getBytes(StandardCharsets.UTF_8), "AES")),
        new KeyStore.PasswordProtection(store.getKeystorePassword().toCharArray())
    );

    try (OutputStream os = new FileOutputStream(keyStoreFile)) {
      ks.store(os, store.getKeystorePassword().toCharArray());
    }

    // Check file is not reloaded by credential store until refresh millis expired
    Assert.assertFalse(manager.needsToReloadKeyStore());
    Assert.assertEquals(1, store.getNames().size());

    //Check reload need is detected by the credential store impl after refresh millis expired
    Awaitility.await()
        .atMost(5000, TimeUnit.MILLISECONDS)
        .until(manager::needsToReloadKeyStore);

    // Check 2 secrets are available in the credential store
    Assert.assertEquals(2, store.getNames().size());
    Assert.assertEquals("def", store.get(AbstractJavaKeyStoreCredentialStore.DEFAULT_SDC_GROUP, "abc", "").get());
    Assert.assertEquals("jkl", store.get(AbstractJavaKeyStoreCredentialStore.DEFAULT_SDC_GROUP, "ghi", "").get());
  }

}
