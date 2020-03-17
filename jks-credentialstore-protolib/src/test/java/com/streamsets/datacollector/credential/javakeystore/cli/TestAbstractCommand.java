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
package com.streamsets.datacollector.credential.javakeystore.cli;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.credential.javakeystore.AbstractJavaKeyStoreCredentialStore;
import com.streamsets.datacollector.credential.javakeystore.Errors;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.credential.CredentialStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import java.util.UUID;

@RunWith(Parameterized.class)
public abstract class TestAbstractCommand<T extends AbstractJavaKeyStoreCredentialStore> {
  private final String productName;
  private String confDir;
  protected AbstractCommand<T> command;
  protected T store;

  public TestAbstractCommand(String productName) {
    this.productName = productName;
  }

  @Parameterized.Parameters(name = "Product name: {0}")
  public static Collection<Object[]> data() throws Exception {
    final LinkedList<Object[]> params = new LinkedList<>();
    params.add(new Object[] {"sdc"});
    params.add(new Object[] {"transformer"});
    return params;
  }

  @Before
  public void setup() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    Properties properties = new Properties();
    properties.setProperty("foo", "bar");
    properties.setProperty("credentialStore.id.config.keystore.type", "PKCS12");
    properties.setProperty("credentialStore.id.config.keystore.file", "credentialStore");
    properties.setProperty("credentialStore.id.config.keystore.storePassword", "1234567890A");
    try (OutputStream os = new FileOutputStream(new File(dir, String.format("%s.properties", productName)))) {
      properties.store(os, "");
    }
    confDir = dir.getAbsolutePath();
    System.setProperty(String.format("%s.conf.dir", productName), confDir);
  }

  @After
  public void cleanup() {
    System.getProperties().remove(String.format("%s.conf.dir", productName));
    Configuration.setFileRefsBaseDir(null);
  }

  @Test
  public void testCreateContext() {
    Configuration configuration = new Configuration();
    configuration.set("credentialStore.id.config.foo", "bar");
    command.productName = productName;
    command.storeId = "id";
    CredentialStore.Context context = command.createContext(configuration, confDir);

    Assert.assertEquals("id", context.getId());
    Assert.assertEquals("bar", context.getConfig("foo"));
    Assert.assertTrue(context.createConfigIssue(Errors.JKS_CRED_STORE_000).toString().contains(Errors.JKS_CRED_STORE_000.toString()));
  }

  @Test
  public void testLoadConfiguration() {
    command.productName = productName;
    command.storeId = "id";
    Configuration configuration = command.loadConfiguration(productName, confDir);
    Assert.assertEquals(3, configuration.getValues().size());
    Assert.assertNull(configuration.get("foo", null));
  }


  @Test
  public void testRunOK() {
    command.productName = productName;
    command.storeId = "id";

    Mockito.doReturn(store).when(command).createStore();
    Configuration configuration = new Configuration();
    Mockito.doReturn(configuration).when(command).loadConfiguration(productName, confDir);
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.doReturn(context).when(command).createContext(Mockito.eq(configuration), Mockito.eq(confDir));
    Mockito.when(store.init(Mockito.eq(context))).thenReturn(Collections.emptyList());

    command.run();
    Mockito.verify(store, Mockito.times(1)).init(Mockito.eq(context));
    Mockito.verify(command, Mockito.times(1)).execute(Mockito.eq(store));
    Mockito.verify(store, Mockito.times(1)).destroy();
  }

  @Test
  public void testRunInitFail() {
    command.productName = productName;
    command.storeId = "id";

    Mockito.doReturn(store).when(command).createStore();
    Configuration configuration = new Configuration();
    Mockito.doReturn(configuration).when(command).loadConfiguration(productName, confDir);
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.doReturn(context).when(command).createContext(Mockito.eq(configuration), Mockito.eq(confDir));
    Mockito.when(store.init(Mockito.eq(context))).thenReturn(ImmutableList.of(Mockito.mock(CredentialStore.ConfigIssue.class)));

    try {
      command.run();
      Assert.fail();
    } catch (RuntimeException ex) {
    }
    Mockito.verify(store, Mockito.times(1)).init(Mockito.eq(context));
    Mockito.verify(command, Mockito.never()).execute(Mockito.any());
    Mockito.verify(store, Mockito.times(1)).destroy();
  }


}
