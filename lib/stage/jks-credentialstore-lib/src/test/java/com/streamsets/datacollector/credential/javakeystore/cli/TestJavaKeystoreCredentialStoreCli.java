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

import com.streamsets.datacollector.util.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.UUID;

public class TestJavaKeystoreCredentialStoreCli {

  @Before
  public void setup() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    Properties properties = new Properties();
    properties.setProperty("credentialStore.jks.config.keystore.type", "PKCS12");
    properties.setProperty("credentialStore.jks.config.keystore.file", "credentialStore");
    properties.setProperty("credentialStore.jks.config.keystore.storePassword", "1234567890A");
    try (OutputStream os = new FileOutputStream(new File(dir, "sdc.properties"))) {
      properties.store(os, "");
    }
    System.setProperty("sdc.conf.dir", dir.getAbsolutePath());
  }

  @After
  public void cleanup() {
    System.getProperties().remove("sdc.conf.dir");
    Configuration.setFileRefsBaseDir(null);
  }

  @Test
  public void testListNoCredentials() throws Exception {
    Assert.assertTrue(new JavaKeystoreCredentialStoreCli().doMain(new String[]{"list", "-i", "jks"}));
  }

  @Test
  public void testAddNoCredentials() throws Exception {
    Assert.assertTrue(new JavaKeystoreCredentialStoreCli().doMain(new String[]{
        "add",
        "-i",
        "jks",
        "-n",
        "foo",
        "-c",
        "secret"
    }));
  }

  @Test
  public void testDeleteNoCredentials() throws Exception {
    Assert.assertFalse(new JavaKeystoreCredentialStoreCli().doMain(new String[]{"delete", "-i", "jks", "-n", "foo"}));
  }

  @Test
  public void testCredentials() throws Exception {
    Assert.assertTrue(new JavaKeystoreCredentialStoreCli().doMain(new String[]{
        "add",
        "-i",
        "jks",
        "-n",
        "foo",
        "-c",
        "secret"
    }));
    Assert.assertTrue(new JavaKeystoreCredentialStoreCli().doMain(new String[]{
        "add",
        "-i",
        "jks",
        "-n",
        "foo1",
        "-c",
        "secret1"
    }));
    Assert.assertTrue(new JavaKeystoreCredentialStoreCli().doMain(new String[]{
        "add",
        "-i",
        "jks",
        "-n",
        "foo2",
        "-c",
        "secret2"
    }));
    Assert.assertTrue(new JavaKeystoreCredentialStoreCli().doMain(new String[]{
        "add",
        "-i",
        "jks",
        "-n",
        "foo3",
        "-c",
        "secret3"
    }));
    Assert.assertTrue(new JavaKeystoreCredentialStoreCli().doMain(new String[]{
        "add",
        "-i",
        "jks",
        "-n",
        "foo4",
        "-c",
        "secret4"
    }));
    Assert.assertTrue(new JavaKeystoreCredentialStoreCli().doMain(new String[]{"list", "-i", "jks"}));
    Assert.assertTrue(new JavaKeystoreCredentialStoreCli().doMain(new String[]{"delete", "-i", "jks", "-n", "foo"}));
    Assert.assertTrue(new JavaKeystoreCredentialStoreCli().doMain(new String[]{"list", "-i", "jks"}));
  }

}
