/**
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
package com.streamsets.datacollector.vault;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.streamsets.datacollector.util.Configuration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VaultIT {
  private static final Logger LOG = LoggerFactory.getLogger(VaultIT.class);

  private static final String VAULT_VERSION = "latest";
  private static final int VAULT_PORT = 8200;
  private static final String VAULT_DEV_ROOT_TOKEN_ID = "root-token";
  private static final String VAULT_DEV_LISTEN_ADDRESS = "0.0.0.0:" + VAULT_PORT;

  private static Vault vault;

  @ClassRule
  public static GenericContainer vaultContainer = new GenericContainer("kunickiaj/vault:" + VAULT_VERSION)
      .withExposedPorts(VAULT_PORT)
      .withEnv("VAULT_DEV_ROOT_TOKEN_ID", VAULT_DEV_ROOT_TOKEN_ID)
      .withEnv("VAULT_DEV_LISTEN_ADDRESS", VAULT_DEV_LISTEN_ADDRESS)
      .withCommand("server", "-dev", "-log-level=debug");

  @BeforeClass
  public static void setUpClass() throws Exception {
    URL url = Resources.getResource("all_policy.json");
    String allPolicy = Resources.toString(url, Charsets.UTF_8);

    VaultConfiguration conf = VaultConfigurationBuilder.newVaultConfiguration()
        .withAddress(
            "http://" + vaultContainer.getContainerIpAddress() + ":" + vaultContainer.getMappedPort(VAULT_PORT)
        )
        .withToken(VAULT_DEV_ROOT_TOKEN_ID)
        .build();

    VaultClient client = new VaultClient(conf);
    boolean enabled = client.sys().auth().enable("app-id", "app-id");
    LOG.info("Enabled app-id: {}", enabled);
    // Mount transit back end for testing nested maps
    client.sys().mounts().mount("transit", "transit");
    LOG.info("Mounted back-end 'transit' to 'transit'");
    client.sys().policy().create("all", allPolicy);
    client.logical().write("auth/app-id/map/app-id/foo", ImmutableMap.of("value", "all"));
    client.logical()
        .write("auth/app-id/map/user-id/" + Vault.calculateUserId(), ImmutableMap.of("value", "foo"));

    client.logical().write("secret/hello", ImmutableMap.of("value", "world!"));
    client.logical().write("transit/keys/sdc", ImmutableMap.of("exportable", true));
    Secret key = client.logical().read("transit/keys/sdc");
    Configuration sdcProperties = new Configuration();
    sdcProperties.set("vault.addr", conf.getAddress());
    sdcProperties.set("vault.app.id", "foo");
    vault = new Vault(sdcProperties);
  }

  @Test
  public void testToken() throws Exception {
    assertTrue(vault.token() != null && !vault.token().isEmpty());
  }

  @Test
  public void testRead() throws Exception {
    assertEquals("world!", vault.read("secret/hello", "value"));
  }

  @Test
  public void testNestedKeys() throws Exception {
    vault.read("transit/export/encryption-key/sdc", "keys/1");
  }
}
