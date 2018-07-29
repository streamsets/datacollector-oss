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
package com.streamsets.datacollector.vault;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.credential.CredentialStore;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.net.URL;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class VaultIT {
  private static final Logger LOG = LoggerFactory.getLogger(VaultIT.class);

  private static final String VAULT_VERSION = "latest";
  private static final int VAULT_PORT = 8200;
  private static final String VAULT_DEV_ROOT_TOKEN_ID = "root-token";
  private static final String VAULT_DEV_LISTEN_ADDRESS = "0.0.0.0:" + VAULT_PORT;

  private static Vault vault;
  private static VaultConfiguration conf;
  private static String roleId;
  private static String secretId;

  @Parameterized.Parameters
  public static Collection<Object> authBackends() {
    return ImmutableList.of(Vault.AuthBackend.APP_ID, Vault.AuthBackend.APP_ROLE);
  }

  @Parameterized.Parameter
  public static Vault.AuthBackend authBackend;

  @ClassRule
  public static GenericContainer vaultContainer = new GenericContainer("vault:" + VAULT_VERSION)
      .withExposedPorts(VAULT_PORT)
      .withEnv("VAULT_DEV_ROOT_TOKEN_ID", VAULT_DEV_ROOT_TOKEN_ID)
      .withEnv("VAULT_DEV_LISTEN_ADDRESS", VAULT_DEV_LISTEN_ADDRESS)
      .withEnv("SKIP_SETCAP", "true")
      .withCommand("server", "-dev", "-log-level=debug");

  @BeforeClass
  public static void setUpClass() throws Exception {
    URL url = Resources.getResource("all_policy.json");
    String allPolicy = Resources.toString(url, Charsets.UTF_8);

    conf = VaultConfigurationBuilder.newVaultConfiguration()
        .withAddress(
            "http://" + vaultContainer.getContainerIpAddress() + ":" + vaultContainer.getMappedPort(VAULT_PORT)
        )
        .withToken(VAULT_DEV_ROOT_TOKEN_ID)
        .build();

    VaultClient client = new VaultClient(conf);
    // Mount legacy app id back end
    boolean enabled = client.sys().auth().enable("app-id", "app-id");
    LOG.info("Enabled app-id: {}", enabled);
    // Mount AppRole back end
    enabled = client.sys().auth().enable("approle", "approle");
    LOG.info("Enabled approle: {}", enabled);
    // Mount transit back end for testing nested maps
    client.sys().mounts().mount("transit", "transit");
    LOG.info("Mounted back-end 'transit' to 'transit'");
    client.sys().policy().create("all", allPolicy);

    // Setup AppID
    client.logical().write("auth/app-id/map/app-id/foo", ImmutableMap.of("value", "all"));
    client.logical()
        .write("auth/app-id/map/user-id/" + Vault.calculateUserId("*"), ImmutableMap.of("value", "foo"));

    // Setup AppRole
    client.logical().write("auth/approle/role/foo", ImmutableMap.of("policies", "all"));
    Secret roleIdSecret = client.logical().read("auth/approle/role/foo/role-id");
    roleId = (String) roleIdSecret.getData().get("role_id");
    Secret secretIdSecret = client.logical().write("auth/approle/role/foo/secret-id", Collections.emptyMap());
    secretId = (String) secretIdSecret.getData().get("secret_id");

    LOG.info("Using Role & Secret: '{}':'{}'", roleId, secretId);

    client.logical().write("secret/hello", ImmutableMap.of("value", "world!"));
    client.logical().write("transit/keys/sdc", ImmutableMap.of("exportable", true));
    Secret key = client.logical().read("transit/keys/sdc");
  }

  @Before
  public void setUp() {
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getConfig(Mockito.eq("addr"))).thenReturn(conf.getAddress());

    if (authBackend == Vault.AuthBackend.APP_ID) {
      Mockito.when(context.getConfig(Mockito.eq("app.id"))).thenReturn("foo");
    } else {
      Mockito.when(context.getConfig(Mockito.eq("role.id"))).thenReturn(roleId);
      Mockito.when(context.getConfig(Mockito.eq("secret.id"))).thenReturn(secretId);
    }
    vault = new Vault(context);
    vault.init();
  }

  @After
  public void cleanUp() {
    vault.destroy();
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
