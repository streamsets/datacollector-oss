/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.util.Configuration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VaultIT {
  private static final Logger LOG = LoggerFactory.getLogger(VaultIT.class);

  private static final String VAULT_VERSION = "latest";
  private static final int VAULT_PORT = 8200;
  private static final String VAULT_DEV_ROOT_TOKEN_ID = "root-token";
  private static final String VAULT_DEV_LISTEN_ADDRESS = "0.0.0.0:" + VAULT_PORT;

  @ClassRule
  public static GenericContainer vaultContainer = new GenericContainer("cgswong/vault:" + VAULT_VERSION)
      .withExposedPorts(VAULT_PORT)
      .withEnv("VAULT_DEV_ROOT_TOKEN_ID", VAULT_DEV_ROOT_TOKEN_ID)
      .withEnv("VAULT_DEV_LISTEN_ADDRESS", VAULT_DEV_LISTEN_ADDRESS)
      .withCommand("server", "-dev", "-log-level=debug");

  @BeforeClass
  public static void setUpClass() throws Exception {
    VaultConfiguration conf = VaultConfigurationBuilder.newVaultConfiguration()
        .withAddress(
            "http://" + vaultContainer.getContainerIpAddress() + ":" + vaultContainer.getMappedPort(VAULT_PORT)
        )
        .withToken(VAULT_DEV_ROOT_TOKEN_ID)
        .build();

    VaultClient client = new VaultClient(conf);
    boolean enabled = client.sys().auth().enable("app-id", "app-id");
    LOG.info("Enabled app-id: {}", enabled);
    client.logical().write("auth/app-id/map/app-id/foo", ImmutableMap.<String, Object>of("value", "root"));
    client.logical()
        .write("auth/app-id/map/user-id/" + Vault.calculateUserId(), ImmutableMap.<String, Object>of("value", "foo"));

    client.logical().write("secret/hello", ImmutableMap.<String, Object>of("value", "world!"));

    Configuration sdcProperties = new Configuration();
    sdcProperties.set("vault.addr", conf.getAddress());
    sdcProperties.set("vault.app.id", "foo");
    Vault.loadRuntimeConfiguration(sdcProperties);
  }

  @Test
  public void testToken() throws Exception {
    assertTrue(Vault.token() != null && !Vault.token().isEmpty());
  }

  @Test
  public void testRead() throws Exception {
    assertEquals("world!", Vault.read("secret/hello", "value"));
  }
}
