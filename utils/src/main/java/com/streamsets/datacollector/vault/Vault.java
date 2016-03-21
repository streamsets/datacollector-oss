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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.vault.api.VaultException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Vault {
  private static final Logger LOG = LoggerFactory.getLogger(Vault.class);
  private static final HashFunction HASH_FUNCTION = Hashing.sha256();
  private static final ConcurrentMap<String, Secret> SECRETS = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Long> LEASES = new ConcurrentHashMap<>();
  private static final ScheduledExecutorService EXECUTOR = Executors.newSingleThreadScheduledExecutor();
  private static final String VAULT_ADDR = "vault.addr";

  private static String appId;
  private static String userId = null;
  private static VaultConfiguration config;
  private static long leaseExpirationBuffer;
  private static long authExpirationTime;
  private static boolean initialized = false;
  private static long renewalInterval;

  private Vault() {
  }

  private static class VaultRenewalTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(VaultRenewalTask.class);
    private final ConcurrentMap<String, Long> leases;
    private final ConcurrentMap<String, Secret> secrets;

    VaultRenewalTask(ConcurrentMap<String, Long> leases, ConcurrentMap<String, Secret> secrets) {
      this.leases = leases;
      this.secrets = secrets;
    }

    @Override
    public void run() {
      try {
        // Remove any expired leases (non-renewable)
        purgeExpiredLeases();

        // Attempt to renew remaining leases
        // Remove any leases that failed to renew successfully.
        purgeFailedRenewals(renewLeases());
      } catch (Throwable t) { // NOSONAR
        LOG.error("Error in lease renewer: {}", t.toString(), t);
      }

      LOG.debug("Completed lease renewal.");
    }

    private void purgeFailedRenewals(List<String> failedRenewalLeases) {
      // Remove any leases that failed to renew
      for (String lease : failedRenewalLeases) {
        LOG.debug("Removing lease '{}' as expired.", lease);
        leases.remove(lease);
      }
    }

    private List<String> renewLeases() {
      List<String> failedRenewalLeases = new ArrayList<>();
      for (Map.Entry<String, Long> lease : leases.entrySet()) {
        LOG.debug("Attempting renewal for leaseId '{}'", lease.getKey());
        if (!renewLease(secrets, lease.getKey())) {
          failedRenewalLeases.add(lease.getKey());
        }
      }
      return failedRenewalLeases;
    }

    private boolean renewLease(Map<String, Secret> secrets, String leaseId) {
      VaultClient vault = new VaultClient(config);

      try {
        Secret renewal = vault.sys().lease().renew(leaseId);
        LOG.debug("Renewed lease '{}' for '{}' seconds", renewal.getLeaseId(), renewal.getLeaseDuration());
        leases.put(renewal.getLeaseId(), System.currentTimeMillis() + (renewal.getLeaseDuration() * 1000));
      } catch (VaultException | RuntimeException e) {
        // We catch IllegalStateException to make sure this lease is removed because there seems to be a bug in
        // Vault that returns a 204 instead of a Secret when trying to renew STS credentials
        // SDC doesn't support STS today, so it is also unlikely for someone to hit this error.
        LOG.error("Failed to renew lease for '{}'", leaseId, e);
        secrets.remove(getPath(leaseId));
        return false;
      }
      return true;
    }

    /**
     * Since we have no way to inform a running pipeline of new credentials, it is likely
     * that the pipeline will simply fail at some point and will have to be restarted
     * either manually or via the built-in retry feature.
     *
     * This means that as long as we simply evict the expired LEASES they should be
     * renewed automatically when they are requested.
     */
    private void purgeExpiredLeases() {
      List<String> expiredLeases = new ArrayList<>(leases.size());
      for (Map.Entry<String, Long> lease : leases.entrySet()) {
        if (lease.getValue() - System.currentTimeMillis() <= leaseExpirationBuffer) {
          expiredLeases.add(lease.getKey());
          secrets.remove(getPath(lease.getKey()));
        }
      }

      for (String lease : expiredLeases) {
        leases.remove(lease);
        LOG.debug("Removing lease '{}' as expired", lease);
      }
    }
  }

  /**
   * The user-id portion of Vault's app-auth should be machine specific and at least
   * somewhat obfuscated to make it more difficult to derive. We use the sha256 hash of
   * the MAC address of the first interface found by InetAddress.getLocalHost().
   *
   * This provides a way for administrators to compute the value itself during/after
   * deployment and authorize the app-id, user-id pair out of band.
   *
   * @return String representation of the user-id portion of an auth token.
   */
  static String calculateUserId() {
    if (userId != null) {
      return userId;
    }

    try {
      InetAddress ip = InetAddress.getLocalHost();
      NetworkInterface netIf = NetworkInterface.getByInetAddress(ip);
      byte[] mac = netIf.getHardwareAddress();

      Hasher hasher = HASH_FUNCTION.newHasher(6); // MAC is 6 bytes.
      hasher.putBytes(mac);
      return hasher.hash().toString();
    } catch (IOException e) {
      LOG.error("Could not compute Vault user-id: '{}'", e.toString(), e);
      throw new VaultRuntimeException("Could not compute Vault user-id: " + e.toString());
    }
  }

  /**
   * Returns the current auth token.
   * @return vault token
   */
  public static String token() {
    return getConfig().getToken();
  }

  /**
   * Reads a secret from the local cache if it hasn't expired and returns the value for the specified key.
   * If the secret isn't cached or has expired, it requests it from Vault again.
   *
   * @param path path in Vault to read
   * @param key key of the property of the secret represented by the path to return
   * @return value of the specified key for the requested secret.
   */
  public static String read(String path, String key) {
    return read(path, key, 0L);
  }

  /**
   * Reads a secret from the local cache if it hasn't expired and returns the value for the specified key.
   * If the secret isn't cached or has expired, it requests it from Vault again.
   *
   * This version of the method will also add the specified delay in milliseconds before returning, but only
   * if the value wasn't already locally cached. This is because certain backends such as AWS will return
   * a secret (access keys for example) before they have propagated to all AWS services. For AWS a delay of up to 5
   * or 10 seconds may be necessary. If you receive a 403 from AWS services you probably need to increase the delay.
   *
   * @param path path in Vault to read
   * @param key key of the property of the secret represented by the path to return
   * @param delay delay in milliseconds to wait before returning the value if it wasn't already cached.
   * @return value of the specified key for the requested secret.
   */
  public static String read(String path, String key, long delay) {
    if (!initialized) {
      throw new VaultRuntimeException("Cannot call read() until Vault is initialized.");
    }

    if (!SECRETS.containsKey(path)) {
      VaultClient vault = new VaultClient(getConfig());
      Secret secret;

      try {
        secret = vault.logical().read(path);
      } catch (VaultException e) {
        LOG.error(e.toString(), e);
        throw new VaultRuntimeException(e.toString());
      }

      // Record the expiration date of this lease
      String leaseId;
      if (secret.isRenewable()) {
        // Only renewable secrets seem to have a leaseId
        leaseId = secret.getLeaseId();
      } else {
        // So for non-renewable secret's we'll store the path with an extra / so that we can purge them correctly.
        leaseId = path + "/";
      }
      LEASES.put(leaseId, System.currentTimeMillis() + (secret.getLeaseDuration() * 1000));

      SECRETS.put(path, secret);

      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    String value = SECRETS.get(path).getData().get(key).toString();
    LOG.trace("Retrieved value for key '{}'", key);
    return value;
  }

  /**
   * Loads sdc.properties in order to do initialization of the Vault.
   *
   * @param sdcProperties Loaded SDC configuration
   */
  public static void loadRuntimeConfiguration(Configuration sdcProperties) {
    if (!sdcProperties.hasName(VAULT_ADDR) || sdcProperties.get(VAULT_ADDR, "").isEmpty()) {
      // Vault is disabled.
      return;
    }
    // Initial config that doesn't contain auth token
    config = parseVaultConfigs(sdcProperties);
    LOG.debug("Scheduling renewal every '{}' seconds.", renewalInterval);
    EXECUTOR.scheduleWithFixedDelay(
        new VaultRenewalTask(LEASES, SECRETS),
        renewalInterval,
        renewalInterval,
        TimeUnit.SECONDS
    );
    initialized = true;
  }

  /**
   * Returns the path portion of the specified leaseId
   *
   * @param leaseId a leaseId
   * @return path portion of leaseId
   */
  private static String getPath(String leaseId) {
    return leaseId.substring(0, leaseId.lastIndexOf('/') - 1);
  }

  private static VaultConfiguration getConfig() {
    if (authExpirationTime - System.currentTimeMillis() <= 1000) {
      VaultClient vault = new VaultClient(config);
      Secret auth;
      try {
        auth = vault.authenticate().appId(appId, calculateUserId());
      } catch (VaultException e) {
        LOG.error(e.toString(), e);
        throw new VaultRuntimeException(e.toString());
      }
      authExpirationTime = System.currentTimeMillis() + (auth.getAuth().getLeaseDuration() * 1000);

      config = VaultConfigurationBuilder.newVaultConfiguration()
          .fromVaultConfiguration(config)
          .withToken(auth.getAuth().getClientToken())
          .build();
    }
    return config;
  }

  private static VaultConfiguration parseVaultConfigs(Configuration sdcProperties) {
    leaseExpirationBuffer = Long.parseLong(sdcProperties.get("vault.lease.expiration.buffer.sec", "120"));
    renewalInterval = Long.parseLong(sdcProperties.get("vault.lease.renewal.interval.sec", "60"));
    appId = sdcProperties.get("vault.app.id", "");

    if (appId.isEmpty()) {
      throw new VaultRuntimeException("vault.app.id must be specified in sdc.properties");
    }

    config = VaultConfigurationBuilder.newVaultConfiguration()
        .withAddress(sdcProperties.get("vault.addr", VaultConfigurationBuilder.DEFAULT_ADDRESS))
        .withOpenTimeout(Integer.parseInt(sdcProperties.get("vault.open.timeout", "0")))
        .withProxyOptions(
            ProxyOptionsBuilder.newProxyOptions()
            .withProxyAddress(sdcProperties.get("vault.proxy.address", ""))
            .withProxyPort(Integer.parseInt(sdcProperties.get("vault.proxy.port", "8080")))
            .withProxyUsername(sdcProperties.get("vault.proxy.username", ""))
            .withProxyPassword(sdcProperties.get("vault.proxy.password", ""))
            .build()
        )
        .withReadTimeout(Integer.parseInt(sdcProperties.get("vault.read.timeout", "0")))
        .withSslOptions(SslOptionsBuilder
            .newSslOptions()
            .withEnabledProtocols(
                sdcProperties.get("vault.ssl.enabled.protocols", SslOptionsBuilder.DEFAULT_PROTOCOLS)
            )
            .withTrustStoreFile(sdcProperties.get("vault.ssl.truststore.file", ""))
            .withTrustStorePassword(sdcProperties.get("vault.ssl.truststore.password", ""))
            .withSslVerify(Boolean.parseBoolean(sdcProperties.get("vault.ssl.verify", "true")))
            .withSslTimeout(Integer.parseInt(sdcProperties.get("vault.ssl.timeout", "0")))
            .build()
        )
        .withTimeout(Integer.parseInt(sdcProperties.get("vault.timeout", "0")))
        .build();

    return config;
  }
}
