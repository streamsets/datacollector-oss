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

import com.google.common.base.Splitter;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.streamsets.datacollector.vault.api.VaultException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Vault {
  private static final Logger LOG = LoggerFactory.getLogger(Vault.class);
  private static final HashFunction HASH_FUNCTION = Hashing.sha256();
  private static final ScheduledExecutorService EXECUTOR = Executors.newSingleThreadScheduledExecutor();
  private static final String VAULT_ADDR = "addr";
  private static final Splitter mapSplitter = Splitter.on('/').trimResults().omitEmptyStrings();
  private final ConcurrentMap<String, Secret> secrets = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> leases = new ConcurrentHashMap<>();

  private static class Configuration  {
    private final CredentialStore.Context context;

    public Configuration(CredentialStore.Context context) {
      this.context = context;
    }

    public boolean hasName(String name) {
      return context.getConfig(name) != null;
    }

    public String get(String name, String defaultValue) {
      String value = context.getConfig(name);
      return (value != null) ? value : defaultValue;
    }

    public long get(String name, long defaultValue) {
      String value = context.getConfig(name);
      return (value != null) ? Long.parseLong(value) : defaultValue;
    }

    public int get(String name, int defaultValue) {
      String value = context.getConfig(name);
      return (value != null) ? Integer.parseInt(value) : defaultValue;
    }

    public boolean get(String name, boolean defaultValue) {
      String value = context.getConfig(name);
      return (value != null) ? Boolean.parseBoolean(value) : defaultValue;
    }

  }

  private CredentialStore.Context context;
  private String csId;
  private String roleId;
  private AuthBackend authBackend;

  private VaultConfiguration config;
  private long leaseExpirationBuffer;
  private long renewalInterval;
  private volatile long authExpirationTime;
  private Configuration configuration;
  private ScheduledFuture scheduledFuture;

  enum AuthBackend {
    APP_ROLE,
    APP_ID
  }

  public Vault(CredentialStore.Context context) {
    this.context = context;
  }

  public void init() {
    csId = context.getId();
    configuration = new Configuration(context);
    if (configuration.get(VAULT_ADDR, "").isEmpty()) {
      throw new VaultRuntimeException("Vault address not set");
    }

    // Initial config that doesn't contain auth token
    config = parseVaultConfigs(configuration);
    // Trigger an initial login
    config = getConfig();
    LOG.debug("CredentialStore '{}' Vault, scheduling renewal every '{}' seconds", csId, renewalInterval);
    scheduledFuture = EXECUTOR.scheduleWithFixedDelay(
        new VaultRenewalTask(leases, secrets),
        renewalInterval,
        renewalInterval,
        TimeUnit.SECONDS
    );

  }

  public void destroy() {
    LOG.debug("CredentialStore '{}' Vault, canceling renewal", csId);
    scheduledFuture.cancel(true);
  }

  private class VaultRenewalTask implements Runnable {
    private final ConcurrentMap<String, Long> leases;
    private final ConcurrentMap<String, Secret> secrets;

    VaultRenewalTask(ConcurrentMap<String, Long> leases, ConcurrentMap<String, Secret> secrets) {
      this.leases = leases;
      this.secrets = secrets;
    }

    @Override
    public void run() {
      try {
        // Renew auth token if needed.
        renewAuthToken();

        // Remove any expired leases (non-renewable)
        purgeExpiredLeases();

        // Attempt to renew remaining leases
        // Remove any leases that failed to renew successfully.
        purgeFailedRenewals(renewLeases());
      } catch (Throwable t) { // NOSONAR
        LOG.error("CredentialStore '{}' Vault, error in lease renewer: {}", csId, t);
      }

      LOG.debug("CredentialStore '{}' Vault, completed lease renewal.", csId);
    }

    private void renewAuthToken() {
      if (authExpirationTime == Long.MAX_VALUE ||
          authExpirationTime - System.currentTimeMillis() > leaseExpirationBuffer) {
        // no op
        return;
      }

      VaultClient vault = new VaultClient(config);
      try {
        Secret token = vault.authenticate().renewSelf();
        long leaseDuration = token.getAuth().getLeaseDuration();
        LOG.debug("CredentialStore '{}' Vault, successfully renewed auth token by '{}'", csId, leaseDuration);
        if (leaseDuration > 0) {
          authExpirationTime = System.currentTimeMillis() + (leaseDuration * 1000);
        } else {
          authExpirationTime = Long.MAX_VALUE;
        }
      } catch (VaultException e) {
        LOG.error("CredentialStore '{}' Vault, Failed to renew auth token", csId, e);
      } finally {
        authExpirationTime = 0; // force a new login next request.
      }
    }

    private void purgeFailedRenewals(List<String> failedRenewalLeases) {
      // Remove any leases that failed to renew
      for (String lease : failedRenewalLeases) {
        LOG.debug("CredentialStore '{}' Vault, removing lease '{}' as expired.", csId, lease);
        leases.remove(lease);
        secrets.remove(getPath(lease));
      }
    }

    private List<String> renewLeases() {
      List<String> failedRenewalLeases = new ArrayList<>();
      for (Map.Entry<String, Long> lease : leases.entrySet()) {
        LOG.debug("CredentialStore '{}' Vault, attempting renewal for leaseId '{}'", csId, lease.getKey());
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
        LOG.debug(
            "CredentialStore '{}' Vault, renewed lease '{}' for '{}' seconds",
            csId,
            renewal.getLeaseId(),
            renewal.getLeaseDuration()
        );
        leases.put(renewal.getLeaseId(), System.currentTimeMillis() + (renewal.getLeaseDuration() * 1000));
      } catch (VaultException | RuntimeException e) {
        // We catch IllegalStateException to make sure this lease is removed because there seems to be a bug in
        // Vault that returns a 204 instead of a Secret when trying to renew STS credentials
        // SDC doesn't support STS today, so it is also unlikely for someone to hit this error.
        LOG.error("CredentialStore '{}' Vault, failed to renew lease for '{}': {}", csId, leaseId, e);
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
     * This means that as long as we simply evict the expired leases they should be
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
        LOG.debug("CredentialStore '{}' Vault, removing lease '{}' as expired", lease);
      }
    }

    /**
     * Returns the path portion of the specified leaseId
     *
     * @param leaseId a leaseId
     * @return path portion of leaseId
     */
    private String getPath(String leaseId) {
      return leaseId.substring(0, leaseId.lastIndexOf('/'));
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
  static String calculateUserId(String csId) {
    try {
      // Try to hash based on default interface
      InetAddress ip = InetAddress.getLocalHost();
      NetworkInterface netIf = NetworkInterface.getByInetAddress(ip);
      byte[] mac = netIf.getHardwareAddress();

      if (mac == null) {
        // In some cases the default interface may be a tap/tun device which has no MAC
        // instead pick the first available interface.
        Enumeration<NetworkInterface> netIfs = NetworkInterface.getNetworkInterfaces();
        while (netIfs.hasMoreElements() && mac == null) {
          netIf = netIfs.nextElement();
          mac = netIf.getHardwareAddress();
        }
      }

      if (mac == null) {
        throw new IllegalStateException("Could not find network interface with MAC address.");
      }

      Hasher hasher = HASH_FUNCTION.newHasher(6); // MAC is 6 bytes.
      return hasher.putBytes(mac).hash().toString();
    } catch (IOException e) {
      LOG.error("CredentialStore '{}' Vault, could not compute Vault user-id: '{}'", csId, e);
      throw new VaultRuntimeException("Could not compute Vault user-id: " + e.toString());
    }
  }

  /**
   * Returns the current auth token.
   * @return vault token
   */
  public String token() {
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
  public String read(String path, String key) {
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
  public String read(String path, String key, long delay) {
    if (!secrets.containsKey(path)) {
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
        // So for non-renewable secrets we'll store the path with an extra / so that we can purge them correctly.
        leaseId = path + "/";
      }
      leases.put(leaseId, System.currentTimeMillis() + (secret.getLeaseDuration() * 1000));

      secrets.put(path, secret);

      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    Map<String, Object> data = secrets.get(path).getData();
    String value = getSecretValue(data, key).orElseThrow(() -> new VaultRuntimeException("Value not found for key"));
    LOG.trace("CredentialStore '{}' Vault, retrieved value for key '{}'", csId, key);
    return value;
  }

  private Optional<String> getSecretValue(Object base, String key) {
    Object data = base;
    for (String part : mapSplitter.split(key)) {
      if (data instanceof Map) {
        data = ((Map) data).get(part);
      } else {
        throw new IllegalStateException(Utils.format("Unsupported data element type '{}'", data.getClass().getName()));
      }
    }

    if (!(data instanceof String)) {
      return Optional.empty();
    } else {
      return Optional.of(data.toString());
    }
  }

  private VaultConfiguration getConfig() {
    if (config == null) {
      throw new VaultRuntimeException("Vault has not been configured for this Data Collector.");
    }

    if (authExpirationTime != Long.MAX_VALUE && authExpirationTime - System.currentTimeMillis() <= 1000) {
      VaultClient vault = new VaultClient(config);
      Secret auth;
      try {
        if (authBackend == AuthBackend.APP_ID) {
          auth = vault.authenticate().appId(roleId, calculateUserId(csId));
        } else {
          auth = vault.authenticate().appRole(roleId, getSecretId());
        }
      } catch (VaultException e) {
        LOG.error(e.toString(), e);
        throw new VaultRuntimeException(e.toString());
      }
      long leaseDuration = auth.getAuth().getLeaseDuration();
      if (leaseDuration > 0) {
        authExpirationTime = System.currentTimeMillis() + (leaseDuration * 1000);
      } else {
        authExpirationTime = Long.MAX_VALUE;
      }

      config = VaultConfigurationBuilder.newVaultConfiguration()
          .fromVaultConfiguration(config)
          .withToken(auth.getAuth().getClientToken())
          .build();
    }
    return config;
  }

  private String getSecretId() {
    // Using a file ref in the config will ensure changes are picked up
    return configuration.get("secret.id", "");
  }

  private VaultConfiguration parseVaultConfigs(Configuration configuration) {
    leaseExpirationBuffer = configuration.get("lease.expiration.buffer.sec", 120);
    renewalInterval = configuration.get("lease.renewal.interval.sec", 60);

    if (configuration.hasName("role.id")) {
      authBackend = AuthBackend.APP_ROLE;
      roleId = configuration.get("role.id", "");

      if (configuration.get("secret.id", "").isEmpty()) {
        throw new VaultRuntimeException("secret.id must be specified when using AppRole auth.");
      }
    } else {
      authBackend = AuthBackend.APP_ID;
      roleId = configuration.get("app.id", "");
      LOG.debug(
          "CredentialStore '{}' Vault, the App ID authentication mode is deprecated, please migrate to AppRole",
          csId
      );
    }

    if (roleId.isEmpty()) {
      throw new VaultRuntimeException("role.id OR vault.app.id must be specified");
    }

    config = VaultConfigurationBuilder.newVaultConfiguration()
        .withAddress(configuration.get("addr", VaultConfigurationBuilder.DEFAULT_ADDRESS))
        .withOpenTimeout(configuration.get("open.timeout", 0))
        .withProxyOptions(
            ProxyOptionsBuilder.newProxyOptions()
            .withProxyAddress(configuration.get("proxy.address", ""))
            .withProxyPort(configuration.get("proxy.port", 8080))
            .withProxyUsername(configuration.get("proxy.username", ""))
            .withProxyPassword(configuration.get("proxy.password", ""))
            .build()
        )
        .withReadTimeout(configuration.get("read.timeout", 0))
        .withSslOptions(SslOptionsBuilder
            .newSslOptions()
            .withEnabledProtocols(
                configuration.get("  ", SslOptionsBuilder.DEFAULT_PROTOCOLS)
            )
            .withTrustStoreFile(configuration.get("ssl.truststore.file", ""))
            .withTrustStorePassword(configuration.get("ssl.truststore.password", ""))
            .withSslVerify(configuration.get("ssl.verify", true))
            .withSslTimeout(configuration.get("ssl.timeout", 0))
            .build()
        )
        .withTimeout(configuration.get("timeout", 0))
        .build();

    return config;
  }
}
