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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.microsoft.aad.adal4j.AuthenticationException;
import com.microsoft.azure.keyvault.KeyVaultClient;
import com.microsoft.azure.keyvault.models.CertificateBundle;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialStoreDef;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@CredentialStoreDef(label = "Azure Key Vault")
public class AzureKeyVaultCredentialStore implements CredentialStore {

  private static final Logger LOG = LoggerFactory.getLogger(AzureKeyVaultCredentialStore.class);

  @VisibleForTesting
  protected static final String DELIMITER_FOR_CACHE_KEY = "\n";

  private static final String VAULT_CLIENT_ID = "client.id";
  private static final String VAULT_CLIENT_KEY = "client.key";
  private static final String VAULT_URL = "vault.url";

  private static final String OPTION_URL = "url";
  private static final String REFRESH_OPTION = "refresh";
  private static final String RETRY_OPTION = "retry";
  private static final String CREDENTIAL_TYPE = "credentialType";
  private static final String CERTIFICATE = "certificate";

  public static final String CACHE_EXPIRATION_PROP = "cache.inactivityExpiration.millis";
  public static final long CACHE_EXPIRATION_DEFAULT = TimeUnit.MINUTES.toMillis(30);
  public static final String CREDENTIAL_REFRESH_PROP = "credential.refresh.millis";
  public static final long CREDENTIAL_REFRESH_DEFAULT = TimeUnit.SECONDS.toMillis(30);
  public static final String CREDENTIAL_RETRY_PROP = "credential.retry.millis";
  public static final long CREDENTIAL_RETRY_DEFAULT = TimeUnit.SECONDS.toMillis(15);

  private static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----";
  private static final String END_CERT = "-----END CERTIFICATE-----";

  private Context context;
  private String clientID;
  private String clientKey;
  private String vaultUrl;

  private LoadingCache<String, CredentialValue> cache;

  private long expirationMillis;
  private long credentialRefresh;
  private long credentialRetry;

  private KeyVaultClient client;

  @Override
  public List<ConfigIssue> init(Context context) {
    List<ConfigIssue> issues = new ArrayList<>();
    this.context = context;

    clientID = context.getConfig(VAULT_CLIENT_ID);
    clientKey = context.getConfig(VAULT_CLIENT_KEY);
    vaultUrl = context.getConfig(VAULT_URL);

    if (clientID == null || clientID.isEmpty()) {
      issues.add(context.createConfigIssue(Errors.KEY_VAULT_CRED_STORE_00, VAULT_CLIENT_ID));
    }
    if (clientKey == null || clientKey.isEmpty()) {
      issues.add(context.createConfigIssue(Errors.KEY_VAULT_CRED_STORE_00, VAULT_CLIENT_KEY));
    }
    if (vaultUrl == null || vaultUrl.isEmpty()) {
      issues.add(context.createConfigIssue(Errors.KEY_VAULT_CRED_STORE_00, VAULT_CLIENT_KEY));
    }

    if (issues.isEmpty()) {
      try {

        client = createClient();
        LOG.debug("Starting Key Vault credential store... URL: '{}'", vaultUrl);
      } catch (Exception e) {
        issues.add(context.createConfigIssue(Errors.KEY_VAULT_CRED_STORE_03, e.getMessage()));
        return issues;
      }

      Configuration configuration = getConfiguration();
      expirationMillis = configuration.get(CACHE_EXPIRATION_PROP, CACHE_EXPIRATION_DEFAULT);

      try {
        getAzureClient().getSecret(vaultUrl, "test-AzureKeyVaultCredentialStore");
      } catch (AuthenticationException e) {
        issues.add(context.createConfigIssue(Errors.KEY_VAULT_CRED_STORE_04, e.getMessage()));
        return issues;
      }

      cache = CacheBuilder.newBuilder()
                          .expireAfterAccess(getCacheExpirationMillis(), TimeUnit.MILLISECONDS)
                          .build(new CacheLoader<String, CredentialValue>() {
                            @Override
                            public CredentialValue load(String key) throws Exception {
                              return AzureKeyVaultCredentialStore.this.createCredentialValue(key);
                            }
                          });
      credentialRefresh = configuration.get(CREDENTIAL_REFRESH_PROP, CREDENTIAL_REFRESH_DEFAULT);
      credentialRetry = configuration.get(CREDENTIAL_RETRY_PROP, CREDENTIAL_RETRY_DEFAULT);

      LOG.debug("Store '{}' credential refresh '{}'ms, retry '{}'ms",
          context.getId(),
          getCredentialRefreshMillis(),
          getCredentialRetryMillis()
      );
    }
    return issues;
  }

  @VisibleForTesting
  protected Configuration getConfiguration() {
    return new Configuration(context);
  }

  @VisibleForTesting
  protected KeyVaultClient createClient() {
    AzureKeyVaultClientFactory azureKeyVaultClientFactory = new AzureKeyVaultClientFactoryImpl();
    return new KeyVaultClient(azureKeyVaultClientFactory.create(clientID, clientKey));
  }


  @Override
  public CredentialValue get(String group, String name, String options) throws StageException {
    try {
      return getCache().get(encode(group, name, options));
    } catch (ExecutionException ex) {
      if (ex.getCause() instanceof StageException) {
        throw (StageException) ex.getCause();
      } else {
        throw new StageException(Errors.KEY_VAULT_CRED_STORE_02, getContext().getId(), name, ex);
      }
    }
  }

  public void destroy() {
    if (cache != null) {
      cache.cleanUp();
      cache = null;
    }
    if (client != null) {
      client = null;
    }
  }

  @VisibleForTesting
  protected LoadingCache<String, CredentialValue> getCache() {
    return cache;
  }


  @VisibleForTesting
  protected CredentialValue createCredentialValue(String encodedStr) {
    return new AzureKeyVaultCredentialValue(encodedStr);
  }

  public class AzureKeyVaultCredentialValue implements CredentialValue {
    @VisibleForTesting
    protected String getGroup() {
      return group;
    }

    @VisibleForTesting
    protected String getName() {
      return name;
    }

    @VisibleForTesting
    protected Map<String, String> getOptions() {
      return options;
    }

    private final String group;
    private final String name;
    private final Map<String, String> options;
    private volatile long lastFetch;
    private volatile long currentInterval;
    private volatile String value;
    private boolean throwException;

    private long refreshMillis = getCredentialRefreshMillis();
    private long retryMillis = getCredentialRetryMillis();

    private AzureKeyVaultCredentialValue(String encodedStr) {
      String[] params = decode(encodedStr);
      group = params[0];
      name = params[1];
      options = Splitter.on(",").omitEmptyStrings().trimResults().withKeyValueSeparator("=").split(params[2]);

      LOG.debug("Get key for: '{}' and options: '{}'", name, options.toString());
      if (options.containsKey(OPTION_URL)) {
        try {
          vaultUrl = options.get(OPTION_URL);
        } catch (Exception ex) {
          LOG.warn("Store '{}' credential '{}' invalid option '{}' value '{}'",
              getContext().getId(),
              name,
              OPTION_URL,
              options.get(OPTION_URL)
          );
        }
      }
      if (options.containsKey(REFRESH_OPTION)) {
        try {
          refreshMillis = Long.parseLong(options.get(REFRESH_OPTION));
        } catch (Exception ex) {
          LOG.warn("Store '{}' credential '{}' invalid option '{}' value '{}'",
              getContext().getId(),
              name,
              REFRESH_OPTION,
              options.get(REFRESH_OPTION)
          );
        }
      }
      if (options.containsKey(RETRY_OPTION)) {
        try {
          retryMillis = Long.parseLong(options.get(RETRY_OPTION));
        } catch (Exception ex) {
          LOG.warn("Store '{}' credential '{}' invalid option '{}' value '{}'",
              getContext().getId(),
              name,
              RETRY_OPTION,
              options.get(RETRY_OPTION)
          );
        }
      }

      LOG.debug("Store '{}' credential updated refresh '{}'ms, retry '{}'ms, url '{}'",
          context.getId(),
          getCredentialRefreshMillis(),
          getCredentialRetryMillis(),
          getURL()
      );
      currentInterval = getCredentialRefreshMillis();
    }

    @VisibleForTesting
    protected long getRefreshMillis() {
      return refreshMillis;
    }

    @VisibleForTesting
    protected long getRetryMillis() {
      return retryMillis;
    }

    @Override
    public String get() throws StageException {
      long now = now();
      if (now - lastFetch > currentInterval) {
        try {
          LOG.debug("Store '{}' credential '{}' fetching value", getContext().getId(), name);
          if (options.containsKey(CREDENTIAL_TYPE) && options.get(CREDENTIAL_TYPE).equals(CERTIFICATE)) {
            CertificateBundle certificate = getAzureClient().getCertificate(vaultUrl, name);
            value = BEGIN_CERT + new String(Base64.getEncoder().encode(certificate.cer())) + END_CERT;
          } else {
            value = getAzureClient().getSecret(vaultUrl, name).value();
          }
          lastFetch = now();
          currentInterval = getRefreshMillis();
          throwException = false;
        } catch (Exception ex) {
          LOG.warn("Store '{}' credential '{}' error fetching value: {}", getContext().getId(), name, ex);
          lastFetch = now();
          currentInterval = getRetryMillis();
          throwException = true;
          throw ex;
        }
      } else if (throwException) {
        throw new StageException(Errors.KEY_VAULT_CRED_STORE_01, getContext().getId(), name, credentialRetry);
      } else {
        LOG.debug("Store '{}' credential '{}' using cached value", getContext().getId(), name);
      }
      return value;
    }

    @Override
    public String toString() {
      return "AzureKeyVaultCredentialStore{" + "name='" + name + '\'' + ", options=" + options + '}';
    }
  }

  @VisibleForTesting
  protected long getCredentialRefreshMillis() {
    return credentialRefresh;
  }

  @VisibleForTesting
  protected long getCredentialRetryMillis() {
    return credentialRetry;
  }

  @VisibleForTesting
  protected long getCacheExpirationMillis() {
    return expirationMillis;
  }

  @VisibleForTesting
  protected String getURL() {
    return vaultUrl;
  }

  public Context getContext() {
    return context;
  }

  @VisibleForTesting
  protected KeyVaultClient getAzureClient() {
    return client;
  }

  protected String encode(String group, String name, String options) {
    return group + DELIMITER_FOR_CACHE_KEY + name + DELIMITER_FOR_CACHE_KEY + options;
  }

  protected String[] decode(String str) {
    List<String> splits = Splitter.on(DELIMITER_FOR_CACHE_KEY).splitToList(str);
    Preconditions.checkArgument(splits.size() == 3,
        Utils.format("Store '{}' invalid value '{}'", getContext().getId(), str)
    );
    return splits.toArray(new String[3]);
  }

  @VisibleForTesting
  protected long now() {
    return System.currentTimeMillis();
  }
}
