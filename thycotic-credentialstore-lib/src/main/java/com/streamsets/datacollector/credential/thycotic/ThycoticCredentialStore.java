/*
 * Copyright 2019 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.datacollector.credential.thycotic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.datacollector.credential.thycotic.api.AuthRenewalTask;
import com.streamsets.datacollector.credential.thycotic.api.GetThycoticSecrets;
import com.streamsets.datacollector.thycotic.SslOptionsBuilder;
import com.streamsets.datacollector.thycotic.SslVerification;
import com.streamsets.datacollector.thycotic.ThycoticConfiguration;
import com.streamsets.datacollector.thycotic.ThycoticConfigurationBuilder;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialStoreDef;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;

/**
 * Credential store backed by Thycotic Secret Server
 */
@CredentialStoreDef(label = "Thycotic Secret Server")
public class ThycoticCredentialStore implements CredentialStore {
  private static final Logger LOG = LoggerFactory.getLogger(ThycoticCredentialStore.class);

  @VisibleForTesting
  private static final String GRANT_TYPE = "password";
  protected static final String DELIMITER_FOR_CACHE_KEY = "\n";
  public static final String FIELD_KEY_SEPARATOR_PROP = "nameSeparator";
  public static final String FIELD_KEY_SEPARATOR_DEFAULT = "-";

  public static final String CACHE_EXPIRATION_PROP = "cache.inactivityExpiration.seconds";
  public static final long CACHE_EXPIRATION_DEFAULT = TimeUnit.SECONDS.toSeconds(1800);

  public static final String CREDENTIAL_REFRESH_PROP = "credential.refresh.seconds";
  public static final long CREDENTIAL_REFRESH_DEFAULT = TimeUnit.SECONDS.toSeconds(30);

  public static final String CREDENTIAL_RETRY_PROP = "credential.retry.seconds";
  public static final long CREDENTIAL_RETRY_DEFAULT = TimeUnit.SECONDS.toSeconds(15);

  public static final String THYCOTIC_SECRET_SERVER_URL = "url";
  public static final String THYCOTIC_SECRET_SERVER_USERNAME = "username";
  public static final String THYCOTIC_SECRET_SERVER_PASSWORD = "password";

  public static final String REFRESH_OPTION = "refresh";
  public static final String RETRY_OPTION = "retry";

  private static final String OPEN_TIMEOUT = "open.timeout";
  private static final String READ_TIMEOUT = "read.timeout";
  private static final String SSL_ENABLED_PROTOCOLS = "ssl.enabled.protocols";
  private static final String SSL_TRUSTSTORE_FILE = "ssl.truststore.file";
  private static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
  private static final String SSL_VERIFY = "ssl.verify";
  private static final String SSL_TIMEOUT = "ssl.timeout";
  private static final String TIMEOUT = "timeout";

  private ThycoticConfiguration config;
  private CloseableHttpClient httpclient;
  private GetThycoticSecrets secret;
  private AuthRenewalTask authRenewal;
  private Configuration configuration;
  private Context context;

  private String secretServerUrl;
  private String username;
  private String password;
  private Integer secretId;
  private String secretField;

  private LoadingCache<String, CredentialValue> credentialCache;

  private long credentialRefresh;
  private long credentialRetry;
  private long cacheExpirationSeconds;

  public List<ConfigIssue> init(Context context) {
    this.context = context;
    List<ConfigIssue> issues = new ArrayList<ConfigIssue>();
    secret = new GetThycoticSecrets();
    httpclient = HttpClients.createDefault();
    configuration = getConfiguration();
    config = getConfig(configuration);
    new SslVerification(config);

    secretServerUrl = context.getConfig(THYCOTIC_SECRET_SERVER_URL);
    username = context.getConfig(THYCOTIC_SECRET_SERVER_USERNAME);
    password = context.getConfig(THYCOTIC_SECRET_SERVER_PASSWORD);

    if (secretServerUrl == null || secretServerUrl.isEmpty()) {
      issues.add(context.createConfigIssue(Errors.THYCOTIC_00, THYCOTIC_SECRET_SERVER_URL));
    }
    if (username == null || username.isEmpty()) {
      issues.add(context.createConfigIssue(Errors.THYCOTIC_00, THYCOTIC_SECRET_SERVER_USERNAME));
    }
    if (password == null || password.isEmpty()) {
      issues.add(context.createConfigIssue(Errors.THYCOTIC_00, THYCOTIC_SECRET_SERVER_PASSWORD));
    }

    if (!checkSecretServerConnection()) {
      issues.add(context.createConfigIssue(Errors.THYCOTIC_02, THYCOTIC_SECRET_SERVER_URL));
    }

    if (issues.isEmpty()) {
      authRenewal = new AuthRenewalTask(getClient(), secretServerUrl, username, password);

      cacheExpirationSeconds = configuration.get(CACHE_EXPIRATION_PROP, CACHE_EXPIRATION_DEFAULT);

      credentialCache = CacheBuilder.newBuilder()
          .expireAfterAccess(getCacheExpirationSeconds(), TimeUnit.SECONDS)
          .build(new CacheLoader<String, CredentialValue>() {
            @Override
            public CredentialValue load(String key) throws Exception {
              return ThycoticCredentialStore.this.createCredentialValue(key);
            }
          });

      credentialRefresh = configuration.get(CREDENTIAL_REFRESH_PROP, CREDENTIAL_REFRESH_DEFAULT);
      credentialRetry = configuration.get(CREDENTIAL_RETRY_PROP, CREDENTIAL_RETRY_DEFAULT);

      LOG.debug("Store '{}' credential refresh '{}'ms, retry '{}'ms",
          context.getId(),
          getCredentialRefreshSeconds(),
          getCredentialRetrySeconds()
      );

    }
    return issues;
  }

  @VisibleForTesting
  protected boolean checkSecretServerConnection() {
    HttpPost httpPost = new HttpPost(secretServerUrl + "/oauth2/token");
    List<NameValuePair> nvps = new ArrayList<NameValuePair>();
    nvps.add(new BasicNameValuePair("username", username));
    nvps.add(new BasicNameValuePair("password", password));
    nvps.add(new BasicNameValuePair("grant_type", GRANT_TYPE));
    try {
      httpPost.setEntity(new UrlEncodedFormEntity(nvps));
      CloseableHttpResponse response = getClient().execute(httpPost);
      if (response.getStatusLine().getStatusCode() == 200) {
        return true;
      }
    } catch (IOException e) {
      LOG.debug("Error in connecting to the secret server: ", e);
    }
    return false;
  }

  @VisibleForTesting
  protected Configuration getConfiguration() {
    return new Configuration(context);
  }

  protected ThycoticConfiguration getConfig(Configuration configuration) {
    return ThycoticConfigurationBuilder.newThycoticConfiguration()
        .withAddress(configuration.get(THYCOTIC_SECRET_SERVER_URL,ThycoticConfigurationBuilder.DEFAULT_ADDRESS))
        .withOpenTimeout(configuration.get(OPEN_TIMEOUT, 0))
        .withReadTimeout(configuration.get(READ_TIMEOUT, 0))
        .withSslOptions((SslOptionsBuilder.newSslOptions()
            .withEnabledProtocols(configuration.get(SSL_ENABLED_PROTOCOLS, SslOptionsBuilder.DEFAULT_PROTOCOLS))
            .withTrustStoreFile(configuration.get(SSL_TRUSTSTORE_FILE, ""))
            .withTrustStorePassword(configuration.get(SSL_TRUSTSTORE_PASSWORD, ""))
            .withSslVerify(configuration.get(SSL_VERIFY, true))
            .withSslTimeout(configuration.get(SSL_TIMEOUT, 0)).build()))
        .withTimeout(configuration.get(TIMEOUT, 0)).build();
  }

  public CredentialValue get(String group, String name, String credentialStoreOptions) throws StageException {
    try {
      return getCredentialCache().get(encode(group, name, credentialStoreOptions));
    } catch (ExecutionException ex) {
      if (ex.getCause() instanceof StageException) {
        throw (StageException) ex.getCause();
      } else {
        throw new StageException(Errors.THYCOTIC_01, getContext().getId(), name, ex);
      }
    }
  }

  public void destroy() {
    if (credentialCache != null) {
      credentialCache.cleanUp();
      credentialCache = null;
    }
  }

  @VisibleForTesting
  protected GetThycoticSecrets getSecret() {
    return secret;
  }

  @VisibleForTesting
  protected AuthRenewalTask getAuth() {
    return authRenewal;
  }

  @VisibleForTesting
  protected CloseableHttpClient getClient() {
    return httpclient;
  }

  @VisibleForTesting
  protected LoadingCache<String, CredentialValue> getCredentialCache() {
    return credentialCache;
  }

  @VisibleForTesting
  protected long getCredentialRefreshSeconds() {
    return credentialRefresh;
  }

  @VisibleForTesting
  protected long getCredentialRetrySeconds() {
    return credentialRetry;
  }

  @VisibleForTesting
  protected long getCacheExpirationSeconds() {
    return cacheExpirationSeconds;
  }

  @VisibleForTesting
  protected CredentialValue createCredentialValue(String encodedStr) {
    return new ThycoticCredentialValue(encodedStr);
  }

  protected String encode(String group, String name, String options) {
    return Joiner.on(DELIMITER_FOR_CACHE_KEY).join(group, name, options);
  }

  public class ThycoticCredentialValue implements CredentialValue {

    private final String group;
    private final String name;
    private final Map<String, String> options;
    private volatile String secret;
    private volatile long lastFetch;
    private volatile long currentInterval;
    private long refreshSeconds = getCredentialRefreshSeconds();
    private long retrySeconds = getCredentialRetrySeconds();

    private ThycoticCredentialValue(String encodedStr) {
      String[] params = decode(encodedStr);
      group = params[0];
      name = params[1];

      String[] splits = name.split(configuration.get(FIELD_KEY_SEPARATOR_PROP, FIELD_KEY_SEPARATOR_DEFAULT));
      Preconditions.checkArgument(splits.length == 2,
          Utils.format("Store '{}' invalid value '{}'", getContext().getId(), name)
      );
      secretId = Integer.valueOf(splits[0]);
      secretField = splits[1];

      options = Splitter.on(",").omitEmptyStrings().trimResults().withKeyValueSeparator("=").split(params[2]);

      if (options.containsKey(REFRESH_OPTION)) {
        try {
          refreshSeconds = Long.parseLong(options.get(REFRESH_OPTION));
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
          retrySeconds = Long.parseLong(options.get(RETRY_OPTION));
        } catch (Exception ex) {
          LOG.warn("Store '{}' credential '{}' invalid option '{}' value '{}'",
              getContext().getId(),
              name,
              RETRY_OPTION,
              options.get(RETRY_OPTION)
          );
        }
      }
    }

    @VisibleForTesting
    protected long getRefreshSeconds() {
      return refreshSeconds;
    }

    @VisibleForTesting
    protected long getRetrySeconds() {
      return retrySeconds;
    }

    public String get() throws StageException {
      long now = now();
      if (now - lastFetch > currentInterval) {
        LOG.debug("Store '{}' credential '{}' fetching value", getContext().getId(), name);
        // In case we have trouble getting the value from the server, keep re-using the previously acquired value
        String updatedSecret = getSecret().getSecretField(getClient(),
            getAuth().getAccessToken(),
            secretServerUrl,
            secretId,
            secretField,
            group
        );
        if (updatedSecret != null) {
          secret = updatedSecret;
          currentInterval = getRefreshSeconds() * 1000;
        } else if (secret == null) {
          // There's no previously acquired value to fall back on - probably a more permanent issue, so let's give up
          throw new StageException(Errors.THYCOTIC_01, name, credentialRetry);
        } else {
          LOG.debug(
              "Store '{}' credential '{}' using cached value due to previous exception",
              getContext().getId(),
              name
          );
          currentInterval = getRetrySeconds() * 1000;
        }
        lastFetch = now();
      } else {
        LOG.debug("Store '{}' credential '{}' using cached value", getContext().getId(), name);
      }
      return secret;
    }

    @Override
    public String toString() {
      return "ThycoticCredentialValue{" + "name='" + name + '\'' + ", options=" + options + '}';
    }
  }

  public Context getContext() {
    return context;
  }

  protected String[] decode(String str) {
    List<String> splits = Splitter.on(DELIMITER_FOR_CACHE_KEY).splitToList(str);
    Preconditions.checkArgument(splits.size() == 3, Utils.format("Store '{}' invalid value '{}'", str));
    return splits.toArray(new String[3]);
  }

  @VisibleForTesting
  protected long now() {
    return System.currentTimeMillis();
  }
}
