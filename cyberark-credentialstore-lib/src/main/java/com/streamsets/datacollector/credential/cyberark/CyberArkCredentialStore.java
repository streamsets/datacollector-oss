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
package com.streamsets.datacollector.credential.cyberark;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialStoreDef;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Credential store backed by CyberArk Application Identity Manager.
 */
@CredentialStoreDef(label = "CyberArk KeyStore")
public class CyberArkCredentialStore implements CredentialStore {
  private static final Logger LOG = LoggerFactory.getLogger(CyberArkCredentialStore.class);

  public static final String CACHE_EXPIRATION_PROP = "cache.inactivityExpiration.millis";
  public static final long CACHE_EXPIRATION_DEFAULT = TimeUnit.MINUTES.toMillis(30);

  public static final String CYBERARK_CONNECTOR_PROP = "connector";
  public static final String CYBERARK_CONNECTOR_LOCAL = "local";
  public static final String CYBERARK_CONNECTOR_WEB_SERVICES = "webservices";
  public static final String CYBERARK_CONNECTOR_DEFAULT = CYBERARK_CONNECTOR_WEB_SERVICES;

  public static final String CREDENTIAL_REFRESH_PROP = "credential.refresh.millis";
  public static final long CREDENTIAL_REFRESH_DEFAULT = TimeUnit.SECONDS.toMillis(30);

  public static final String CREDENTIAL_RETRY_PROP = "credential.retry.millis";
  public static final long CREDENTIAL_RETRY_DEFAULT = TimeUnit.SECONDS.toMillis(15);
  public static final String REFRESH_OPTION = "refresh";
  public static final String RETRY_OPTION = "retry";

  private Context context;
  private Fetcher fetcher;
  private LoadingCache<String, CredentialValue> cache;
  private long expirationMillis;
  private long credentialRefresh;
  private long credentialRetry;

  public List<ConfigIssue> init(Context context) {
    this.context = context;
    List<ConfigIssue> issues = new ArrayList<>();
    Configuration configuration = new Configuration(context);
    expirationMillis = configuration.get(CACHE_EXPIRATION_PROP, CACHE_EXPIRATION_DEFAULT);
    try {
      fetcher = createFetcher(configuration);
      LOG.debug("Using '{}'", fetcher.getClass().getSimpleName());
      fetcher.init(configuration);
    } catch (Exception ex) {
      issues.add(context.createConfigIssue(Errors.CYBERARCK_000, context.getId(), ex));
    }
    if (issues.isEmpty()) {
      cache = CacheBuilder.newBuilder()
                          .expireAfterAccess(getCacheExpirationMillis(), TimeUnit.MILLISECONDS)
                          .build(new CacheLoader<String, CredentialValue>() {
                            @Override
                            public CredentialValue load(String key) throws Exception {
                              return CyberArkCredentialStore.this.createCredentialValue(key);
                            }
                          });
      credentialRefresh = configuration.get(CREDENTIAL_REFRESH_PROP, CREDENTIAL_REFRESH_DEFAULT);
      credentialRetry = configuration.get(CREDENTIAL_RETRY_PROP, CREDENTIAL_RETRY_DEFAULT);
      LOG.debug("Store '{}' credential refresh '{}'ms, retry '{}'ms", context.getId(), getCredentialRefreshMillis(), getCredentialRetryMillis());
    }
    return issues;
  }

  @VisibleForTesting
  protected Fetcher createFetcher(Configuration configuration) {
    Fetcher fetcher;
    String fetcherType = configuration.get(CYBERARK_CONNECTOR_PROP, CYBERARK_CONNECTOR_DEFAULT);
    switch (fetcherType) {
      case CYBERARK_CONNECTOR_WEB_SERVICES:
        fetcher = new WebServicesFetcher();
        break;
      case CYBERARK_CONNECTOR_LOCAL:
        fetcher = new LocalFetcher();
        break;
      default:
        throw new RuntimeException(Utils.format("Store '{}' invalid fetcher type '{}'", getContext().getId(), fetcherType));
    }
    return fetcher;
  }

  public CredentialValue get(String group, String name, String options) throws StageException {
    try {
      return getCache().get(encode(group, name, options));
    } catch (ExecutionException ex) {
      if (ex.getCause() instanceof StageException) {
        throw (StageException) ex.getCause();
      } else {
        throw new StageException(Errors.CYBERARCK_001, getContext().getId(), name, ex);
      }
    }
  }

  public void destroy() {
    if (cache != null) {
      cache.cleanUp();
      cache = null;
    }
    if (fetcher != null) {
      fetcher.destroy();
      fetcher = null;
    }
  }

  @VisibleForTesting
  protected LoadingCache<String, CredentialValue> getCache() {
    return cache;
  }

  @VisibleForTesting
  protected  Fetcher getFetcher() {
    return fetcher;
  }

  @VisibleForTesting
  protected long getCacheExpirationMillis() {
    return expirationMillis;
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
  protected static final String DELIMITER_FOR_CACHE_KEY = "\n";

  protected String encode(String group, String name, String options) {
    return group + DELIMITER_FOR_CACHE_KEY + name + DELIMITER_FOR_CACHE_KEY + options;
  }

  protected String[] decode(String str) {
    List<String> splits = Splitter.on(DELIMITER_FOR_CACHE_KEY).splitToList(str);
    Preconditions.checkArgument(splits.size() == 3, Utils.format("Store '{}' invalid value '{}'", getContext().getId(), str));
    return splits.toArray(new String[3]);
  }

  @VisibleForTesting
  protected CredentialValue createCredentialValue(String encodedStr) {
    return new CyberArkCredentialValue(encodedStr);
  }

  public Context getContext() {
    return context;
  }

  class CyberArkCredentialValue implements CredentialValue {
    private final String group;
    private final String name;
    private final Map<String, String> options;
    private volatile long lastFetch;
    private volatile long currentInterval;
    private volatile String value;
    private long refreshMillis = getCredentialRefreshMillis();
    private long retryMillis = getCredentialRetryMillis();
    private boolean throwException;

    private CyberArkCredentialValue(String encodedStr) {
      String[] params = decode(encodedStr);
      group = params[0];
      name = params[1];
      options = Splitter.on(",").omitEmptyStrings().trimResults().withKeyValueSeparator("=").split(params[2]);
      if (options.containsKey(REFRESH_OPTION)) {
        try {
          refreshMillis = Long.parseLong(options.get(REFRESH_OPTION));
        } catch (Exception ex) {
          LOG.warn("Store '{}' credential '{}' invalid option '{}' value '{}'", getContext().getId(),name, REFRESH_OPTION, options.get(REFRESH_OPTION));
        }
      }
      if (options.containsKey(RETRY_OPTION)) {
        try {
          retryMillis = Long.parseLong(options.get(RETRY_OPTION));
        } catch (Exception ex) {
          LOG.warn("Store '{}' credential '{}' invalid option '{}' value '{}'", getContext().getId(), name, RETRY_OPTION, options.get(RETRY_OPTION));
        }
      }
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
          value = getFetcher().fetch(group, name, options);
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
        throw new StageException(Errors.CYBERARCK_002, getContext().getId(), name, credentialRetry);
      } else {
        LOG.debug("Store '{}' credential '{}' using cached value", getContext().getId(), name);
      }
      return value;
    }

    @Override
    public String toString() {
      return "CyberArkCredentialValue{" + "name='" + name + '\'' + ", options=" + options + '}';
    }
  }

  @VisibleForTesting
  protected long now() {
    return System.currentTimeMillis();
  }


}
