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

package com.streamsets.datacollector.credential.aws.secrets.manager;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.secretsmanager.caching.SecretCache;
import com.amazonaws.secretsmanager.caching.SecretCacheConfiguration;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.AWSSecretsManagerException;
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialStoreDef;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@CredentialStoreDef(label = "AWS Secrets Manager")
public class AWSSecretsManagerCredentialStore implements CredentialStore {

  private static final Logger LOG = LoggerFactory.getLogger(AWSSecretsManagerCredentialStore.class);

  public static final String NAME_KEY_SEPARATOR_PROP = "nameKey.separator";
  public static final String NAME_KEY_SEPARTOR_DEFAULT = "&";
  public static final String SEPARATOR_OPTION = "separator";
  public static final String AWS_REGION_PROP = "region";
  public static final String AWS_ACCESS_KEY_PROP = "access.key";
  public static final String AWS_SECRET_KEY_PROP = "secret.key";
  public static final String CACHE_MAX_SIZE_PROP = "cache.max.size";
  public static final String CACHE_TTL_MILLIS_PROP = "cache.ttl.millis";
  public static final String ALWAYS_REFRESH_OPTION = "alwaysRefresh";

  private SecretCache secretCache;
  private String nameKeySeparator;

  @Override
  public List<ConfigIssue> init(Context context) {
    List<ConfigIssue> issues = new ArrayList<>();

    nameKeySeparator = context.getConfig(NAME_KEY_SEPARATOR_PROP);
    if (nameKeySeparator == null) {
      nameKeySeparator = NAME_KEY_SEPARTOR_DEFAULT;
    }

    String region = context.getConfig(AWS_REGION_PROP);
    if (region == null || region.isEmpty()) {
      issues.add(context.createConfigIssue(Errors.AWS_SECRETS_MANAGER_CRED_STORE_00, AWS_REGION_PROP));
    }

    String accessKey = context.getConfig(AWS_ACCESS_KEY_PROP);
    String secretKey = context.getConfig(AWS_SECRET_KEY_PROP);

    String cacheSizeStr = context.getConfig(CACHE_MAX_SIZE_PROP);
    int cacheSize = (cacheSizeStr != null)
        ? Integer.parseInt(cacheSizeStr)
        : SecretCacheConfiguration.DEFAULT_MAX_CACHE_SIZE;

    String cacheTTLStr = context.getConfig(CACHE_TTL_MILLIS_PROP);
    long cacheTTL = (cacheTTLStr != null)
        ? Integer.parseInt(cacheTTLStr)
        : SecretCacheConfiguration.DEFAULT_CACHE_ITEM_TTL;

    if (issues.isEmpty()) {
      LOG.debug("Creating Secret Cache for region '{}'", region);
      secretCache = createSecretCache(accessKey, secretKey, region, cacheSize, cacheTTL);
      validateCredentialStoreConnection(context, issues);
    }

    return issues;
  }

  public static AWSCredentialsProvider getCredentialsProvider(String accessKey, String secretKey) throws StageException {
    if (!StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey)) {
      return new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
    } else {
      return new DefaultAWSCredentialsProviderChain();
    }
  }

  protected SecretCache createSecretCache(
      String awsAccessKey,
      String awsSecretKey,
      String region,
      int cacheSize,
      long cacheTTL
  ) {
    AWSCredentialsProvider credentials = getCredentialsProvider(awsAccessKey, awsSecretKey);
    AWSSecretsManagerClientBuilder clientBuilder = AWSSecretsManagerClientBuilder
        .standard()
        .withRegion(region)
        .withCredentials(credentials);

    SecretCacheConfiguration cacheConf = new SecretCacheConfiguration()
        .withMaxCacheSize(cacheSize)
        .withCacheItemTTL(cacheTTL)
        .withClient(clientBuilder.build());

    return new SecretCache(cacheConf);
  }

  @Override
  public CredentialValue get(String group, String name, String options) throws StageException {
    Utils.checkNotNull(group, "group cannot be NULL");
    Utils.checkNotNull(name, "name cannot be NULL");
    if (options != null) {
      LOG.debug("Get name-key for: '{}' and options: '{}'", name, options);
    } else {
      LOG.debug("Get name-key for: '{}' and no options", name);
    }

    Map<String, String> optionsMap = options != null ? Splitter.on(",")
        .omitEmptyStrings()
        .trimResults()
        .withKeyValueSeparator("=")
        .split(options) : Collections.emptyMap();

    String separator = optionsMap.get(SEPARATOR_OPTION);
    if (separator == null) {
      separator = nameKeySeparator;
    }

    String[] splits = name.split(Pattern.quote(separator), 2);
    if (splits.length != 2) {
      throw new IllegalArgumentException(
          Utils.format("AWSSecretsManagerCredentialStore name '{}' should be '<name>{}<key>'",
              name,
              separator
      ));
    }

    String alwaysRefreshStr = optionsMap.get(ALWAYS_REFRESH_OPTION);
    boolean alwaysRefresh = (alwaysRefreshStr != null && alwaysRefreshStr.equals("true"));

    CredentialValue credential = new AWSSecretsManagerCredentialValue(splits[0], splits[1], alwaysRefresh);
    credential.get();
    return credential;
  }

  @Override
  public void destroy() {
    if (secretCache != null) {
      secretCache.close();
    }
  }

  /**
   * Verify connectivity to AWS Secrets Manager credential store and appends any issue found to the issues list.
   */
  private void validateCredentialStoreConnection(Context context, List<ConfigIssue> issues) {
    try {
      // The only way to verify the connection is just to try to perform an operation on a dummy, nonexistent secret
      // and expect a ResourceNotFoundException or an AccessDeniedException. The former is expected
      // when reading permissions (i.e. GetSecretValue/DescribeSecret actions) are allowed for any resource;
      // otherwise, the latter is expected.
      secretCache.getSecretString("test-AWSSecretsManagerCredentialStore");
    } catch (ResourceNotFoundException ex) {
      // Ignore.
    } catch (AWSSecretsManagerException ex) {
      // Ignore only for AccessDeniedException (there is no specific class, checking ErrorCode).
      if (!ex.getErrorCode().equals("AccessDeniedException")) {
        LOG.error(Errors.AWS_SECRETS_MANAGER_CRED_STORE_01.getMessage(), ex.getMessage(), ex);
        issues.add(context.createConfigIssue(Errors.AWS_SECRETS_MANAGER_CRED_STORE_01, ex.getMessage(), ex));
      }
    } catch (Exception ex) {
      LOG.error(Errors.AWS_SECRETS_MANAGER_CRED_STORE_01.getMessage(), ex.getMessage(), ex);
      issues.add(context.createConfigIssue(Errors.AWS_SECRETS_MANAGER_CRED_STORE_01, ex.getMessage(), ex));
    }
  }

  private static String parseJSONAndGetValue(String json, String key) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> map = mapper.readValue(json, Map.class);
    Object value = map.get(key);
    if (value == null) {
      return null;
    }
    return value.toString();
  }

  public class AWSSecretsManagerCredentialValue implements CredentialValue {
    private String name;
    private String key;
    private boolean alwaysRefresh;

    private AWSSecretsManagerCredentialValue(String name, String key, boolean alwaysRefresh) {
      this.name = name;
      this.key = key;
      this.alwaysRefresh = alwaysRefresh;
      LOG.debug(
          "Created AWSSecretsManagerCredentialValue with name '{}', key '{}', and alwaysRefresh '{}'",
          name,
          key,
          alwaysRefresh
      );
    }

    @Override
    public String get() throws StageException {
      if (alwaysRefresh) {
        try {
          LOG.trace("Force refreshing '{}'", name);
          secretCache.refreshNow(name);
        } catch (InterruptedException ie) {
          LOG.warn("Encountered InterruptedException while refreshing credential '{}'", name, ie);
        }
      }
      try {
        String json = secretCache.getSecretString(name);
        if (json == null) {
          throw new StageException(Errors.AWS_SECRETS_MANAGER_CRED_STORE_02, name);
        }
        try {
          String value = parseJSONAndGetValue(json, key);
          if (value == null) {
            throw new StageException(Errors.AWS_SECRETS_MANAGER_CRED_STORE_04, key, name);
          }
          return value;
        } catch (IOException ioe) {
          throw new StageException(Errors.AWS_SECRETS_MANAGER_CRED_STORE_04, key, name, ioe);
        }
      } catch (AWSSecretsManagerException ex) {
        throw new StageException(Errors.AWS_SECRETS_MANAGER_CRED_STORE_03, name, ex);
      }
    }

    @Override
    public String toString() {
      return "AWSSecretsManagerCredentialValue{name='" + name + "', key='" + key + "', alwaysRefresh="
          + alwaysRefresh + "'}";
    }
  }
}
