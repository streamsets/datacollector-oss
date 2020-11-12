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
import com.google.common.base.Splitter;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialStoreDef;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@CredentialStoreDef(label = "AWS Secrets Manager")
public class AWSSecretsManagerCredentialStore implements CredentialStore {

  private static final Logger LOG = LoggerFactory.getLogger(AWSSecretsManagerCredentialStore.class);

  public static final String NAME_KEY_SEPARATOR_PROP = "nameKey.separator";
  public static final String NAME_KEY_SEPARATOR_DEFAULT = "&";
  public static final String SEPARATOR_OPTION = "separator";
  public static final String AWS_REGION_PROP = "region";
  public static final String SECURITY_METHOD_PROP = "security.method";
  public static final String AWS_ACCESS_KEY_PROP = "access.key";
  public static final String AWS_SECRET_KEY_PROP = "secret.key";
  public static final String CACHE_MAX_SIZE_PROP = "cache.max.size";
  public static final String CACHE_TTL_MILLIS_PROP = "cache.ttl.millis";
  public static final String ALWAYS_REFRESH_OPTION = "alwaysRefresh";

  public static final String AUTH_METHOD_INSTANCE_PROFILE = "instanceProfile";
  public static final String AUTH_METHOD_ACCESS_KEYS = "accessKeys";

  private SecretCache secretCache;
  private String nameKeySeparator;

  private String region;
  private String accessKey;
  private String secretKey;
  private String securityMethod;

  @Override
  public List<ConfigIssue> init(Context context) {
    List<ConfigIssue> issues = new ArrayList<>();

    nameKeySeparator = context.getConfig(NAME_KEY_SEPARATOR_PROP);
    if (nameKeySeparator == null) {
      nameKeySeparator = NAME_KEY_SEPARATOR_DEFAULT;
    }

    region = context.getConfig(AWS_REGION_PROP);
    if (region == null || region.isEmpty()) {
      issues.add(context.createConfigIssue(Errors.AWS_SECRETS_MANAGER_CRED_STORE_00, AWS_REGION_PROP));
    }


    String cacheSizeStr = context.getConfig(CACHE_MAX_SIZE_PROP);
    int cacheSize = cacheSizeStr != null
                    ? Integer.parseInt(cacheSizeStr)
                    : SecretCacheConfiguration.DEFAULT_MAX_CACHE_SIZE;

    String cacheTTLStr = context.getConfig(CACHE_TTL_MILLIS_PROP);
    long cacheTTL = cacheTTLStr != null
                    ? Integer.parseInt(cacheTTLStr)
                    : SecretCacheConfiguration.DEFAULT_CACHE_ITEM_TTL;

    securityMethod = context.getConfig(SECURITY_METHOD_PROP);
    accessKey = context.getConfig(AWS_ACCESS_KEY_PROP);
    secretKey = context.getConfig(AWS_SECRET_KEY_PROP);

    if (StringUtils.isEmpty(securityMethod)) {
      //For backwards compatibility, if the user does not set the property is defaulted to access keys
      securityMethod = (!StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey))
                       ? AUTH_METHOD_ACCESS_KEYS
                       : AUTH_METHOD_INSTANCE_PROFILE;

      LOG.error(
          "Missing security.method is not set, it will be mandatory in future release, defaulting to {}",
          securityMethod
      );
    }

    if (securityMethod.equals(AUTH_METHOD_ACCESS_KEYS)) {
      if (StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(secretKey)) {
        issues.add(context.createConfigIssue(Errors.AWS_SECRETS_MANAGER_CRED_STORE_06));
      }
    } else if (!securityMethod.equals(AUTH_METHOD_INSTANCE_PROFILE)) {
      issues.add(context.createConfigIssue(Errors.AWS_SECRETS_MANAGER_CRED_STORE_05));
    }

    if (issues.isEmpty()) {
      LOG.debug("Creating Secret Cache for region '{}'", region);
      secretCache = createSecretCache(cacheSize, cacheTTL);
      validateCredentialStoreConnection(context, issues);
    }

    return issues;
  }

  public AWSCredentialsProvider getCredentialsProvider() {
    AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();

    if (AUTH_METHOD_ACCESS_KEYS.equals(securityMethod)) {
      credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
    }

    return credentialsProvider;
  }

  protected SecretCache createSecretCache(int cacheSize, long cacheTTL) {
    AWSCredentialsProvider credentials = getCredentialsProvider();
    AWSSecretsManagerClientBuilder clientBuilder = AWSSecretsManagerClientBuilder.standard()
                                                                                 .withRegion(getRegion())
                                                                                 .withCredentials(credentials);

    SecretCacheConfiguration cacheConf = new SecretCacheConfiguration().withMaxCacheSize(cacheSize).withCacheItemTTL(
        cacheTTL).withClient(clientBuilder.build());

    return new SecretCache(cacheConf);
  }

  String getRegion() {
    return region;
  }

  @Override
  public CredentialValue get(String group, String name, String options) {
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
      throw new IllegalArgumentException(Utils.format("AWSSecretsManagerCredentialStore name '{}' should be " +
              "'<name>{}<key>'",
          name,
          separator
      ));
    }

    String alwaysRefreshStr = optionsMap.get(ALWAYS_REFRESH_OPTION);
    boolean alwaysRefresh = (alwaysRefreshStr != null && alwaysRefreshStr.equals("true"));

    CredentialValue credential = new AWSSecretsManagerCredentialValue(splits[0], splits[1], alwaysRefresh, secretCache);
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
}
