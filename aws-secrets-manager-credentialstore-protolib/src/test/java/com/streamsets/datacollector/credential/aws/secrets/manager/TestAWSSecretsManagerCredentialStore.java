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

import com.amazonaws.SdkClientException;
import com.amazonaws.secretsmanager.caching.SecretCache;
import com.amazonaws.secretsmanager.caching.SecretCacheConfiguration;
import com.amazonaws.services.secretsmanager.model.AWSSecretsManagerException;
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestAWSSecretsManagerCredentialStore {

  CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);

  private final static int cacheSize = 20;
  private final static long cacheTTL = 1000L;

  @Before
  public void setUp() {
    String awsAccessKey = "access-key";
    String awsSecretKey = "secret-key";
    String region = "us-west-2";
    String securityMethod = "accessKeys";
    Mockito.when(context.getConfig(AWSSecretsManagerCredentialStore.AWS_REGION_PROP)).thenReturn(region);
    Mockito.when(context.getConfig(AWSSecretsManagerCredentialStore.SECURITY_METHOD_PROP)).thenReturn(securityMethod);
    Mockito.when(context.getConfig(AWSSecretsManagerCredentialStore.AWS_ACCESS_KEY_PROP)).thenReturn(awsAccessKey);
    Mockito.when(context.getConfig(AWSSecretsManagerCredentialStore.AWS_SECRET_KEY_PROP)).thenReturn(awsSecretKey);
    Mockito.when(context.getConfig(AWSSecretsManagerCredentialStore.CACHE_MAX_SIZE_PROP)).thenReturn(
        Integer.toString(cacheSize));
    Mockito.when(context.getConfig(AWSSecretsManagerCredentialStore.CACHE_TTL_MILLIS_PROP)).thenReturn(
        Long.toString(cacheTTL));
  }

  @Test
  public void testInitMissingRegion() throws Exception {
    AWSSecretsManagerCredentialStore secretManager = new AWSSecretsManagerCredentialStore();

    Mockito.when(context.getConfig(AWSSecretsManagerCredentialStore.AWS_REGION_PROP)).thenReturn(null);
    List<CredentialStore.ConfigIssue> issues = secretManager.init(context);
    Assert.assertEquals(1, issues.size());

    for (String prop : new String[]{
        AWSSecretsManagerCredentialStore.AWS_REGION_PROP
    }) {
      Mockito.verify(context, Mockito.times(1)).createConfigIssue(Errors.AWS_SECRETS_MANAGER_CRED_STORE_00, prop);
    }
  }

  @Test
  public void testInitMissingSecurityMethod() throws Exception {
    String region = "us-west-2";
    String awsAccessKey = "access-key";
    String awsSecretKey = "secret-key";

    CredentialStore.Context defaultContext = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(defaultContext.getConfig(AWSSecretsManagerCredentialStore.AWS_REGION_PROP)).thenReturn(region);
    Mockito.when(defaultContext.getConfig(AWSSecretsManagerCredentialStore.SECURITY_METHOD_PROP)).thenReturn(null);
    Mockito.when(defaultContext.getConfig(AWSSecretsManagerCredentialStore.AWS_ACCESS_KEY_PROP)).thenReturn(awsAccessKey);
    Mockito.when(defaultContext.getConfig(AWSSecretsManagerCredentialStore.AWS_SECRET_KEY_PROP)).thenReturn(awsSecretKey);

    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerCredentialStore secretManager = createAWSSecretsManagerCredentialStore(secretCache);
    List<CredentialStore.ConfigIssue> issues = secretManager.init(defaultContext);
    Assert.assertEquals(0, issues.size());

    Mockito.verify(secretManager, Mockito.times(1)).createSecretCache(
        SecretCacheConfiguration.DEFAULT_MAX_CACHE_SIZE,
        SecretCacheConfiguration.DEFAULT_CACHE_ITEM_TTL
    );
  }

  @Test
  public void testInitMissingSecretKey() throws Exception {
    AWSSecretsManagerCredentialStore secretManager = new AWSSecretsManagerCredentialStore();

    Mockito.when(context.getConfig(AWSSecretsManagerCredentialStore.AWS_SECRET_KEY_PROP)).thenReturn(null);
    List<CredentialStore.ConfigIssue> issues = secretManager.init(context);
    Assert.assertEquals(1, issues.size());
    Mockito.verify(context, Mockito.times(1)).createConfigIssue(Errors.AWS_SECRETS_MANAGER_CRED_STORE_06);
  }

  @Test
  public void testInitMissingAccessKey() throws Exception {
    AWSSecretsManagerCredentialStore secretManager = new AWSSecretsManagerCredentialStore();

    Mockito.when(context.getConfig(AWSSecretsManagerCredentialStore.AWS_ACCESS_KEY_PROP)).thenReturn(null);
    List<CredentialStore.ConfigIssue> issues = secretManager.init(context);
    Assert.assertEquals(1, issues.size());
    Mockito.verify(context, Mockito.times(1)).createConfigIssue(Errors.AWS_SECRETS_MANAGER_CRED_STORE_06);
  }

  @Test
  public void testInitAccessKeys() throws Exception {
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerCredentialStore secretManager = createAWSSecretsManagerCredentialStore(secretCache);
    List<CredentialStore.ConfigIssue> issues = secretManager.init(context);
    Assert.assertEquals(0, issues.size());

    Mockito.verify(secretManager, Mockito.times(1)).createSecretCache(
        cacheSize,
        cacheTTL
    );
  }

  @Test
  public void testInitInstanceProfile() throws Exception {
    Mockito.when(context.getConfig(AWSSecretsManagerCredentialStore.SECURITY_METHOD_PROP)).thenReturn("instanceProfile");
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerCredentialStore secretManager = createAWSSecretsManagerCredentialStore(secretCache);
    List<CredentialStore.ConfigIssue> issues = secretManager.init(context);
    Assert.assertEquals(0, issues.size());

    Mockito.verify(secretManager, Mockito.times(1)).createSecretCache(
        cacheSize,
        cacheTTL
    );
  }

  @Test
  public void testInitDefaultConfigs() throws Exception {
    String region = "us-west-2";
    String awsAccessKey = "access-key";
    String awsSecretKey = "secret-key";
    String securityMethod = "accessKeys";

    CredentialStore.Context defaultContext = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(defaultContext.getConfig(AWSSecretsManagerCredentialStore.AWS_REGION_PROP)).thenReturn(region);
    Mockito.when(defaultContext.getConfig(AWSSecretsManagerCredentialStore.SECURITY_METHOD_PROP)).thenReturn(securityMethod);
    Mockito.when(defaultContext.getConfig(AWSSecretsManagerCredentialStore.AWS_ACCESS_KEY_PROP)).thenReturn(awsAccessKey);
    Mockito.when(defaultContext.getConfig(AWSSecretsManagerCredentialStore.AWS_SECRET_KEY_PROP)).thenReturn(awsSecretKey);

    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerCredentialStore secretManager = createAWSSecretsManagerCredentialStore(secretCache);
    List<CredentialStore.ConfigIssue> issues = secretManager.init(defaultContext);
    Assert.assertEquals(0, issues.size());

    Mockito.verify(secretManager, Mockito.times(1)).createSecretCache(
        SecretCacheConfiguration.DEFAULT_MAX_CACHE_SIZE,
        SecretCacheConfiguration.DEFAULT_CACHE_ITEM_TTL
    );
  }

  @Test
  public void testInitIncorrectRegion() throws Exception {
    Mockito.when(context.getConfig(AWSSecretsManagerCredentialStore.AWS_REGION_PROP)).thenReturn("us-west-20");

    SecretCache secretCache = Mockito.mock(SecretCache.class);
    SdkClientException exception = new SdkClientException("message");
    AWSSecretsManagerCredentialStore secretManager = createAWSSecretsManagerCredentialStore(secretCache, exception);
    List<CredentialStore.ConfigIssue> issues = secretManager.init(context);
    Assert.assertEquals(1, issues.size());
    Mockito.verify(context, Mockito.times(1)).createConfigIssue(
        Errors.AWS_SECRETS_MANAGER_CRED_STORE_01,
        exception.getMessage(),
        exception
    );
  }

  @Test
  public void testInitIncorrectAWSKeys() throws Exception {
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerException exception = new AWSSecretsManagerException("message");
    exception.setErrorCode("IncompleteSignature");
    AWSSecretsManagerCredentialStore secretManager = createAWSSecretsManagerCredentialStore(secretCache, exception);
    List<CredentialStore.ConfigIssue> issues = secretManager.init(context);
    Assert.assertEquals(1, issues.size());
    Mockito.verify(context, Mockito.times(1)).createConfigIssue(
        Errors.AWS_SECRETS_MANAGER_CRED_STORE_01,
        exception.getMessage(),
        exception
    );
  }

  @Test
  public void testInitAccessDeniedException() throws Exception {
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerException exception = new AWSSecretsManagerException("message");
    exception.setErrorCode("AccessDeniedException");
    AWSSecretsManagerCredentialStore secretManager = createAWSSecretsManagerCredentialStore(secretCache, exception);
    List<CredentialStore.ConfigIssue> issues = secretManager.init(context);
    Assert.assertEquals(0, issues.size());  // AccessDeniedException should be ignored at initialization.
  }

  @Test
  public void testInitResourceNotFoundException() throws Exception {
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerException exception = new ResourceNotFoundException("message");
    exception.setErrorCode("ResourceNotFoundException");
    AWSSecretsManagerCredentialStore secretManager = createAWSSecretsManagerCredentialStore(secretCache, exception);
    List<CredentialStore.ConfigIssue> issues = secretManager.init(context);
    Assert.assertEquals(0, issues.size());  // ResourceNotFoundException should be ignored at initialization.
  }

  @Test
  public void testGetCredentialMissingSeparator() throws Exception {
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerCredentialStore secretManager = setupNominalAWSSecretsManagerCredentialStore(secretCache);

    try {
      secretManager.get("", "a", null);
      Assert.fail("Expected a StageException");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testGetCredentialNotFound() throws Exception {
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerCredentialStore secretManager = setupNominalAWSSecretsManagerCredentialStore(secretCache);

    String credName = "credName";
    String credKey = "credKey";
    Mockito.when(secretCache.getSecretString(credName)).thenReturn(null);
    try {
      secretManager.get("", credName + "&" + credKey, null);
      Assert.fail("Expected a StageException");
    } catch (StageException e) {
      Assert.assertEquals(Errors.AWS_SECRETS_MANAGER_CRED_STORE_02, e.getErrorCode());
    }
  }

  @Test
  public void testGetCredentialNotFound2() throws Exception {
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerCredentialStore secretManager = setupNominalAWSSecretsManagerCredentialStore(secretCache);

    String credName = "credName";
    String credKey = "credKey";
    Mockito.when(secretCache.getSecretString(credName)).thenThrow(new ResourceNotFoundException(""));
    try {
      secretManager.get("", credName + "&" + credKey, null);
      Assert.fail("Expected a StageException");
    } catch (StageException e) {
      Assert.assertEquals(Errors.AWS_SECRETS_MANAGER_CRED_STORE_03, e.getErrorCode());
    }
  }

  @Test
  public void testGetKeyNotFound() throws Exception {
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerCredentialStore secretManager = setupNominalAWSSecretsManagerCredentialStore(secretCache);

    String credName = "credName";
    String credKey = "credKey";
    Mockito.when(secretCache.getSecretString(credName)).thenReturn(createJSONString("foo", "bar"));
    try {
      secretManager.get("", credName + "&" + credKey, null);
      Assert.fail("Expected a StageException");
    } catch (StageException e) {
      Assert.assertEquals(Errors.AWS_SECRETS_MANAGER_CRED_STORE_04, e.getErrorCode());
    }
  }

  @Test
  public void testGet() throws Exception {
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerCredentialStore secretManager = setupNominalAWSSecretsManagerCredentialStore(secretCache);

    String credName = "credName";
    String credKey = "credKey";
    String credValue = "credValue";
    Mockito.when(secretCache.getSecretString(credName)).thenReturn(createJSONString(credKey, credValue));
    CredentialValue credentialValue = secretManager.get("", credName + "&" + credKey, null);
    Assert.assertEquals(credValue, credentialValue.get());
    Mockito.verify(secretCache, Mockito.times(0)).refreshNow(credName);
  }

  @Test
  public void testGetOtherSeparator() throws Exception {
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerCredentialStore secretManager = setupNominalAWSSecretsManagerCredentialStore(secretCache);

    String credName = "credName";
    String credKey = "credKey";
    String credValue = "credValue";
    Mockito.when(secretCache.getSecretString(credName)).thenReturn(createJSONString(credKey, credValue));
    CredentialValue credentialValue = secretManager.get(
        "",
        credName + "|" + credKey,
        AWSSecretsManagerCredentialStore.SEPARATOR_OPTION + "=|"
    );
    Assert.assertEquals(credValue, credentialValue.get());
    Mockito.verify(secretCache, Mockito.times(0)).refreshNow(credName);
  }

  @Test
  public void testGetOtherSeparatorWithNonDefault() throws Exception {
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerCredentialStore secretManager = setupNominalAWSSecretsManagerCredentialStore(secretCache, "|");

    String credName = "credName";
    String credKey = "credKey";
    String credValue = "credValue";
    Mockito.when(secretCache.getSecretString(credName)).thenReturn(createJSONString(credKey, credValue));
    CredentialValue credentialValue = secretManager.get(
        "",
        credName + "|" + credKey,
        null
    );
    Assert.assertEquals(credValue, credentialValue.get());
    Mockito.verify(secretCache, Mockito.times(0)).refreshNow(credName);

    credentialValue = secretManager.get(
        "",
        credName + "]" + credKey,
        AWSSecretsManagerCredentialStore.SEPARATOR_OPTION + "=]"
    );
    Assert.assertEquals(credValue, credentialValue.get());
    Mockito.verify(secretCache, Mockito.times(0)).refreshNow(credName);
  }

  @Test
  public void testGetAlwaysRefresh() throws Exception {
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerCredentialStore secretManager = setupNominalAWSSecretsManagerCredentialStore(secretCache);

    String credName = "credName";
    String credKey = "credKey";
    String credValue = "credValue";
    Mockito.when(secretCache.getSecretString(credName)).thenReturn(createJSONString(credKey, credValue));
    Mockito.verify(secretCache, Mockito.times(0)).refreshNow(credName);
    CredentialValue credentialValue = secretManager.get(
        "",
        credName + "&" + credKey,
        AWSSecretsManagerCredentialStore.ALWAYS_REFRESH_OPTION + "=true"
    );
    Mockito.verify(secretCache, Mockito.times(1)).refreshNow(credName);
    Assert.assertEquals(credValue, credentialValue.get());
    Mockito.verify(secretCache, Mockito.times(2)).refreshNow(credName);
  }

  @Test
  public void testDestroy() throws Exception {
    SecretCache secretCache = Mockito.mock(SecretCache.class);
    AWSSecretsManagerCredentialStore secretManager = setupNominalAWSSecretsManagerCredentialStore(secretCache);

    Mockito.verify(secretCache, Mockito.times(0)).close();
    secretManager.destroy();
    Mockito.verify(secretCache, Mockito.times(1)).close();
  }

  private AWSSecretsManagerCredentialStore createAWSSecretsManagerCredentialStore(SecretCache secretCache) {
    return createAWSSecretsManagerCredentialStore(secretCache, new ResourceNotFoundException(""));
  }

  private AWSSecretsManagerCredentialStore createAWSSecretsManagerCredentialStore(
      SecretCache secretCache,
      Exception verifyException
  ) {
    AWSSecretsManagerCredentialStore credentialStore = Mockito.spy(new AWSSecretsManagerCredentialStore());
    Mockito.when(credentialStore.getRegion()).thenReturn("us-west-2");
    Mockito.when(credentialStore.createSecretCache(
        Mockito.anyInt(),
        Mockito.anyLong()
    )).thenReturn(secretCache);
    Mockito.when(secretCache.getSecretString("test-AWSSecretsManagerCredentialStore")).thenThrow(verifyException);
    return credentialStore;
  }

  private AWSSecretsManagerCredentialStore setupNominalAWSSecretsManagerCredentialStore(SecretCache secretCache) {
    return setupNominalAWSSecretsManagerCredentialStore(secretCache, null);
  }

  private AWSSecretsManagerCredentialStore setupNominalAWSSecretsManagerCredentialStore(
      SecretCache secretCache, String nameKeySeparator
  ) {
    if (nameKeySeparator != null) {
      Mockito.when(context.getConfig(AWSSecretsManagerCredentialStore.NAME_KEY_SEPARATOR_PROP)).thenReturn(
          nameKeySeparator);
    }

    AWSSecretsManagerCredentialStore secretManager = createAWSSecretsManagerCredentialStore(secretCache);
    List<CredentialStore.ConfigIssue> issues = secretManager.init(context);
    Assert.assertEquals(0, issues.size());

    return secretManager;
  }

  private String createJSONString(String... keyValuePairs) throws IOException {
    Assert.assertEquals("Must have an even number of key-value pairs", 0, keyValuePairs.length % 2);
    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < keyValuePairs.length; i++) {
      map.put(keyValuePairs[i], keyValuePairs[i+1]);
      i++;
    }
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(map);
  }
}
