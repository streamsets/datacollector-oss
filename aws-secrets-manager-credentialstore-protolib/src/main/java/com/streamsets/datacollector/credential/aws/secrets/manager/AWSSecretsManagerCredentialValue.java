/*
 * Copyright 2020 StreamSets Inc.
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

import com.amazonaws.secretsmanager.caching.SecretCache;
import com.amazonaws.services.secretsmanager.model.AWSSecretsManagerException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class AWSSecretsManagerCredentialValue implements CredentialValue {

  private static final Logger LOG = LoggerFactory.getLogger(AWSSecretsManagerCredentialValue.class);

  private final String name;
  private final String key;
  private final boolean alwaysRefresh;
  private final SecretCache secretCache;

  AWSSecretsManagerCredentialValue(
      String name, String key, boolean alwaysRefresh, SecretCache secretCache
  ) {
    this.name = name;
    this.key = key;
    this.alwaysRefresh = alwaysRefresh;
    this.secretCache = secretCache;
    LOG.debug(
        "Created AWSSecretsManagerCredentialValue with name '{}', key '{}', and alwaysRefresh '{}'",
        name,
        key,
        alwaysRefresh
    );
  }

  @Override
  public String get() {
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
      return parseJSONAndGetValue(json, key, name);

    } catch (AWSSecretsManagerException ex) {
      throw new StageException(Errors.AWS_SECRETS_MANAGER_CRED_STORE_03, name, ex);
    }
  }

  @Override
  public String toString() {
    return "AWSSecretsManagerCredentialValue{name='".concat(name).concat("', key='").concat(key).concat("', " +
        "alwaysRefresh=").concat(String.valueOf(alwaysRefresh)).concat("'}");
  }

  private static String parseJSONAndGetValue(String json, String key, String name) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> map = mapper.readValue(json, Map.class);
      String value = map.get(key) == null ? null : map.get(key).toString();
      if (value == null) {
        throw new StageException(Errors.AWS_SECRETS_MANAGER_CRED_STORE_04, key, name);
      }
      return value;
    } catch (IOException ioe) {
      throw new StageException(Errors.AWS_SECRETS_MANAGER_CRED_STORE_04, key, name, ioe);
    }
  }
}