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
package com.streamsets.pipeline.lib.googlecloud;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.stage.common.AbstractGoogleConnection;
import com.streamsets.pipeline.stage.common.CredentialsProviderType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

public abstract class GoogleCloudCredentialsConfig {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudCredentialsConfig.class);
  public static final String CONF_CREDENTIALS_CREDENTIALS_PROVIDER = "conf.credentials.connection.credentialsProvider";

  public abstract CredentialsProviderType getCredentialsProvider();

  public abstract String getPath();

  public abstract String getProjectId();

  public abstract CredentialValue getCredentialsFileContent();

  /**
   * Tries to create a {@link CredentialsProvider} for the appropriate type of credentials supplied.
   *
   * @param context stage context used for creating config issues
   * @param issues list of issues to append to
   * @return Optional containing a credentials provider if it was successfully created.
   */
  public Optional<CredentialsProvider> getCredentialsProvider(Stage.Context context, List<Stage.ConfigIssue> issues) {
    CredentialsProvider provider = null;

    if (getCredentialsProvider().equals(CredentialsProviderType.DEFAULT_PROVIDER)) {
      return Optional.of(SubscriptionAdminSettings.defaultCredentialsProviderBuilder().build());
    } else if (getCredentialsProvider().equals(CredentialsProviderType.JSON_PROVIDER) ||
        getCredentialsProvider().equals(CredentialsProviderType.JSON)) {
      Credentials credentials = getCredentials(context, issues);
      provider = new FixedCredentialsProvider() {
        @Nullable
        @Override
        public Credentials getCredentials() {
          return credentials;
        }
      };
    }

    return Optional.ofNullable(provider);
  }

  /**
   * Reads a JSON credentials file for a service account from and returns any errors.
   *
   * @param issues list to append any discovered issues.
   * @return a generic credentials object
   */
  private Credentials getCredentials(Stage.Context context, List<Stage.ConfigIssue> issues) {
    Credentials credentials = null;

    try (InputStream in = getCredentialsInputStream(context, issues)) {
      if (in != null) {
        credentials = ServiceAccountCredentials.fromStream(in);
      }
    } catch (IOException | IllegalArgumentException e) {
      LOG.error(Errors.GOOGLE_02.getMessage(), e);
      issues.add(context.createConfigIssue("CREDENTIALS", CONF_CREDENTIALS_CREDENTIALS_PROVIDER, Errors.GOOGLE_02));
    }

    return credentials;
  }

  @VisibleForTesting
  InputStream getCredentialsInputStream(Stage.Context context, List<Stage.ConfigIssue> issues)
      throws FileNotFoundException {
    if (getCredentialsProvider().equals(CredentialsProviderType.JSON_PROVIDER)) {
      File credentialsFile;
      if (Paths.get(getPath()).isAbsolute()) {
        credentialsFile = new File(getPath());
      } else {
        credentialsFile = new File(context.getResourcesDirectory(), getPath());
      }

      if (!credentialsFile.exists() || !credentialsFile.isFile()) {
        LOG.error(Errors.GOOGLE_01.getMessage(), credentialsFile.getPath());
        issues.add(context.createConfigIssue("CREDENTIALS",
            CONF_CREDENTIALS_CREDENTIALS_PROVIDER,
            Errors.GOOGLE_01,
            credentialsFile.getPath()
        ));
        return null;
      }
      return new FileInputStream(credentialsFile);
    } else if (getCredentialsProvider().equals(CredentialsProviderType.JSON) &&
        !getCredentialsFileContent().get().isEmpty()) {
      return new ByteArrayInputStream(getCredentialsFileContent().get().getBytes());
    }
    return null;
  }
}
