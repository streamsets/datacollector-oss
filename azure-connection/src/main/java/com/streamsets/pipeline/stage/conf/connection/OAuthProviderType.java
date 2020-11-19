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
package com.streamsets.pipeline.stage.conf.connection;

import com.streamsets.pipeline.api.Label;

public enum OAuthProviderType implements Label {
  CLIENT("OAuth with Service Principal", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"),
  SHARED_KEY("Shared Key", ""),
  ;

  private String label;
  private String OAuthTypeClass;

  OAuthProviderType(String label, String OAuthTypeClass) {
    this.label = label;
    this.OAuthTypeClass = OAuthTypeClass;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public String getOAuthTypeClass() {
    return OAuthTypeClass;
  }

}
