/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.pipeline.lib.kafka.connection;

import com.streamsets.pipeline.api.Label;

public enum SaslMechanisms implements Label {
  GSSAPI("GSSAPI (Kerberos)", "GSSAPI"),
  PLAIN("PLAIN", "PLAIN"),
  ;

  private final String label;
  private final String mechanism;

  SaslMechanisms(String label, String mechanism) {
    this.label = label;
    this.mechanism = mechanism;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public String getMechanism() {
    return mechanism;
  }

  public boolean isOneOf(SaslMechanisms... mechanisms) {
    if (mechanisms == null) {
      return false;
    }
    for (SaslMechanisms m : mechanisms) {
      if (this == m) {
        return true;
      }
    }
    return false;
  }
}
