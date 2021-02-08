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
package com.streamsets.pipeline.lib.kafka.connection;

import com.streamsets.pipeline.api.Label;

public enum KafkaSecurityOptions implements Label {
  PLAINTEXT("None (Security Protocol=PLAINTEXT)", "PLAINTEXT"),
  SSL("SSL/TLS Encryption (Security Protocol=SSL)", "SSL"),
  SSL_AUTH("SSL/TLS Encryption and Authentication (Security Protocol=SSL)", "SSL"),
  SASL_PLAINTEXT("SASL Authentication (Security Protocol=SASL_PLAINTEXT)", "SASL_PLAINTEXT"),
  SASL_SSL("SASL Authentication on SSL/TLS (Security Protocol=SASL_SSL)", "SASL_SSL"),
  ;

  private final String label;
  private final String protocol;

  KafkaSecurityOptions(String label, String protocol) {
    this.label = label;
    this.protocol = protocol;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public String getProtocol() {
    return protocol;
  }

  public boolean isOneOf(KafkaSecurityOptions... options) {
    if (options == null) {
      return false;
    }
    for (KafkaSecurityOptions op : options) {
      if (this == op) {
        return true;
      }
    }
    return false;
  }
}
