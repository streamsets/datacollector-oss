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
package com.streamsets.pipeline.stage.origin.opcua;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;


@GenerateResourceBundle
public enum SecurityPolicyValues implements Label {
  NONE("None", SecurityPolicy.None),
  BASIC_128_RSA_15("Basic128Rsa15", SecurityPolicy.Basic128Rsa15),
  BASIC_256("Basic256", SecurityPolicy.Basic256),
  BASIC_256_SHA_256("Basic256Sha256", SecurityPolicy.Basic256Sha256),
  ;

  private final String label;
  private final SecurityPolicy securityPolicy;

  SecurityPolicyValues(String label, SecurityPolicy securityPolicy) {
    this.label = label;
    this.securityPolicy = securityPolicy;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public SecurityPolicy getSecurityPolicy() {
    return securityPolicy;
  }
}
