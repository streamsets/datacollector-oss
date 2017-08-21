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
package com.streamsets.pipeline.lib.tls;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum KeyStoreType implements Label {
  JKS("Java Keystore file (JKS)", "JKS"),
  PKCS12("PKCS-12 (p12 file)", "PKCS12"),
  ;

  private final String label;
  private final String javaValue;

  KeyStoreType(String label, String javaValue) {
    this.label = label;
    this.javaValue = javaValue;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public String getJavaValue() {
    return javaValue;
  }
}
