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
package com.streamsets.datacollector.credential.cyberark;

import com.streamsets.pipeline.api.credential.CredentialStore;

class Configuration {
  private final CredentialStore.Context context;

  public Configuration(CredentialStore.Context context) {
    this.context = context;
  }


  public String getId() {
    return context.getId();
  }

  public boolean hasName(String name) {
    return context.getConfig(name) != null;
  }

  public String get(String name, String defaultValue) {
    String value = context.getConfig(name);
    return (value != null) ? value : defaultValue;
  }

  public long get(String name, long defaultValue) {
    String value = context.getConfig(name);
    return (value != null) ? Long.parseLong(value) : defaultValue;
  }

  public int get(String name, int defaultValue) {
    String value = context.getConfig(name);
    return (value != null) ? Integer.parseInt(value) : defaultValue;
  }

  public boolean get(String name, boolean defaultValue) {
    String value = context.getConfig(name);
    return (value != null) ? Boolean.parseBoolean(value) : defaultValue;
  }

}
