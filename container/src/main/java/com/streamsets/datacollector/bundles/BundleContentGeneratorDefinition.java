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
package com.streamsets.datacollector.bundles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal representation of the bundle content creator.
 */
public class BundleContentGeneratorDefinition {
  private static final Logger LOG = LoggerFactory.getLogger(BundleContentGeneratorDefinition.class);

  private Class<? extends BundleContentGenerator> klass;
  private String name;
  private String id;
  private String description;
  private int version;
  private boolean enabledByDefault;
  private int order;

  public BundleContentGeneratorDefinition(
    Class<? extends BundleContentGenerator> klass,
    String name,
    String id,
    String description,
    int version,
    boolean enabledByDefault,
    int order
  ) {
    this.klass = klass;
    this.name = name;
    this.id = id;
    this.description = description;
    this.version = version;
    this.enabledByDefault = enabledByDefault;
    this.order = order;
  }

  public Class<? extends BundleContentGenerator> getKlass() {
    return klass;
  }

  public String getName() {
    return name;
  }

  public String getId() {
    return id;
  }

  public String getDescription() {
    return description;
  }

  public int getVersion() {
    return version;
  }

  public boolean isEnabledByDefault() {
    return enabledByDefault;
  }

  public int getOrder() {
    return order;
  }

  public BundleContentGenerator createInstance() {
    try {
      return getKlass().newInstance();
    } catch (Exception ex) {
      LOG.warn("Could not create instance for generator '{}', error: {}", getKlass().getName(), ex.toString(), ex);
      return (context, writer) -> {};
    }
  }

}
