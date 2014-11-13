/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.container.LocaleInContext;
import com.streamsets.pipeline.container.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class Message {
  private static final String DEFAULT_BUNDLE = "pipeline-container-bundle";
  private static final Object[] NULL_ONE_ARG = { null};
  private static final Logger LOG = LoggerFactory.getLogger(Message.class);

  private final ClassLoader classLoader;
  private final String bundleName;
  private final String bundleKey;
  private final String defaultTemplate;
  private final Object[] args;

  public Message(ClassLoader classLoader, String bundleName, String bundleKey, String defaultTemplate, Object... args) {
    this.classLoader = Preconditions.checkNotNull(classLoader, "classLoader cannot be null");
    this.bundleName = Preconditions.checkNotNull(bundleName, "bundleName cannot be null");
    this.bundleKey = Preconditions.checkNotNull(bundleKey, "bundleKey cannot be null");
    this.defaultTemplate = Preconditions.checkNotNull(defaultTemplate, "defaultTemplate cannot be null");
    // we need to do this trick because of the ... syntax sugar resolution when null is used
    this.args = (args != null) ? args : NULL_ONE_ARG;
  }

  public Message(String bundleKey, String defaultTemplate, Object... args) {
    this(Message.class.getClassLoader(), DEFAULT_BUNDLE, bundleKey, defaultTemplate, args);
  }

  public String getDefaultMessage() {
    return Utils.format(defaultTemplate, args);
  }

  @JsonValue
  public String getMessage() {
    String template = defaultTemplate;
    Locale locale = LocaleInContext.get();
    if (locale != null) {
      try {
        ResourceBundle rb = ResourceBundle.getBundle(bundleName, locale, classLoader);
        if (rb.containsKey(bundleKey)) {
          template = rb.getString(bundleKey);
        } else {
          LOG.warn("ResourceBundle '{}' does not have key '{}'", bundleName, bundleKey);
        }
      } catch (MissingResourceException ex) {
        LOG.warn("ResourceBundle '{}' not found via ClassLoader '{}'", bundleName, classLoader);
      }
    }
    return Utils.format(template, args);
  }

  public String toString() {
    return Utils.format("Message[bundleName='{}' bundleKey='{}' defaultTemplate='{}']: {}", bundleName, bundleKey,
                        defaultTemplate, getDefaultMessage());
  }

}
