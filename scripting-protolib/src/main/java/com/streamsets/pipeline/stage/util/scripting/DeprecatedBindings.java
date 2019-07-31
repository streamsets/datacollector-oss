/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.util.scripting;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;

import javax.script.SimpleBindings;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A DeprecatedBindings is like a SimpleBindings which can also contain deprecated bindings
 * which will send a warning to the log when used for the first time.
 */
public class DeprecatedBindings extends SimpleBindings {

  private final Logger log;
  private final ConcurrentHashMap<String, String> warningsToThrow;

  public static ImmutableMap<String, String> allDeprecatedMappings = ImmutableMap.<String, String>builder()
      .put("error", "sdc.error")
      .put("log", "sdc.log")
      .put("state", "sdc.state")
      .put("sdcFunctions", "sdc")
      .put("NULL_BOOLEAN", "sdc.NULL_BOOLEAN")
      .put("NULL_CHAR", "sdc.NULL_CHAR")
      .put("NULL_BYTE", "sdc.NULL_BYTE")
      .put("NULL_SHORT", "sdc.NULL_SHORT")
      .put("NULL_INTEGER", "sdc.NULL_INTEGER")
      .put("NULL_LONG", "sdc.NULL_LONG")
      .put("NULL_FLOAT", "sdc.NULL_FLOAT")
      .put("NULL_DOUBLE", "sdc.NULL_DOUBLE")
      .put("NULL_DATE", "sdc.NULL_DATE")
      .put("NULL_DATETIME", "sdc.NULL_DATETIME")
      .put("NULL_TIME", "sdc.NULL_DATETIME")
      .put("NULL_DECIMAL", "sdc.NULL_DECIMAL")
      .put("NULL_BYTE_ARRAY", "sdc.NULL_BYTE_ARRAY")
      .put("NULL_STRING", "sdc.NULL_STRING")
      .put("NULL_LIST", "sdc.NULL_LIST")
      .put("NULL_MAP", "sdc.NULL_MAP")
      .build();

  /**
   * @param log - where to send the warning when a deprecated binding is used
   * @param warningsToThrow - ConcurrentMap to ensure that we only throw one
   *                           warning per deprecated binding until pipeline is restarted
   */
  public DeprecatedBindings(Logger log, ConcurrentHashMap<String, String> warningsToThrow) {
    super();
    this.log = log;
    this.warningsToThrow = warningsToThrow;
  }

  @Override
  public Object get(Object key) {
    if (!(key instanceof String)) {
      return null;
    }
    String oldName = (String) key;
    String newName = warningsToThrow.remove(oldName);
    // if we encounter a key that has a warning to throw, we throw it
    if (newName != null) {
      log.warn(
          String.format("SCRIPT WARNING: The name '%s' is deprecated. Use '%s' instead. "
                  + "This binding will be removed in a future release.",
              oldName,
              newName
          ));
    }
    return super.get(key);
  }
}