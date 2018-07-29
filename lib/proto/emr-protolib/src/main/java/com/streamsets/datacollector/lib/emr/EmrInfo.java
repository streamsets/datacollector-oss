/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.lib.emr;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class EmrInfo {
  private static final String EMR_STAGE_LIB_FILE = "emr-stagelib.properties";
  private static final String EMR_VERSION_KEY = "emr.version";

  private static final String EMR_VERSION;

  static {
    try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(EMR_STAGE_LIB_FILE)) {
      if (is == null) {
        throw new RuntimeException("Could not find 'emr.properties' file in classpath");
      }
      Properties properties = new Properties();
      properties.load(is);
      EMR_VERSION = properties.getProperty(EMR_VERSION_KEY, null);
      if (EMR_VERSION == null) {
        throw new RuntimeException("'emr.version' not defined in stagelib 'emr.properties' file");
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static String getVersion() {
    return EMR_VERSION;
  }
}
