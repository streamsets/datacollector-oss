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
package com.streamsets.pipeline.stage.cloudstorage.lib;

public final class GcsUtil {
  public static final String PATH_DELIMITER = "/";

  private GcsUtil() {
  }

  public static String normalizePrefix(String prefix) {
    if (prefix != null) {
      // if prefix starts with delimiter, remove it
      if (prefix.startsWith(PATH_DELIMITER)) {
        prefix = prefix.substring(PATH_DELIMITER.length());
      }
      // if prefix does not end with delimiter, add one
      if (!prefix.isEmpty() && !prefix.endsWith(PATH_DELIMITER)) {
        prefix = prefix + PATH_DELIMITER;
      }
    }
    return prefix;
  }
}
