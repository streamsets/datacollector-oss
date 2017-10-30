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
package com.streamsets.pipeline.lib.dirspooler;

import java.io.File;

public class SpoolDirUtil {
  private SpoolDirUtil() {}

  /**
   * True if f1 is "newer" than f2.
   */
  public static boolean compareFiles(File f1, File f2) {
    return f1.lastModified() > f2.lastModified() ||
        (f1.lastModified() == f2.lastModified() && f1.getName().compareTo(f2.getName()) > 0);
  }
}
