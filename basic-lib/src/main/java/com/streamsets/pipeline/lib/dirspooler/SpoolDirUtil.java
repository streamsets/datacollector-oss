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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;

public class SpoolDirUtil {
  private SpoolDirUtil() {}
  private final static Logger LOG = LoggerFactory.getLogger(SpoolDirUtil.class);

  /**
   * True if f1 is "newer" than f2.
   */
  public static boolean compareFiles(File f1, File f2) {
    if (!Files.exists(f2.toPath())) {
      return true;
    }

    long mtime1 = f1.lastModified();
    long mtime2 = f2.lastModified();

    try {
      long ctime1 = ((FileTime) Files.getAttribute(f1.toPath(), "unix:ctime")).toMillis();
      long ctime2 = ((FileTime) Files.getAttribute(f2.toPath(), "unix:ctime")).toMillis();

      long time1 = Math.max(mtime1, ctime1);
      long time2 = Math.max(mtime2, ctime2);

      int compares = Long.compare(time1, time2);

      if (compares != 0) {
        return compares > 0;
      }
    } catch (IOException ex) {
      LOG.error("Failed to get ctime: '{}'", f1.getName(), ex);
      return false;
    }

    return f1.getName().compareTo(f2.getName()) > 0;
  }
}