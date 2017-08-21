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
package com.streamsets.pipeline.stage.destination.hdfs.util;

  import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Various utilities that are reusable across all stages dealing with HDFS.
 */
public class HdfsUtils {

  /**
   * Parse String representation of permissions into HDFS FsPermission class.
   *
   * This method accepts the following formats:
   *  * Octal like '777' or '770'
   *  * HDFS style changes like 'a-rwx'
   *  * Unix style write up with 9 characters like 'rwxrwx---'
   *
   * @param permissions String representing the permissions
   * @return Parsed FsPermission object
   */
  public static FsPermission parseFsPermission(String permissions) throws IllegalArgumentException {
    try {
      // Octal or symbolic representation
      return new FsPermission(permissions);
    } catch (IllegalArgumentException e) {
      // FsPermission.valueOf will work with unix style permissions which is 10 characters
      // where the first character says the type of file
      if (permissions.length() == 9) {
        // This means it is a posix standard without the first character for file type
        // We will simply set it to '-' suggesting regular file
        permissions = "-" + permissions;
      }

      // Try to parse unix style format.
      return FsPermission.valueOf(permissions);
    }
  }
}

