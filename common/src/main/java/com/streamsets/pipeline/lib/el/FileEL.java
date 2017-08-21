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
package com.streamsets.pipeline.lib.el;

import com.google.common.collect.Iterables;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import org.apache.commons.io.FilenameUtils;

import java.nio.file.Path;
import java.nio.file.Paths;

public class FileEL {

  private FileEL() {
    // Instantiation is prohibited
  }

  @ElFunction(
      prefix = "file",
      name = "fileName",
      description = "Returns just file name from given path."
  )
  public static String fileName (
    @ElParam("filePath") String filePath
  ) {
    if(isEmpty(filePath)) {
      return null;
    }
    return FilenameUtils.getName(filePath);
  }

  @ElFunction(
      prefix = "file",
      name = "parentPath",
      description = "Returns parent path to given file or directory. Returns path without separator (e.g. result will not end with slash)."
  )
  public static String parentPath (
    @ElParam("filePath") String filePath
  ) {
    if(isEmpty(filePath)) {
      return null;
    }
    return FilenameUtils.getFullPathNoEndSeparator(filePath);
  }

  @ElFunction(
      prefix = "file",
      name = "fileExtension",
      description = "Returns file extension from given path (e.g. 'txt' from /path/file.txt)."
  )
  public static String fileExtension (
    @ElParam("filePath") String filePath
  ) {
    if(isEmpty(filePath)) {
      return null;
    }

    return FilenameUtils.getExtension(filePath);
  }

  @ElFunction(
      prefix = "file",
      name = "removeExtension",
      description = "Returns path without the extension (e.g. '/path/file' from /path/file.txt)."
  )
  public static String removeExtension (
    @ElParam("filePath") String filePath
  ) {
    if(isEmpty(filePath)) {
      return null;
    }

    return FilenameUtils.removeExtension(filePath);
  }

  @ElFunction(
      prefix = "file",
      name = "pathElement",
      description = "Returns element (directory or file name) from given index. On path /path/to/file.txt, 'path' have index 0, 'to' have index 1 and file.txt have index 2."
  )
  public static String pathElement (
    @ElParam("filePath") String filePath,
    @ElParam("index") int index
  ) {
    if(isEmpty(filePath)) {
      return null;
    }

    // Array of elements of the path
    Path[] elements = Iterables.toArray(Paths.get(filePath), Path.class);

    // Accept negative index to count from the end of array
    if(index < 0) {
      // Using + as the number is negative, so the resulting operation is actually subtraction
      index = elements.length + index;
    }

    // Index have to be in range, otherwise return null (which will be very likely converted to empty string by EL engine)
    if(index < 0 || index >= elements.length) {
      return null;
    }

    return elements[index].toString();
  }

  private static boolean isEmpty(String str) {
    return str == null || str.isEmpty();
  }

}
