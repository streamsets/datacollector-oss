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
package com.streamsets.pipeline;

import java.io.IOException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class ClassLoaderUtil {

  static final String SERVICES_PREFIX = "/META-INF/services/";

  private static final String SERVICES_PREFIX_CANONICALIZED = canonicalizeClass(SERVICES_PREFIX);
  private static final String[] EMPTY_STRING_ARRAY = new String[0];

  private ClassLoaderUtil() {}

  static String canonicalizeClass(String name) {
    String canonicalName = name.replace('/', '.');
    while (canonicalName.startsWith(".")) {
      canonicalName = canonicalName.substring(1);
    }
    return canonicalName;
  }

  static String canonicalizeClassOrResource(String name) {
    String canonicalName = canonicalizeClass(name);
    if (canonicalName.startsWith(SERVICES_PREFIX_CANONICALIZED)) {
      canonicalName = canonicalName.substring(SERVICES_PREFIX_CANONICALIZED.length());
    }
    return canonicalName;
  }

  static <T> T checkNotNull(T value, String name) {
    if (value == null) {
      throw new NullPointerException("Value " + name + " is null");
    }
    return value;
  }

  static String[] getTrimmedStrings(String str){
    if (null == str) {
      return EMPTY_STRING_ARRAY;
    }
    if (str.isEmpty()) {
      return EMPTY_STRING_ARRAY;
    }
    return str.trim().split("\\s*,\\s*");
  }

  public static URL[] getJarUrlsInDirectory(final String directory) {
    final Path dirPath = Paths.get(directory);
    if (!dirPath.toFile().exists()) {
      throw new IllegalArgumentException(String.format("Path %s does not exist", directory));
    } else if (!dirPath.toFile().isDirectory()) {
      throw new IllegalArgumentException(String.format("Path %s is not a directory", directory));
    } else {
      final List<URL> jarURLs = new LinkedList<>();
      try (final DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath,
          path -> path.toFile().isFile() && path.toString().toLowerCase().endsWith(".jar")
      )) {
        for (Path jar : stream) {
          // can't use lambda because of MalformedURLException being thrown here
          jarURLs.add(jar.toUri().toURL());
        }
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format(
            "IOException attempting to traverse directory %s for jar files: %s",
            directory,
            e.getMessage()
        ), e);
      }
      return jarURLs.toArray(new URL[jarURLs.size()]);
    }
  }

}
