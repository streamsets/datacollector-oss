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
package com.streamsets.pipeline.lib.io;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;

public class WildcardFilter implements DirectoryStream.Filter<Path> {
  private PathMatcher fileMatcher;

  private WildcardFilter(String pattern) {
    fileMatcher = FileSystems.getDefault().getPathMatcher(pattern);
  }

  public static DirectoryStream.Filter<Path> createGlob(String glob) {
    return new WildcardFilter("glob:" + glob);
  }

  public static DirectoryStream.Filter<Path> createRegex(String regex) {
    return new WildcardFilter("regex:" + regex);
  }

  @Override
  public boolean accept(Path entry) throws IOException {
    return fileMatcher.matches(entry.getFileName());
  }

}
