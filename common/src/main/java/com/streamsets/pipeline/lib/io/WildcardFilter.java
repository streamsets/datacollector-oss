/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
