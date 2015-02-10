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

  public WildcardFilter(String globPattern) {
    fileMatcher = FileSystems.getDefault().getPathMatcher("glob:" + globPattern);
  }

  @Override
  public boolean accept(Path entry) throws IOException {
    return fileMatcher.matches(entry.getFileName());
  }

}
