/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.api.impl.Utils;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Comparator;

public class PeriodicFilesRollMode implements RollMode {
  private String regexPattern;
  private PathMatcher fileMatcher;

  @Override
  public void setPattern(String filePattern) {
    this.regexPattern = "regex:" + Utils.checkNotNull(filePattern, "regexPattern");
    fileMatcher = FileSystems.getDefault().getPathMatcher(this.regexPattern);
  }

  @Override
  public boolean isFirstAcceptable(String liveFileName, String firstFileName) {
    return firstFileName == null || firstFileName.isEmpty() || fileMatcher.matches(Paths.get(firstFileName));
  }

  @Override
  public String getLiveFileName(String liveFileName) {
    return null;
  }

  @Override
  public boolean isCurrentAcceptable(String liveFileName, String currentName) {
    return fileMatcher.matches(Paths.get(currentName));
  }

  private class FileFilter implements DirectoryStream.Filter<Path> {
    private final String currentFileName;

    public FileFilter(Path currentFile) {
      this.currentFileName = currentFile.getFileName().toString();
    }

    @Override
    public boolean accept(Path entry) throws IOException {
      boolean accept = false;
      if (fileMatcher.matches(entry.getFileName())) {
        accept = entry.getFileName().toString().compareTo(currentFileName) > 0;
      }
      return accept;
    }
  }

  @Override
  public boolean isFileRolled(LiveFile liveFile, LiveFile currentFile) throws IOException {
    DirectoryStream.Filter<Path> filter = new FileFilter(currentFile.getPath());
    try (DirectoryStream<Path> matches = Files.newDirectoryStream(currentFile.getPath().getParent(), filter)) {
      return matches.iterator().hasNext();
    }
  }

  @Override
  public Comparator<Path> getComparator(String liveFileName) {
    return new Comparator<Path>() {
      @Override
      public int compare(Path o1, Path o2) {
        return o1.compareTo(o2);
      }
    };
  }

  @Override
  public String getPattern(String liveFile) {
    return regexPattern;
  }

}
