/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;

public interface RollMode {

  public void setPattern(String filePattern);

  public String getLiveFileName(String liveFileName);

  public boolean isFirstAcceptable(String liveFileName, String firstFileName);

  public boolean isCurrentAcceptable(String liveFileName, String currentName);

  public boolean isFileRolled(LiveFile liveFile, LiveFile currentFile) throws IOException;

  public Comparator<Path> getComparator(String liveFileName);

  //must be a PathMatcher glob: or regex: pattern
  public String getPattern(String liveFile);

}
