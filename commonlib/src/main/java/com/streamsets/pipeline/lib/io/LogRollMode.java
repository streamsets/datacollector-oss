/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.api.impl.Utils;

import java.nio.file.Path;
import java.util.Comparator;

// trick to be able to use 'private' constants in the enum
class LogRollModeConstants {
  private static final String YYYY_REGEX = "\\.[0-9]{4}";
  static final String YYYY_WW_REGEX = YYYY_REGEX + "-[0-9]{2}";
  static final String YYYY_MM_REGEX = YYYY_REGEX + "-(01|02|03|04|05|06|07|09|10|11|12)";
  static final String YYYY_MM_DD_REGEX = YYYY_MM_REGEX + "-[0-9]{2}";
  static final String YYYY_MM_DD_HH_REGEX = YYYY_MM_DD_REGEX + "-[0-9]{2}";
  static final String YYYY_MM_DD_HH_MM_REGEX = YYYY_MM_DD_HH_REGEX + "-[0-9]{2}";
}

/**
 * Set of {@link RollMode} for Log4j and log files in general where the live log file is renamed upon
 * 'archival' and a new live log file is created.
 * <p/>
 * The assumption for all these roll modes is that the live file name is always the same.
 */
public enum LogRollMode implements RollMode {
  REVERSE_COUNTER(ReverseCounterComparator.class, "regex:", "\\.[0-9]+$"),
  DATE_YYYY_MM(StringComparator.class, "regex:", LogRollModeConstants.YYYY_MM_REGEX + "$"),
  DATE_YYYY_MM_DD(StringComparator.class, "regex:", LogRollModeConstants.YYYY_MM_DD_REGEX + "$"),
  DATE_YYYY_MM_DD_HH(StringComparator.class, "regex:", LogRollModeConstants.YYYY_MM_DD_HH_REGEX + "$"),
  DATE_YYYY_MM_DD_HH_MM(StringComparator.class, "regex:", LogRollModeConstants.YYYY_MM_DD_HH_MM_REGEX + "$"),
  DATE_YYYY_WW(StringComparator.class, "regex:", LogRollModeConstants.YYYY_WW_REGEX + "$"),
  ALPHABETICAL(StringComparator.class, "glob:", ".*"),
  ;

  private final String patternPrefix;
  private final String patternPostfix;
  private final Class comparatorClass;

  LogRollMode(Class comparatorClass, String patternPrefix, String patternPostfix) {
    this.comparatorClass = comparatorClass;
    this.patternPrefix = patternPrefix;
    this.patternPostfix = patternPostfix;
  }


  @Override
  public void setPattern(String filePattern) {
    //NOP
  }

  @Override
  public String getLiveFileName(String liveFileName) {
    Utils.checkNotNull(liveFileName, "liveFileName");
    Utils.checkArgument(!liveFileName.isEmpty(), "liveFileName cannot be empty");
    return liveFileName;
  }

  @Override
  public boolean isFirstAcceptable(String liveFileName, String firstFileName) {
    Utils.checkNotNull(liveFileName, "liveFileName");
    return firstFileName == null || firstFileName.isEmpty() || firstFileName.startsWith(liveFileName + ".");
  }

  @Override
  public boolean isCurrentAcceptable(String liveFileName, String currentName) {
    return !liveFileName.equals(currentName);
  }

  @Override
  public boolean isFileRolled(LiveFile liveFile, LiveFile currentFile) {
    return !currentFile.equals(liveFile);
  }


  @Override
  @SuppressWarnings("unchecked")
  public Comparator<Path> getComparator(String liveFileName) {
    try {
      Object obj = comparatorClass.newInstance();
      ((LiveFileNameSetter)obj).setName(liveFileName);
      return (Comparator<Path>) obj;
    } catch (Exception ex) {
      throw new RuntimeException("It should not happen: " + ex.getMessage(), ex);
    }
  }

  @Override
  public String getPattern(String liveFileName) {
    return patternPrefix + liveFileName + patternPostfix;
  }

  @Override
  public String toString() {
    return "LogRollMode[" + name() + "]";
  }


  private interface LiveFileNameSetter {

    public void setName(String name);

  }

  static class StringComparator implements Comparator<Path>, LiveFileNameSetter {
    private int liveNameLength;

    @Override
    public void setName(String liveFileName) {
      this.liveNameLength = liveFileName.length() + 1;
    }

    @Override
    public int compare(Path o1, Path o2) {
      String rd1 = o1.getFileName().toString().substring(liveNameLength);
      String rd2 = o2.getFileName().toString().substring(liveNameLength);
      return rd1.compareTo(rd2);
    }
  }

  static class ReverseCounterComparator implements Comparator<Path>, LiveFileNameSetter {
    private int liveNameLength;

    @Override
    public void setName(String liveFileName) {
      this.liveNameLength = liveFileName.length() + 1;
    }

    @Override
    public int compare(Path o1, Path o2) {
      String rd1 = o1.getFileName().toString().substring(liveNameLength);
      String rd2 = o2.getFileName().toString().substring(liveNameLength);
      return Integer.parseInt(rd2) - Integer.parseInt(rd1);
    }
  }

}
