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

import com.streamsets.pipeline.api.impl.Utils;

import java.io.Serializable;
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

  private LogRollModeConstants() {}
}

/**
 * Set of {@link RollMode} for Log4j and log files in general where the live log file is renamed upon
 * 'archival' and a new live log file is created.
 * <p/>
 * The assumption for all these roll modes is that the live file name is always the same.
 */
public enum LogRollModeFactory implements RollModeFactory {
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

  LogRollModeFactory(Class comparatorClass, String patternPrefix, String patternPostfix) {
    this.comparatorClass = comparatorClass;
    this.patternPrefix = patternPrefix;
    this.patternPostfix = patternPostfix;
  }


  @Override
  public String getTokenForPattern() {
    return "";
  }

  @Override
  public RollMode get(String fileName, String periodicPattern) {
    return new LogRollMode(patternPrefix, patternPostfix, comparatorClass, fileName);
  }

  private class LogRollMode implements RollMode {
    private final String patternPrefix;
    private final String patternPostfix;
    private final Class comparatorClass;
    private final String liveFileName;

    private LogRollMode(String patternPrefix, String patternPostfix, Class comparatorClass, String liveFileName) {
      this.patternPrefix = patternPrefix;
      this.patternPostfix = patternPostfix;
      this.comparatorClass = comparatorClass;
      this.liveFileName = liveFileName;
    }


    @Override
    public String getLiveFileName() {
      Utils.checkNotNull(liveFileName, "liveFileName");
      Utils.checkArgument(!liveFileName.isEmpty(), "liveFileName cannot be empty");
      return liveFileName;
    }

    @Override
    public boolean isFirstAcceptable(String firstFileName) {
      Utils.checkNotNull(liveFileName, "liveFileName");
      return firstFileName == null || firstFileName.isEmpty() || firstFileName.startsWith(liveFileName + ".");
    }

    @Override
    public boolean isCurrentAcceptable(String currentName) {
      return !liveFileName.equals(currentName);
    }

    @Override
    public boolean isFileRolled(LiveFile currentFile) {
      return !liveFileName.equals(currentFile.getPath().getFileName().toString());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Comparator<Path> getComparator() {
      try {
        Object obj = comparatorClass.newInstance();
        ((LiveFileNameSetter) obj).setName(liveFileName);
        return (Comparator<Path>) obj;
      } catch (Exception ex) {
        throw new RuntimeException(Utils.format("Unexpected exception: {}", ex.toString()), ex);
      }
    }

    @Override
    public String getPattern() {
      return patternPrefix + liveFileName + patternPostfix;
    }

    @Override
    public String toString() {
      return name();
    }

  }

  private interface LiveFileNameSetter {

    void setName(String name);

  }

  static class StringComparator implements Comparator<Path>, LiveFileNameSetter, Serializable {
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

  static class ReverseCounterComparator implements Comparator<Path>, LiveFileNameSetter, Serializable {
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
