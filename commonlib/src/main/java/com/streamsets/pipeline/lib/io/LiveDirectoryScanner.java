/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * The <code>LiveDirectoryScanner</code> scans a directory for the next file to process.
 * <p/>
 * There is a 'live' file, which is a file that is actively been written to it and there are 'rolled' files which
 * are not actively written to anymore (they are previous 'live' files).
 * <p/>
 * IMPORTANT: 'Rolled' files must have the 'live' file name as prefix.
 * <p/>
 * When asked for a file, the last processed file is passed (or <code>null</code> if none) and the scanner will
 * return the next 'rolled' file in order, if there are no more 'rolled' files it will return the 'live' file,
 * if there is not 'live' file it will return <code>null</code>.
 * <p/>
 * {@link LiveFile}s are used in order to handle the case of file renames.
 * <p/>
 * There are 3 possible orders for 'rolled' files, reverse counter, date and alphabetical. Reverse counter follows
 * Log4j <code>RollingFileAppender</code> file renaming handling. Date ordering supports 5 different precisions
 * following Log4j <code>DailyRollingFileAppender</code> file renaming handling. Alphabetical uses alphabetical
 * order of the postfix.
 */
public class LiveDirectoryScanner {

  private static final String YYYY_REGEX = "\\.[0-9]{4}";
  private static final String YYYY_MM_REGEX = YYYY_REGEX + "-(01|02|03|04|05|06|07|09|10|11|12)";
  private static final String YYYY_MM_DD_REGEX = YYYY_MM_REGEX + "-[0-9]{2}";
  private static final String YYYY_MM_DD_HH_REGEX = YYYY_MM_DD_REGEX + "-[0-9]{2}";
  private static final String YYYY_MM_DD_HH_MM_REGEX = YYYY_MM_DD_HH_REGEX + "-[0-9]{2}";
  private static final String YYYY_WW_REGEX = YYYY_REGEX + "-[0-9]{2}";

  public enum RolledFilesMode {
    REVERSE_COUNTER(ReverseCounterComparator.class, "regex:", "\\.[0-9]+$"),
    DATE_YYYY_MM(StringComparator.class, "regex:", YYYY_MM_REGEX + "$"),
    DATE_YYYY_MM_DD(StringComparator.class, "regex:", YYYY_MM_DD_REGEX + "$"),
    DATE_YYYY_MM_DD_HH(StringComparator.class, "regex:", YYYY_MM_DD_HH_REGEX + "$"),
    DATE_YYYY_MM_DD_HH_MM(StringComparator.class, "regex:", YYYY_MM_DD_HH_MM_REGEX + "$"),
    DATE_YYYY_WW(StringComparator.class, "regex:", YYYY_WW_REGEX + "$"),
    ALPHABETICAL(StringComparator.class, "glob:", ".*"),
    ;

    private final String patternPrefix;
    private final String patternPostfix;
    private final Class comparatorClass;

    RolledFilesMode(Class comparatorClass, String patternPrefix, String patternPostfix) {
      this.comparatorClass = comparatorClass;
      this.patternPrefix = patternPrefix;
      this.patternPostfix = patternPostfix;
    }

    @SuppressWarnings("unchecked")
    private Comparator<Path> getComparator(String liveFileName) {
      try {
        Object obj = comparatorClass.newInstance();
        ((LiveFileNameSetter)obj).setName(liveFileName);
        return (Comparator<Path>) obj;
      } catch (Exception ex) {
        throw new RuntimeException("It should not happen: " + ex.getMessage(), ex);
      }
    }

    String getPattern(String liveFileName) {
      return patternPrefix + liveFileName + patternPostfix;
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

  private static final Logger LOG = LoggerFactory.getLogger(LiveDirectoryScanner.class);

  private final Path firstFile;
  private final String liveFileName;

  private final File dir;
  private final PathMatcher fileMatcher;

  private final Comparator<Path> pathComparator;

  /**
   * Creates a <code>LiveDirectoryScanner</code> instance.
   *
   * @param dirName directory to scan.
   * @param liveFileName name of 'live' file.
   * @param firstFileName first 'rolled' file to look for if [@link #scan()} is invoked with <code>null</code>.
   * @param rolledFilesMode rolled files mode to use for ordering rolled files.
   * @throws IOException thrown if the scanner could not be created due to an IO error.
   */
  public LiveDirectoryScanner(String dirName, String liveFileName, String firstFileName, RolledFilesMode rolledFilesMode)
      throws IOException {
    Utils.checkNotNull(dirName, "dirName");
    Utils.checkArgument(!dirName.isEmpty(), "dirName cannot be empty");
    Utils.checkNotNull(liveFileName, "liveFileName");
    Utils.checkArgument(!liveFileName.isEmpty(), "liveFileName cannot be empty");
    Utils.checkNotNull(rolledFilesMode, "nonLivPostfix");

    this.liveFileName = liveFileName;

    dir = new File(dirName);
    if (!dir.exists()) {
      throw new IOException(Utils.format("Directory path '{}' does not exist", dir.getAbsolutePath()));
    }
    if (!dir.isDirectory()) {
      throw new IOException(Utils.format("Directory path '{}' is not a directory", dir.getAbsolutePath()));
    }

    if (firstFileName == null || firstFileName.isEmpty()) {
      this.firstFile = null;
    } else {
      Utils.checkArgument(firstFileName.startsWith(liveFileName),
                          Utils.formatL("liveFileName '{}' should be a prefix of firstFileName '{}'",
                                        liveFileName, firstFileName));
      this.firstFile = new File(dir, firstFileName).toPath();
    }

    pathComparator = rolledFilesMode.getComparator(liveFileName);

    //TODO check if we need to escape liveFileName
    fileMatcher = FileSystems.getDefault().getPathMatcher(rolledFilesMode.getPattern(liveFileName));

  }

  private class FileFilter implements DirectoryStream.Filter<Path> {
    private final Path firstFile;
    private final boolean includeFirstFileName;
    private final Comparator<Path> comparator;

    public FileFilter(Path firstFile, boolean includeFirstFileName, Comparator<Path> comparator) {
      this.firstFile = firstFile;
      this.includeFirstFileName = includeFirstFileName;
      this.comparator = comparator;
    }

    @Override
    public boolean accept(Path entry) throws IOException {
      boolean accept = false;
      if (fileMatcher.matches(entry.getFileName())) {
        if (firstFile == null) {
          accept = true;
        } else {
          int compares = comparator.compare(entry, firstFile);
          accept = (compares == 0 && includeFirstFileName) || (compares > 0);
        }
      }
      return accept;
    }
  }

  // last == null means start from beginning

  /**
   * Scans the directory of for the next file.
   *
   * @param current the last 'rolled' file processed. Use <code>null</code> to look for the first one. The provided
   *                file cannot be the 'live' file.
   * @return the next 'rolled' file in order, if there are no more 'rolled' files it will return the 'live' file,
   * if there is not 'live' file it will return <code>null</code>.
   * @throws IOException thrown if the directory could not be scanned.
   */
  public LiveFile scan(LiveFile current) throws IOException {
    try {
      return scanInternal(current);
    } catch (NoSuchFileException ex) {
      // this could happen because there has been a file rotation/deletion after the search/filter/sort and before
      // the creation of the nen current. Lets sleep for 50ms and try again, if fails again give up.
      try {
        Thread.sleep(50);
      } catch (InterruptedException iex) {
        //NOP
      }
      return scanInternal(current);
    }
  }

  private LiveFile scanInternal(LiveFile current) throws IOException {
    Utils.checkArgument(current == null || !liveFileName.equals(current.getPath().getFileName().toString()),
                        Utils.formatL("Current file '{}' cannot be the live file", current));
    FileFilter filter ;
    if (current == null) {
      // we don't have current file,
      // let scan from the configured first file and include the first file itself if found
      filter = new FileFilter(firstFile, true, pathComparator);
    } else {
      // we do have a current file, we need to find the next file
      filter = new FileFilter(current.getPath(), false, pathComparator);
    }
    List<Path> matchingFiles = new ArrayList<>();
    try (DirectoryStream<Path> matchingFile = Files.newDirectoryStream(dir.toPath(), filter)) {
      for (Path file : matchingFile) {
        LOG.trace("Found file '{}'", file);
        matchingFiles.add(file);
      }
    }
    LOG.debug("getCurrent() scanned '{}' matching files", matchingFiles.size());
    if (matchingFiles.size() > 0) {
      // sort all matching files (they don't necessary come in order from the OS)
      // we sort them using the comparator of the NonLivePostfix
      Collections.sort(matchingFiles, pathComparator);
      // we found a non live file, create it as such
      current = new LiveFile(matchingFiles.get(0), false);
    } else {
      // we are not behind with non-live files, lets return the live file
      try {
        current = new LiveFile(new File(dir, liveFileName).toPath());
      } catch (NoSuchFileException ex) {
        // if the live file does not currently exists, return null as we cannot have a LiveFile without an iNode
        current = null;
      }
    }
    return current;
  }

}
