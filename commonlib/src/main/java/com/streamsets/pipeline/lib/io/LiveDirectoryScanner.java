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
import com.streamsets.pipeline.lib.util.ThreadUtil;
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

  private static final Logger LOG = LoggerFactory.getLogger(LiveDirectoryScanner.class);

  private final Path firstFile;
  private final String liveFileName;

  private final File dir;
  private final PathMatcher fileMatcher;

  private final RollMode rollMode;
  private final Comparator<Path> pathComparator;

  /**
   * Creates a <code>LiveDirectoryScanner</code> instance.
   *
   * @param dirName directory to scan.
   * @param firstFileName first 'rolled' file to look for if [@link #scan()} is invoked with <code>null</code>.
   * @param rollMode rolled files mode to use for ordering rolled files.
   * @throws IOException thrown if the scanner could not be created due to an IO error.
   */
  public LiveDirectoryScanner(String dirName, String firstFileName, RollMode rollMode)
      throws IOException {
    Utils.checkNotNull(dirName, "dirName");
    Utils.checkArgument(!dirName.isEmpty(), "dirName cannot be empty");
    Utils.checkNotNull(rollMode, "rollMode");

    dir = new File(dirName);
    if (!dir.exists()) {
      throw new IOException(Utils.format("Directory path '{}' does not exist", dir.getAbsolutePath()));
    }
    if (!dir.isDirectory()) {
      throw new IOException(Utils.format("Directory path '{}' is not a directory", dir.getAbsolutePath()));
    }

    this.rollMode = rollMode;

    // liveFileName needs to be massaged by roll mode
    this.liveFileName = rollMode.getLiveFileName();

    // firstFileName needs to be verified by roll mode
    Utils.checkArgument(this.rollMode.isFirstAcceptable(firstFileName),
        Utils.formatL("firstFileName '{}' is not an acceptable file name", firstFileName));
    this.firstFile = (firstFileName == null || firstFileName.isEmpty()) ? null : new File(dir, firstFileName).toPath();

    pathComparator = this.rollMode.getComparator();

    //TODO check if we need to escape liveFileName
    fileMatcher = FileSystems.getDefault().getPathMatcher(this.rollMode.getPattern());
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
      if (fileMatcher.matches(entry.getFileName()) && Files.isRegularFile(entry)) {
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
      ThreadUtil.sleep(50);
      return scanInternal(current);
    }
  }

  /**
   * Scans the directory for number of files yet to be processed.
   *
   * @param current the last 'rolled' file processed. Use <code>null</code> to look for the first one.
   * @return the number of files yet to be processed.
   * @throws IOException thrown if the directory could not be scanned.
   */
  public long getPendingFiles(LiveFile current) throws IOException{
    //Current will not be acceptable for roll files (if active file is without a counter/date pattern)
    //and will be later renamed to a file with counter/date suffix, if that is the case we should
    //return 0 as number of pending files
    if (current == null || rollMode.isCurrentAcceptable(current.getPath().getFileName().toString())) {
      return findToBeProcessedMatchingFiles(current!=null? current.refresh() : null).size();
    }
    return 0;
  }

  private LiveFile scanInternal(LiveFile current) throws IOException {
    Utils.checkArgument(current == null || rollMode.isCurrentAcceptable(
        current.getPath().getFileName().toString()),
        Utils.formatL("Current file '{}' is not acceptable for live file '{}' using '{}'",
            current, liveFileName, rollMode));
    List<Path> matchingFiles = findToBeProcessedMatchingFiles(current);
    LOG.debug("Scanned '{}' matching files", matchingFiles.size());
    if (matchingFiles.size() > 0) {
      // sort all matching files (they don't necessary come in order from the OS)
      // we sort them using the comparator of the NonLivePostfix
      Collections.sort(matchingFiles, pathComparator);

      // we found a rolled file, create it as such
      current = new LiveFile(matchingFiles.get(0));
    } else {
      // we are not behind with rolled files, lets return the live file
      try {
        if (liveFileName != null) {
          current = new LiveFile(new File(dir, liveFileName).toPath());
        } else {
          current = null;
        }
      } catch (NoSuchFileException ex) {
        // if the live file does not currently exists, return null as we cannot have a LiveFile without an iNode
        current = null;
      }
    }
    LOG.debug("Scan selected '{}' ", current);
    return current;
  }

  private List<Path> findToBeProcessedMatchingFiles(LiveFile current) throws IOException {
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
    try (DirectoryStream<Path> matches = Files.newDirectoryStream(dir.toPath(), filter)) {
      for (Path file : matches) {
        matchingFiles.add(file);
      }
    }
    return matchingFiles;
  }
}
