/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.lib.dirspooler;

import com.streamsets.pipeline.lib.io.fileref.AbstractSpoolerFileRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Interface for components that are exposing filesystem of both local and hdfs to the dirspooler
 */
public interface WrappedFileSystem {
  /**
   * Tells whether or not the file exists.
   *
   * @param filePath {@link WrappedFile}
   * @return  {@code true} if, and only if, the file exists
   */
  boolean exists(WrappedFile filePath);

  /**
   * Delete given file.
   *
   * @param filePath
   * @throws IOException
   */
  void delete(WrappedFile filePath) throws IOException;

  /**
   * Move given file.
   *
   * @param filePath Original file
   * @param destFilePath Destination file (including filename)
   * @throws IOException
   */
  void move(WrappedFile filePath, WrappedFile destFilePath) throws IOException;

  /**
   * Get last modified time.
   *
   * @param filePath
   * @return
   * @throws IOException
   */
  long getLastModifiedTime(WrappedFile filePath) throws IOException;

  /**
   * Get last file change time.
   *
   * @param filePath
   * @return
   * @throws IOException
   */
  long getChangedTime(WrappedFile filePath) throws IOException;

  /**
   * Verify if given path represents a directory.
   *
   * @param filePath
   * @return
   */
  boolean isDirectory(WrappedFile filePath);

  /**
   * Scan given directory for files.
   *
   * @param dirPath Directory to scan
   * @param startingFile Starting file
   * @param toProcess Resulting queue to store newly discovered files
   * @param includeStartingFile Whether or not to include the starting file itself in the resulting queue
   * @param useLastModified
   * @throws IOException
   */
  void addFiles(WrappedFile dirPath, WrappedFile startingFile, List<WrappedFile> toProcess, boolean includeStartingFile, boolean useLastModified) throws IOException;

  /**
   * Move files to the archive directory.
   *
   * @param archiveDirPath
   * @param toProcess
   * @param timeThreshold
   * @throws IOException
   */
  void archiveFiles(WrappedFile archiveDirPath, List<WrappedFile> toProcess, long timeThreshold) throws IOException;

  /**
   * Scan for all sub-directories on given path.
   *
   * @param dirPath Directory to scan
   * @param directories Resulting queue to store newly discovered directories
   * @throws Exception
   */
  void addDirectory(WrappedFile dirPath, List<WrappedFile> directories) throws Exception;

  /**
   * Walk through a directory and collect all the files contained inside that are pending to read. Glob patterns
   * are supported; in which case the walk will be performed for each directory that matches the pattern, and the
   * resulting list will be a concatenation of the files found for each directory.
   *
   * @param dirPath Directory or glob pattern taken as the root path for the walk.
   * @param startingFile Employed to filter out any file already read, according to the read order criteria. If null
   *     or empty, no file will be discarded.
   * @param includeStartingFile Discard or not the startingFile from the returned list.
   * @param useLastModified Define the read order criteria: last modified timestamp or lexicographical order.
   * @return A list containing all the files found which are pending to read.
   */
  default List<WrappedFile> walkDirectory(WrappedFile dirPath, WrappedFile startingFile,
      boolean includeStartingFile, boolean useLastModified) {
    return new ArrayList<>();
  }

  /**
   * ?
   *
   * @param dirpath
   * @param startingFile
   * @param useLastModified
   * @param toProcess
   * @throws IOException
   */
  void handleOldFiles(WrappedFile dirpath, WrappedFile startingFile, boolean useLastModified, List<WrappedFile> toProcess) throws IOException;

  /**
   * Return WrappedFile variant for given file path.
   *
   * @param file
   * @return
   * @throws IOException
   */
  WrappedFile getFile(String file) throws IOException;

  /**
   * Return WrappedFile variant for given file path.
   *
   * @param file
   * @return
   * @throws IOException
   */
  WrappedFile getFile(String dir, String file) throws IOException;

  /**
   * Create all missing directories for given path.
   *
   * @param filePath
   * @throws IOException
   */
  void mkdirs(WrappedFile filePath) throws IOException;

  /**
   * @param fileName
   * @return
   */
  boolean patternMatches(String fileName);

  /**
   * Returns file comparator to verify which file should be processed first.
   *
   * @param useLastModified
   * @return
   */
  Comparator<WrappedFile> getComparator(boolean useLastModified);

  /**
   * Safe comparator call that avoids various error conditions such as doesn't throw NoSuchFileException if
   * files were moved (archived) while processing.
   *
   * @param path1 {@link WrappedFile}
   * @param path2 {@link WrappedFile}
   * @param useLastModified {@code true} if order by last modified timestamp
   * @return  {@code true} if, and only if, the file exists
   */
  int compare(WrappedFile path1, WrappedFile path2, boolean useLastModified);

  /**
   * @param spoolDirPath
   * @return
   */
  boolean findDirectoryPathCreationWatcher(List<WrappedFile> spoolDirPath);

  /**
   * Returns the FileRef Builder for whole file data format
   *
   * @return  AbstractSpoolerFileRef.Builder
   */
  AbstractSpoolerFileRef.Builder getFileRefBuilder();
}
