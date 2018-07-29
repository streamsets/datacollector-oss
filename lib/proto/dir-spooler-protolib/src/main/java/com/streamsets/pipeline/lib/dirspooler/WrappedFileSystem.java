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

import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.lib.io.fileref.AbstractSpoolerFileRef;

import java.io.IOException;
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
   * Tells whether or not the file exists.
   *
   * @param filePath {@link WrappedFile}
   */
  void delete(WrappedFile filePath) throws IOException;

  /**
   * Tells whether or not the file exists.
   *
   * @param filePath {@link WrappedFile}
   */
  void move(WrappedFile filePath, WrappedFile destFilePath) throws IOException;

  /**
   * Tells whether or not the file exists.
   *
   * @param filePath {@link WrappedFile}
   * @return  {@code long} if, and only if, the file exists
   */
  long getLastModifiedTime(WrappedFile filePath) throws IOException;

  /**
   * Tells whether or not the file exists.
   *
   * @param filePath {@link WrappedFile}
   * @return  {@code true} if, and only if, the file exists
   */
  long getChangedTime(WrappedFile filePath) throws IOException;

  /**
   * Tells whether or not the file exists.
   *
   * @param filePath {@link WrappedFile}
   * @return  {@code true} if, and only if, the file exists
   */
  boolean isDirectory(WrappedFile filePath);

  /**
   * Tells whether or not the file exists.
   *
   * @param filePath {@link WrappedFile}
   * @return  {@code true} if, and only if, the file exists
   */
  void addFiles(WrappedFile archiveDirPath, WrappedFile startingFile, List<WrappedFile> toProcess, boolean includeStartingFile, boolean useLastModified) throws IOException;

  /**
   * Tells whether or not the file exists.
   *
   * @param filePath {@link WrappedFile}
   * @return  {@code true} if, and only if, the file exists
   */
  void archiveFiles(WrappedFile archiveDirPath, List<WrappedFile> toProcess, long timeThreshold) throws IOException;

  /**
   * Tells whether or not the file exists.
   *
   * @param filePath {@link WrappedFile}
   * @return  {@code true} if, and only if, the file exists
   */
  void addDirectory(WrappedFile dirPath, List<WrappedFile> directories) throws Exception;

  /**
   * Tells whether or not the file exists.
   *
   * @param filePath {@link WrappedFile}
   * @return  {@code true} if, and only if, the file exists
   */
  void handleOldFiles(WrappedFile dirpath, WrappedFile startingFile, boolean useLastModified, List<WrappedFile> toProcess) throws IOException;

  /**
   * Tells whether or not the file exists.
   *
   * @param filePath {@link WrappedFile}
   * @return  {@code true} if, and only if, the file exists
   */
  WrappedFile getFile(String file);

  /**
   * Tells whether or not the file exists.
   *
   * @param filePath {@link WrappedFile}
   * @return  {@code true} if, and only if, the file exists
   */
  WrappedFile getFile(String dir, String file);

  /**
   * Tells whether or not the file exists.
   *
   * @param filePath {@link WrappedFile}
   * @return  {@code true} if, and only if, the file exists
   */
  void mkdir(WrappedFile filePath);

  /**
   * Tells whether or not the file exists.
   *
   * @param fileName {@code String}
   * @return  {@code true} if, and only if, the file exists
   */
  boolean patternMatches(String fileName);

  /**
   * Tells whether or not the file exists.
   *
   * @param filePath {@code WrappedFile}
   * @return  {@link WrappedFile} if, and only if, the file exists
   */
  Comparator<WrappedFile> getComparator(boolean useLastModified);

  /**
   * Tells whether or not the file exists.
   *
   * @param path1 {@link WrappedFile}
   * @param path2 {@link WrappedFile}
   * @param useLastModified {@code true} if order by last modified timestamp
   * @return  {@code true} if, and only if, the file exists
   */
  int compare(WrappedFile path1, WrappedFile path2, boolean useLastModified);

  /**
   * Tells whether or not the file exists.
   *
   * @param the list of spoolDirPath {@link WrappedFile}
   * @return  {@code true} if, and only if, the file exists
   */
  boolean findDirectoryPathCreationWatcher(List<WrappedFile> spoolDirPath);

  /**
   * Returns the FileRef Builder for whole file data format
   *
   * @return  AbstractSpoolerFileRef.Builder
   */
  AbstractSpoolerFileRef.Builder getFileRefBuilder();
}
