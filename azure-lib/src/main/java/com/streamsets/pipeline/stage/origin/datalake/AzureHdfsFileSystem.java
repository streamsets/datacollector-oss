/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.datalake;

import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.lib.dirspooler.WrappedFile;
import com.streamsets.pipeline.stage.origin.hdfs.spooler.HdfsFile;
import com.streamsets.pipeline.stage.origin.hdfs.spooler.HdfsFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

/**
 * AzureHdfsFileSystem provides some reimplementations to address:
 *
 * SDC-12302: A temporary workaround for HADOOP-16479 to fix the wrong modification time in the FileStatus object. It
 * should be removed once v3.3.0 is released, and HdfsFileSystem.fs should be reverted to private.
 *
 * SDC-12642: Improve the performance when scanning Azure directories to collect the pending files to be included in the
 * batches. To that end, the walkDirectory function is provided.
 *
 * @return An instance of the temporary AzureHdfsFileSystem
 */
public class AzureHdfsFileSystem extends HdfsFileSystem {

  private final static Logger LOG = LoggerFactory.getLogger(AzureHdfsFileSystem.class);

  public AzureHdfsFileSystem(String filePattern, PathMatcherMode mode, boolean processSubdirectories, FileSystem fs) {
    super(filePattern, mode, processSubdirectories, fs);
  }

  /**
   * SDC-12302: Method that temporarily reverts the effects of HADOOP-16479 until version 3.3.0 is available
   *
   * @return The corrected modification time after patching the time zone offset
   */
  public long patchLastModifiedTime(long buggyModTime) {
    long offset = TimeZone.getDefault().getOffset(System.currentTimeMillis());
    return buggyModTime + offset;
  }

  /**
   * Expand a potential Glob pattern and return the paths corresponding to files and directories.
   *
   * @param path The potential Glob pattern to expand.
   * @param includeFiles If false, files are filtered out from the resulting list.
   * @param includeDirs If false, directories are filtered out from the resulting list.
   * @return If path is not a Glob pattern, return a list with that path or empty list, depending on the filter
   *   parameters. If path is a Glob pattern, return a list with the expansion after being filtered.
   */
  private List<FileStatus> expandGlobPattern(WrappedFile path, boolean includeFiles, boolean includeDirs) {

    List<FileStatus> result = new ArrayList<>();
    try {
      // Prefer globStatus without filter and then filter the result. Otherwise, getFileStatus would be called twice
      // for each file (inside globStatus and inside the filter).
      FileStatus[] stats = fs.globStatus(new Path(path.getAbsolutePath()));

      for (FileStatus stat : stats) {
        if ((includeFiles && stat.isFile()) || (includeDirs && stat.isDirectory())) {
          result.add(stat);
        }
      }
    } catch (IOException ex) {
      LOG.error("Failed to expand potential glob pattern '{}'", path.toString(), ex);
    }

    return result;
  }

  public List<WrappedFile> walkDirectory(WrappedFile dirPath, WrappedFile startingFile, boolean includeStartingFile,
      boolean useLastModified) {

    // Azure DataLake Gen1/Gen2 implementations of Filesystem#listFiles could traverse the same file twice under some
    // circumstances (observed for subdirs deeper than 10 nested levels). Using a set to workaround the problem of
    // duplicated files.
    Set<WrappedFile> filesSet = new HashSet<>();
    List<FileStatus> dirs = expandGlobPattern(dirPath, false, true);
    long scanTime = System.currentTimeMillis();

    for (FileStatus dirStatus : dirs) {
      try {
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(dirStatus.getPath(), processSubdirectories);
        while (iter.hasNext()) {
          FileStatus fileStatus = iter.next();
          LOG.trace("walkDirectory: {}", fileStatus.getPath());
          if (fileStatus.isDirectory() || !patternMatches(fileStatus.getPath().getName())) {
            continue;
          }

          HdfsFile hdfsFile = new HdfsFile(fs, fileStatus.getPath());
          // SDC-12302: Method that temporarily reverts the effects of HADOOP-16479 until version 3.3.0 is available
          long fixedModificationTime = patchLastModifiedTime(fileStatus.getModificationTime());
          if (fixedModificationTime < scanTime) {
            if (startingFile == null || startingFile.toString().isEmpty()) {
              filesSet.add(hdfsFile);
            } else {
              int compares = compare(hdfsFile, startingFile, useLastModified);
              if (includeStartingFile) {
                if (compares >= 0) {
                  filesSet.add(hdfsFile);
                }
              } else {
                if (compares > 0) {
                  filesSet.add(hdfsFile);
                }
              }
            }
          }
        }
      } catch (IOException ex) {
        LOG.error("Failed to list files in '{}'", dirStatus.getPath().toString(), ex);
      }
    }
    return new ArrayList<>(filesSet);
  }

}
