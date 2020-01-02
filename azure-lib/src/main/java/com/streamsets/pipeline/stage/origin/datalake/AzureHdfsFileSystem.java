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
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import com.streamsets.pipeline.stage.origin.hdfs.spooler.HdfsFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * AzureHdfsFileSystem provides some reimplementations to address:
 *
 * SDC-12642: Improve the performance when scanning Azure directories to collect the pending files to be included in the
 * batches. To that end, the walkDirectory function is provided.
 *
 * SDC-12740: Improve the performance when populating the pending files queue by storing the metadata information of the
 * files to avoid unnecessary requests to ADLS service. To that end, metadata is stored in the AzureFile objects and
 * consulted when inserting a new file into the queue (see walkDirectory and getLastModifiedTime).
 *
 * @return An instance of the temporary AzureHdfsFileSystem
 */
public class AzureHdfsFileSystem extends HdfsFileSystem {

  private final static Logger LOG = LoggerFactory.getLogger(AzureHdfsFileSystem.class);

  public AzureHdfsFileSystem(String filePattern, PathMatcherMode mode, boolean processSubdirectories, FileSystem fs) {
    super(filePattern, mode, processSubdirectories, fs);
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

    // Azure DataLake Gen1/Gen2 implementations of Filesystem#listFiles could traverse the same file twice for some
    // (unknown) reason. Using a set to workaround the problem of duplicated files.
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

          WrappedFile file = new AzureFile(fs, fileStatus);
          long modificationTime = fileStatus.getModificationTime();
          if (modificationTime < scanTime) {
            if (startingFile == null || startingFile.toString().isEmpty()) {
              filesSet.add(file);
            } else {
              int compares = compare(file, startingFile, useLastModified);
              if (includeStartingFile) {
                if (compares >= 0) {
                  filesSet.add(file);
                }
              } else {
                if (compares > 0) {
                  filesSet.add(file);
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

  @Override
  public long getLastModifiedTime(WrappedFile file) throws IOException {
    Map<String, Object> metadata = file.getCustomMetadata();

    return (metadata != null && metadata.containsKey(HeaderAttributeConstants.LAST_MODIFIED_TIME))
           ? (long) metadata.get(HeaderAttributeConstants.LAST_MODIFIED_TIME)
           : fs.getFileStatus(new Path(file.getAbsolutePath())).getModificationTime();
  }

  @Override
  public Comparator<WrappedFile> getComparator(boolean useLastModified) {
    return (WrappedFile file1, WrappedFile file2) -> {
      String filePath1 = file1.getAbsolutePath();
      String filePath2 = file2.getAbsolutePath();

      if (useLastModified) {
        if (filePath1.isEmpty() || filePath2.isEmpty()) {
          return 1;
        }

        try {
          long mtime1 = getLastModifiedTime(file1);
          long mtime2 = getLastModifiedTime(file2);

          int compares = Long.compare(mtime1, mtime2);
          if (compares != 0) {
            return compares;
          }

        } catch (IOException ex) {
          LOG.error("Could not sort files due to IO Exception", ex);
          throw new RuntimeException(ex);
        }
      }

      return filePath1.compareTo(filePath2);
    };
  }

  @Override
  public int compare(WrappedFile path1, WrappedFile path2, boolean useLastModified) {
    if (path1 == null || path2 == null) {
      return 1;
    }

    String filePath1 = path1.getAbsolutePath();
    String filePath2 = path2.getAbsolutePath();

    if (useLastModified) {
      if (filePath1.isEmpty() || filePath2.isEmpty()) {
        return 1;
      }

      try {
        long mtime1 = getLastModifiedTime(path1);
        long mtime2 = getLastModifiedTime(path2);

        int compares = Long.compare(mtime1, mtime2);
        if (compares != 0) {
          return compares;
        }

      } catch (IOException ex) {
        LOG.debug("Starting file may have already been archived.", ex);
        return 1;
      } catch (RuntimeException ex) {
        LOG.error("Error while comparing files", ex);
        throw ex;
      }
    }

    return filePath1.compareTo(filePath2);
  }

  @Override
  public boolean isDirectory(WrappedFile file) {
    Map<String, Object> metadata = file.getCustomMetadata();

    if (metadata != null && metadata.containsKey(HeaderAttributeConstants.IS_DIRECTORY)) {
      return (boolean) metadata.get(HeaderAttributeConstants.IS_DIRECTORY);
    } else {
      try {
        return fs.isDirectory(new Path(file.getAbsolutePath()));
      } catch (IOException ex) {
        LOG.error("failed to open file: '{}'", file.getFileName(), ex);
        return false;
      }
    }
  }


}
