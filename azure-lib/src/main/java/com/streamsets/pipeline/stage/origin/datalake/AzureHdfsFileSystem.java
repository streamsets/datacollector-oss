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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.TimeZone;

/**
 * SDC-12302: AzureHdfsFileSystem has been created as a temporary workaround for HADOOP-16479 to fix the wrong
 *  modification time in the FileStatus object, and should be removed once v3.3.0 is released
 *  HdfsFileSystem.fs should be reverted to private
 *
 * @return An instance of the temporary AzureHdfsFileSystem
 */
public class AzureHdfsFileSystem extends HdfsFileSystem {

  private final static Logger LOG = LoggerFactory.getLogger(AzureHdfsFileSystem.class);

  public AzureHdfsFileSystem(String filePattern, PathMatcherMode mode, boolean processSubdirectories, FileSystem fs) {
    super(filePattern, mode, processSubdirectories, fs);
  }

  @Override
  public void addFiles(WrappedFile dirFile, WrappedFile startingFile, List<WrappedFile> toProcess,
                       boolean includeStartingFile, boolean useLastModified) throws IOException {
    final long scanTime = System.currentTimeMillis();

    PathFilter pathFilter = new PathFilter() {
      @Override
      public boolean accept(org.apache.hadoop.fs.Path entry) {
        try {
          FileStatus fileStatus = fs.getFileStatus(entry);
          if (fileStatus.isDirectory()) {
            return false;
          }

          if(!patternMatches(entry.getName())) {
            return false;
          }

          HdfsFile hdfsFile = new HdfsFile(fs, entry);
          // SDC-12302: Method that temporarily reverts the effects of HADOOP-16479 until version 3.3.0 is available
          long fixedModificationTime = patchLastModifiedTime(fileStatus.getModificationTime());
          if (fixedModificationTime < scanTime) {
            if (startingFile == null || startingFile.toString().isEmpty()) {
              toProcess.add(hdfsFile);
            } else {
              int compares = compare(hdfsFile, startingFile, useLastModified);
              if (includeStartingFile) {
                if (compares >= 0) {
                  toProcess.add(hdfsFile);
                }
              } else {
                if (compares > 0) {
                  toProcess.add(hdfsFile);
                }
              }
            }
          }
        } catch (IOException ex) {
          LOG.error("Failed to open file {}", entry.toString());
        }
        return false;
      }
    };

    fs.globStatus(new Path(dirFile.getAbsolutePath(), "*"), pathFilter);
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
}
