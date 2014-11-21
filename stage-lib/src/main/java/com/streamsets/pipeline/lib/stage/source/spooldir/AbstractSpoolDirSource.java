/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;

import java.io.File;
import java.util.concurrent.TimeUnit;

public abstract class AbstractSpoolDirSource extends BaseSource {

  private static final String OFFSET_SEPARATOR = "::";

  @ConfigDef(name="spoolDir",
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Spool directory")
  public String spoolDir;

  @ConfigDef(name="filePattern",
      required = true,
      type = ConfigDef.Type.STRING,
      label = "File pattern to look for, files must be lexicographically monotonic increasing")
  public String filePattern;


  @ConfigDef(name="initialFile",
      required = false,
      type = ConfigDef.Type.STRING,
      label = "First file to process",
      description = "If set, all files lexicographically older than this will be ignored")
  public String initialFileToProcess;

  @ConfigDef(name="postProcessing",
      required = true,
      type = ConfigDef.Type.STRING,
      label = "File post processing handling",
      description = "Action to take after the file has been processed: NONE (default), DELETE, ARCHIVE",
      defaultValue = "NONE")
  public String postProcessing;

  @ConfigDef(name="archiveDir",
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Archive directory",
      description = "Directory to archive processed files after processing. " +
                    "Only used if post processing is set to ARCHIVE")
  public String archiveDir;

  @ConfigDef(name="archiveRetention",
      required = false,
      type = ConfigDef.Type.INTEGER,
      label = "Archive retention time",
      description = "How long archived files should be kept before deleting, a value of zero means forever. " +
                    "Only used if post processing is set to ARCHIVE",
      defaultValue = "0")
  public long retentionTime;

  @ConfigDef(name="retentionUnit",
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Archive retention time unit",
      description = "Time unit for archive retention: SECONDS, MINUTES, HOURS, DAYS. " +
                    "Only used if post processing is set to ARCHIVE",
      defaultValue = "DAYS")
  public String retentionTimeUnit;

  @ConfigDef(name="poolingTimeOut",
      required = false,
      type = ConfigDef.Type.INTEGER,
      label = "File pooling timeout (secs)",
      description = "Timeout when waiting for a new file, when a timeout happens, an empty batch will be run",
      defaultValue = "DAYS")
  public long poolingTimeoutSecs;

  private DirectorySpooler spooler;
  private File currentFile;

  @Override
  protected void init() throws StageException {
    super.init();
    DirectorySpooler.FilePostProcessing postProc = DirectorySpooler.FilePostProcessing.valueOf(postProcessing);
    DirectorySpooler.Builder builder = DirectorySpooler.builder().setDir(spoolDir).setFilePattern(filePattern).
        setPostProcessing(postProc);
    if (postProc == DirectorySpooler.FilePostProcessing.ARCHIVE) {
      builder.setArchiveDir(archiveDir);
      builder.setArchiveRetention(retentionTime, TimeUnit.valueOf(retentionTimeUnit));
    }
    spooler = builder.build();
    spooler.init(initialFileToProcess);
  }

  @Override
  public void destroy() {
    spooler.destroy();
    super.destroy();
  }

  protected DirectorySpooler getSpooler() {
    return spooler;
  }

  protected String getFileFromSourceOffset(String sourceOffset) throws StageException {
    String file = null;
    if (sourceOffset != null) {
      file = sourceOffset;
      int separator = sourceOffset.indexOf(OFFSET_SEPARATOR);
      if (separator > -1) {
        file = sourceOffset.substring(0, separator);
      }
    }
    return file;
  }

  protected long getOffsetFromSourceOffset(String sourceOffset) throws StageException {
    long offset = 0;
    if (sourceOffset != null) {
      int separator = sourceOffset.indexOf(OFFSET_SEPARATOR);
      if (separator > -1) {
        offset = Long.parseLong(sourceOffset.substring(separator + OFFSET_SEPARATOR.length()));
      }
    }
    return offset;
  }

  protected String createSourceOffset(String file, long fileOffset) {
    return file + OFFSET_SEPARATOR + Long.toString(fileOffset);
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String file = getFileFromSourceOffset(lastSourceOffset);
    long offset = getOffsetFromSourceOffset(lastSourceOffset);
    if (currentFile == null || currentFile.getName().compareTo(file) < 0 || offset != -1) {
      try {
        File nextAvailFile = null;
        do {
          if (nextAvailFile != null) {
            //LOG warn discarding nextAvailFile {} as offset asked for file {}
          }
          nextAvailFile = getSpooler().poolForFile(poolingTimeoutSecs, TimeUnit.SECONDS);
        } while (nextAvailFile != null && nextAvailFile.getName().compareTo(file) < 0);
        if (nextAvailFile == null) {
          //LOG debug timeout waiting or file
        } else {
          currentFile = nextAvailFile;
          file = currentFile.getName();
          offset = 0;
        }
      } catch (InterruptedException ex) {
      }
    }
    if (currentFile != null) {
      offset = produce(currentFile, offset, maxBatchSize, batchMaker);
    }
    return createSourceOffset(file, offset);
  }

  //return -1 if file was fully read
  protected abstract long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker)
      throws StageException;

}
