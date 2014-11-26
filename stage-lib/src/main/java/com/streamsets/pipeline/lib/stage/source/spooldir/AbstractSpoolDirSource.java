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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

public abstract class AbstractSpoolDirSource extends BaseSource {
  private final static Logger LOG = LoggerFactory.getLogger(AbstractSpoolDirSource.class);

  private static final String OFFSET_SEPARATOR = "::";

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "Spool Directory")
  public String spoolDir;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "File Pattern",
      description = "File pattern to look for, files must be lexicographically monotonic increasing")
  public String filePattern;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      label = "Max Files in Spool Directory",
      description =
          "Maximum number of files in spool directory waiting to be processed, if exceeded teh source goes into error")
  public int maxSpoolFiles;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      label = "First File to Process",
      description = "If set, all files lexicographically older than this will be ignored")
  public String initialFileToProcess;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "File Post Processing Handling",
      description = "Action to take after the file has been processed: NONE (default), DELETE, ARCHIVE",
      defaultValue = "NONE")
  public String postProcessing;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      label = "Archive Directory",
      description = "Directory to archive processed files after processing. " +
                    "Only used if post processing is set to ARCHIVE")
  public String archiveDir;

  @ConfigDef(required = false,
      type = ConfigDef.Type.INTEGER,
      label = "Archive Retention Time (minutes)",
      description = "How long archived files should be kept before deleting, a value of zero means forever. " +
                    "Only used if post processing is set to ARCHIVE",
      defaultValue = "0")
  public long retentionTimeMins;

  @ConfigDef(required = false,
      type = ConfigDef.Type.INTEGER,
      label = "File Pooling Timeout (secs)",
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
        setMaxSpoolFiles(maxSpoolFiles).setPostProcessing(postProc);
    if (postProc == DirectorySpooler.FilePostProcessing.ARCHIVE) {
      builder.setArchiveDir(archiveDir);
      builder.setArchiveRetention(retentionTimeMins);
    }
    builder.setContext(getContext());
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
    return (file != null) ? file + OFFSET_SEPARATOR + Long.toString(fileOffset) : null;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String file = getFileFromSourceOffset(lastSourceOffset);
    long offset = getOffsetFromSourceOffset(lastSourceOffset);
    if (currentFile == null || file == null || currentFile.getName().compareTo(file) < 0 || offset == -1) {
      try {
        File nextAvailFile = null;
        do {
          if (nextAvailFile != null) {
            LOG.warn("Ignoring file '{}' in spool directory as is lesser than offset file '{}'",
                     nextAvailFile.getName(), file);
          }
          nextAvailFile = getSpooler().poolForFile(poolingTimeoutSecs, TimeUnit.SECONDS);
        } while (nextAvailFile != null && (file != null && nextAvailFile.getName().compareTo(file) < 0));
        if (nextAvailFile == null) {
          LOG.debug("No new file available in spool directory after '{}' secs, dispatching an empty batch",
                    poolingTimeoutSecs);
        } else {
          currentFile = nextAvailFile;
          if (file == null || nextAvailFile.getName().compareTo(file) > 0) {
            file = currentFile.getName();
            offset = 0;
          }
        }
      } catch (InterruptedException ex) {
        LOG.warn("Spool pooling interrupted '{}'", ex.getMessage(), ex);
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
