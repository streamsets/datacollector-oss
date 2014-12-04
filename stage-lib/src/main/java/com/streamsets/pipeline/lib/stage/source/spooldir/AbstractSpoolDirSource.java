/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.util.StageLibError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
      defaultValue = "10",
      description =
          "Maximum number of files in spool directory waiting to be processed, if exceeded teh source goes into error")
  public int maxSpoolFiles;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      label = "First File to Process",
      description = "If set, all files lexicographically older than this will be ignored",
      defaultValue = "")
  public String initialFileToProcess;


  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "File Post Processing Handling",
      description = "Action to take after the file has been processed: NONE (default), DELETE, ARCHIVE",
      defaultValue = "NONE")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = PostProcessingOptionsChooserValues.class)
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
      defaultValue = "60")
  public long poolingTimeoutSecs;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      label = "Error Archive Directory",
      description = "Directory to archive files that could not be fully processed due to unrecoverable data errors")
  public String errorArchiveDir;

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
    if (errorArchiveDir != null && !errorArchiveDir.isEmpty()) {
      builder.setErrorArchiveDir(errorArchiveDir);
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
      currentFile = null;
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
          LOG.debug("No new file available in spool directory after '{}' secs", poolingTimeoutSecs);
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
      try {
        offset = produce(currentFile, offset, maxBatchSize, batchMaker);
      } catch (BadSpoolFileException ex) {
        long filePos = (ex.getCause() != null && ex.getCause() instanceof OverrunException)
                       ? ((OverrunException)ex.getCause()).getStreamOffset() : -1;
        LOG.error("Spool file '{}' at position '{}', {}", currentFile, filePos, ex.getMessage(), ex);
        try {
          spooler.handleFileInError(currentFile);
        } catch (IOException ex1) {
          throw new StageException(StageLibError.LIB_0001, currentFile, ex1.getMessage(), ex1);
        }
        offset = -1;
      }
    }
    return createSourceOffset(file, offset);
  }

  //return -1 if file was fully read
  protected abstract long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker)
      throws StageException, BadSpoolFileException;

}
