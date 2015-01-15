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
  public DirectorySpooler.FilePostProcessing postProcessing;

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
    DirectorySpooler.Builder builder = DirectorySpooler.builder().setDir(spoolDir).setFilePattern(filePattern).
        setMaxSpoolFiles(maxSpoolFiles).setPostProcessing(postProcessing);
    if (postProcessing == DirectorySpooler.FilePostProcessing.ARCHIVE) {
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

  private boolean hasToFetchNextFileFromSpooler(String file, long offset) {
    return
        // we don't have a current file half way processed in the current agent execution
        currentFile == null ||
        // we don't have a file half way processed from a previous agent execution via offset tracking
        file == null ||
        // the current file is lexicographically lesser than the one reported via offset tracking
        // this can happen if somebody drop
        currentFile.getName().compareTo(file) < 0 ||
        // the current file has been fully processed
        offset == -1;
  }

  private boolean isFileFromSpoolerEligible(File spoolerFile, String offsetFile, long offsetInFile) {
    if (spoolerFile == null) {
      // there is no new file from spooler, we return yes to break the loop
      return true;
    }
    if (offsetFile == null) {
      // file reported by offset tracking is NULL, means we are starting from zero
      return true;
    }
    if (spoolerFile.getName().compareTo(offsetFile) == 0 && offsetInFile != -1) {
      // file reported by spooler is equal than current offset file
      // and we didn't fully process (not -1) the current file
      return true;
    }
    if (spoolerFile.getName().compareTo(offsetFile) > 0) {
      // file reported by spooler is newer than current offset file
      return true;
    }
    return false;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    // if lastSourceOffset is NULL (beginning of source) it returns NULL
    String file = getFileFromSourceOffset(lastSourceOffset);
    // if lastSourceOffset is NULL (beginning of source) it returns 0
    long offset = getOffsetFromSourceOffset(lastSourceOffset);
    if (hasToFetchNextFileFromSpooler(file, offset)) {
      currentFile = null;
      try {
        File nextAvailFile = null;
        do {
          if (nextAvailFile != null) {
            LOG.warn("Ignoring file '{}' in spool directory as is lesser than offset file '{}'",
                     nextAvailFile.getName(), file);
          }
          nextAvailFile = getSpooler().poolForFile(poolingTimeoutSecs, TimeUnit.SECONDS);
        } while (!isFileFromSpoolerEligible(nextAvailFile, file, offset));

        if (nextAvailFile == null) {
          // no file to process
          LOG.debug("No new file available in spool directory after '{}' secs, produccing empty batch",
                    poolingTimeoutSecs);
        } else {
          // file to process
          currentFile = nextAvailFile;

          // if the current offset file is null or the file returned by the spooler is greater than the current offset
          // file we take the file returned by the spooler as the new file and set the offset to zero
          // if not, it means the spooler returned us the current file, we just keep processing it from the last
          // offset we processed (known via offset tracking)
          if (file == null || nextAvailFile.getName().compareTo(file) > 0) {
            file = currentFile.getName();
            offset = 0;
          }
        }
      } catch (InterruptedException ex) {
        // the spooler was interrupted while waiting for a file, we log and return, the pipeline agent will invoke us
        // again to wait for a file again
        LOG.warn("Pooling interrupted");
      }
    }

    if (currentFile != null) {
      // we have a file to process (from before or new from spooler)
      try {
        // we ask for a batch from the currentFile starting at offset
        offset = produce(currentFile, offset, maxBatchSize, batchMaker);
      } catch (BadSpoolFileException ex) {
        // the processing fo the current file had an unrecoverable error we log the reason, file and offset if avail
        long filePos = (ex.getCause() != null && ex.getCause() instanceof OverrunException)
                       ? ((OverrunException)ex.getCause()).getStreamOffset() : -1;
        LOG.error(StageLibError.LIB_0101.getMessage(), currentFile, filePos, ex.getMessage(), ex);
        getContext().reportError(StageLibError.LIB_0101, currentFile, filePos, ex.getMessage());
        try {
          // then we ask the spooler to error handle the failed file
          spooler.handleFileInError(currentFile);
        } catch (IOException ex1) {
          throw new StageException(StageLibError.LIB_0100, currentFile, ex1.getMessage(), ex1);
        }
        // we set the offset to -1 to indicate we are done with the file and we should fetch a new one from the spooler
        offset = -1;
      }
    }
    // create a new offset using the current file and offset
    return createSourceOffset(file, offset);
  }

  /**
   * Processes a batch from the specified file and offset up to a maximum batch size. If the file is fully process
   * it must return -1, otherwise it must return the offset to continue from next invocation.
   */
  protected abstract long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker)
      throws StageException, BadSpoolFileException;

}
