/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;
import org.apache.xerces.util.XMLChar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.PathMatcher;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SpoolDirSource extends BaseSource {
  private final static Logger LOG = LoggerFactory.getLogger(SpoolDirSource.class);
  private static final String OFFSET_SEPARATOR = "::";

  private final DataFormat dataFormat;
  private final String spoolDir;
  private final int batchSize;
  private long poolingTimeoutSecs;
  private String filePattern;
  private int maxSpoolFiles;
  private String initialFileToProcess;
  private final String errorArchiveDir;
  private final PostProcessingOptions postProcessing;
  private final String archiveDir;
  private final long retentionTimeMins;
  private final CsvMode csvFileFormat;
  private final boolean hasHeaderLine;
  private final boolean convertToMap;
  private final JsonMode jsonContent;
  private final int maxJsonObjectLen;
  private final int maxLogLineLength;
  private final boolean setTruncated;
  private final String xmlRecordElement;
  private final int maxXmlObjectLen;

  public SpoolDirSource(DataFormat dataFormat, String spoolDir, int batchSize, long poolingTimeoutSecs,
      String filePattern, int maxSpoolFiles, String initialFileToProcess, String errorArchiveDir,
      PostProcessingOptions postProcessing, String archiveDir, long retentionTimeMins,
      CsvMode csvFileFormat, boolean hasHeaderLine, boolean convertToMap,
      JsonMode jsonContent, int maxJsonObjectLen, int maxLogLineLength, boolean setTruncated,
      String xmlRecordElement, int maxXmlObjectLen) {
    this.dataFormat = dataFormat;
    this.spoolDir = spoolDir;
    this.batchSize = batchSize;
    this.poolingTimeoutSecs = poolingTimeoutSecs;
    this.filePattern = filePattern;
    this.maxSpoolFiles = maxSpoolFiles;
    this.initialFileToProcess = initialFileToProcess;
    this.errorArchiveDir = errorArchiveDir;
    this.postProcessing = postProcessing;
    this.archiveDir = archiveDir;
    this.retentionTimeMins = retentionTimeMins;
    this.csvFileFormat = csvFileFormat;
    this.hasHeaderLine = hasHeaderLine;
    this.convertToMap = convertToMap;
    this.jsonContent = jsonContent;
    this.maxJsonObjectLen = maxJsonObjectLen;
    this.maxLogLineLength = maxLogLineLength;
    this.setTruncated = setTruncated;
    this.xmlRecordElement = xmlRecordElement;
    this.maxXmlObjectLen = maxXmlObjectLen;
  }

  private DirectorySpooler spooler;
  private File currentFile;
  private DataProducer dataProducer;

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();

    validateDataFormat(issues);

    validateDir(spoolDir, Groups.FILES.name(), "spoolDir", issues);

    if (batchSize < 1) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "batchSize", Errors.SPOOLDIR_14));
    }

    if (poolingTimeoutSecs < 1) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "poolingTimeoutSecs", Errors.SPOOLDIR_15));
    }

    validateFilePattern(issues);

    if (maxSpoolFiles < 1) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "maxSpoolFiles", Errors.SPOOLDIR_17));
    }

    validateInitialFileToProcess(issues);

    if (errorArchiveDir != null && !errorArchiveDir.isEmpty()) {
      validateDir(errorArchiveDir, Groups.POST_PROCESSING.name(), "errorArchiveDir", issues);
    }

    if (postProcessing == PostProcessingOptions.ARCHIVE) {
      validateDir(archiveDir, Groups.POST_PROCESSING.name(), "archiveDir", issues);
    }

    if (retentionTimeMins < 1) {
      issues.add(getContext().createConfigIssue(Groups.POST_PROCESSING.name(), "retentionTimeMins", Errors.SPOOLDIR_19));
    }

    switch (dataFormat) {
      case JSON:
        if (maxJsonObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.JSON.name(), "maxJsonObjectLen", Errors.SPOOLDIR_20));
        }
        break;
      case TEXT:
        if (maxLogLineLength < 1) {
          issues.add(getContext().createConfigIssue(Groups.TEXT.name(), "maxLogLineLength", Errors.SPOOLDIR_21));
        }
        break;
      case XML:
        if (maxXmlObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "maxXmlObjectLen", Errors.SPOOLDIR_22));
        }
        if (!XMLChar.isValidName(xmlRecordElement)) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "xmlRecordElement", Errors.SPOOLDIR_23, xmlRecordElement));
        }
        break;
    }

    return issues;
  }

  private void validateDataFormat(List<ConfigIssue> issues) {
    switch (dataFormat) {
      case TEXT:
      case JSON:
      case DELIMITED:
      case XML:
      case SDC_JSON:
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.FILES.name(), "dataFormat", Errors.SPOOLDIR_10, dataFormat));
    }
  }

  private void validateDir(String dir, String group, String config, List<ConfigIssue> issues) {
    if (dir.isEmpty()) {
      issues.add(getContext().createConfigIssue(group, config, Errors.SPOOLDIR_11));
    }
    File fDir = new File(dir);
    if (!fDir.exists()) {
      issues.add(getContext().createConfigIssue(group, config, Errors.SPOOLDIR_12, dir));
    }
    if (!fDir.isDirectory()) {
      issues.add(getContext().createConfigIssue(group, config, Errors.SPOOLDIR_13, dir));
    }
  }

  private void validateFilePattern(List<ConfigIssue> issues) {
    try {
      DirectorySpooler.createPathMatcher(filePattern);
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "filePattern", Errors.SPOOLDIR_16, filePattern,
                                                ex.getMessage(), ex));
    }
  }

  private void validateInitialFileToProcess(List<ConfigIssue> issues) {
    try {
      PathMatcher pathMatcher = DirectorySpooler.createPathMatcher(filePattern);
      if (!pathMatcher.matches(new File(initialFileToProcess).toPath())) {
        issues.add(getContext().createConfigIssue(Groups.FILES.name(), "initialFileToProcess", Errors.SPOOLDIR_18,
                                                  initialFileToProcess, filePattern));
      }
    } catch (Exception ex) {
    }
  }

  @Override
  protected void init() throws StageException {
    super.init();

    switch (dataFormat) {
      case TEXT:
        dataProducer = new LogDataProducer(getContext(), maxLogLineLength, setTruncated);
        break;
      case JSON:
        dataProducer = new JsonDataProducer(getContext(), jsonContent, maxJsonObjectLen);
        break;
      case DELIMITED:
        dataProducer = new CsvDataProducer(getContext(), csvFileFormat, hasHeaderLine, convertToMap);
        break;
      case XML:
        dataProducer = new XmlDataProducer(getContext(), xmlRecordElement, maxXmlObjectLen);
        break;
      case SDC_JSON:
        filePattern = "records-??????.json";
        initialFileToProcess = "";
        maxSpoolFiles = 10000;
        dataProducer = new RecordJsonDataProducer(getContext());
        break;
    }

    if (getContext().isPreview()) {
      poolingTimeoutSecs = 1;
    }

    DirectorySpooler.Builder builder = DirectorySpooler.builder().setDir(spoolDir).setFilePattern(filePattern).
        setMaxSpoolFiles(maxSpoolFiles).setPostProcessing(postProcessing.getSpoolerAction());
    if (postProcessing == PostProcessingOptions.ARCHIVE) {
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
    int batchSize = Math.min(this.batchSize, maxBatchSize);
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
        offset = produce(currentFile, offset, batchSize, batchMaker);
      } catch (BadSpoolFileException ex) {
        LOG.error(Errors.SPOOLDIR_01.getMessage(), ex.getFile(), ex.getPos(), ex.getMessage(), ex);
        getContext().reportError(Errors.SPOOLDIR_01, ex.getFile(), ex.getPos(), ex.getMessage());
        try {
          // then we ask the spooler to error handle the failed file
          spooler.handleCurrentFileAsError();
        } catch (IOException ex1) {
          throw new StageException(Errors.SPOOLDIR_00, currentFile, ex1.getMessage(), ex1);
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
  public long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker) throws StageException,
      BadSpoolFileException {
    return dataProducer.produce(file, offset, maxBatchSize, batchMaker);
  }

}
