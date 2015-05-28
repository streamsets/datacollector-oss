/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;
import com.streamsets.pipeline.lib.io.ObjectLengthException;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.lib.parser.xml.XmlDataParserFactory;
import org.apache.xerces.util.XMLChar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.PathMatcher;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SpoolDirSource extends BaseSource {
  private final static Logger LOG = LoggerFactory.getLogger(SpoolDirSource.class);
  private static final String OFFSET_SEPARATOR = "::";
  private static final int MIN_OVERRUN_LIMIT = 64 * 1000;
  private static final int MAX_OVERRUN_LIMIT = 1000 * 1000;

  private final DataFormat dataFormat;
  private String charset;
  private final int overrunLimit;
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
  private final CsvHeader csvHeader;
  private final int csvMaxObjectLen;
  private final JsonMode jsonContent;
  private final int jsonMaxObjectLen;
  private final int textMaxLineLen;
  private final String xmlRecordElement;
  private final int xmlMaxObjectLen;
  private final LogMode logMode;
  private final int logMaxObjectLen;
  private final boolean logRetainOriginalLine;
  private final String customLogFormat;
  private final String regex;
  private final String grokPatternDefinition;
  private final String grokPattern;
  private final List<RegExConfig> fieldPathsToGroupName;
  private final boolean enableLog4jCustomLogFormat;
  private final String log4jCustomLogFormat;
  private final int maxStackTraceLines;
  private final OnParseError onParseError;

  public SpoolDirSource(DataFormat dataFormat, String charset, int overrunLimit, String spoolDir, int batchSize,
      long poolingTimeoutSecs,
      String filePattern, int maxSpoolFiles, String initialFileToProcess, String errorArchiveDir,
      PostProcessingOptions postProcessing, String archiveDir, long retentionTimeMins,
      CsvMode csvFileFormat, CsvHeader csvHeader, int csvMaxObjectLen, JsonMode jsonContent, int jsonMaxObjectLen,
      int textMaxLineLen, String xmlRecordElement, int xmlMaxObjectLen, LogMode logMode, int logMaxObjectLen,
      boolean retainOriginalLine, String customLogFormat, String regex, List<RegExConfig> fieldPathsToGroupName,
      String grokPatternDefinition, String grokPattern, boolean enableLog4jCustomLogFormat,
      String log4jCustomLogFormat, OnParseError onParseError, int maxStackTraceLines) {
    this.dataFormat = dataFormat;
    this.charset = charset;
    this.overrunLimit = overrunLimit * 1000;
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
    this.csvHeader = csvHeader;
    this.csvMaxObjectLen = csvMaxObjectLen;
    this.jsonContent = jsonContent;
    this.jsonMaxObjectLen = jsonMaxObjectLen;
    this.textMaxLineLen = textMaxLineLen;
    this.xmlRecordElement = xmlRecordElement;
    this.xmlMaxObjectLen = xmlMaxObjectLen;
    this.logMode = logMode;
    this.logMaxObjectLen = logMaxObjectLen;
    this.logRetainOriginalLine = retainOriginalLine;
    this.customLogFormat = customLogFormat;
    this.regex = regex;
    this.fieldPathsToGroupName = fieldPathsToGroupName;
    this.grokPatternDefinition = grokPatternDefinition;
    this.grokPattern = grokPattern;
    this.enableLog4jCustomLogFormat = enableLog4jCustomLogFormat;
    this.log4jCustomLogFormat = log4jCustomLogFormat;
    this.onParseError = onParseError;
    this.maxStackTraceLines = maxStackTraceLines;
  }

  private Charset fileCharset;
  private DirectorySpooler spooler;
  private File currentFile;
  private DataParserFactory parserFactory;
  private DataParser parser;
  private LogDataFormatValidator logDataFormatValidator;

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();

    validateDir(spoolDir, Groups.FILES.name(), "spoolDir", issues);

    if (overrunLimit < MIN_OVERRUN_LIMIT || overrunLimit >= MAX_OVERRUN_LIMIT) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "overrunLimit", Errors.SPOOLDIR_06));
    }

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
      if (archiveDir != null && !archiveDir.isEmpty()) {
        validateDir(archiveDir, Groups.POST_PROCESSING.name(), "archiveDir", issues);
      } else {
        issues.add(getContext().createConfigIssue(Groups.POST_PROCESSING.name(), "archiveDir", Errors.SPOOLDIR_11));
      }
      if (retentionTimeMins < 1) {
        issues.add(getContext().createConfigIssue(Groups.POST_PROCESSING.name(), "retentionTimeMins", Errors.SPOOLDIR_19));
      }
    }


    switch (dataFormat) {
      case JSON:
        if (jsonMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.JSON.name(), "jsonMaxObjectLen", Errors.SPOOLDIR_20));
        }
        break;
      case TEXT:
        if (textMaxLineLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.TEXT.name(), "textMaxLineLen", Errors.SPOOLDIR_20));
        }
        break;
      case DELIMITED:
        if (csvMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.DELIMITED.name(), "csvMaxObjectLen", Errors.SPOOLDIR_20));
        }
        break;
      case XML:
        if (xmlMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "xmlMaxObjectLen", Errors.SPOOLDIR_20));
        }
        if (xmlRecordElement == null || xmlRecordElement.isEmpty()) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "xmlRecordElement", Errors.SPOOLDIR_26));
        } else if (!XMLChar.isValidName(xmlRecordElement)) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "xmlRecordElement", Errors.SPOOLDIR_23,
                                                    xmlRecordElement));
        }
        break;
      case SDC_JSON:
        break;
      case LOG:
        logDataFormatValidator = new LogDataFormatValidator(logMode, logMaxObjectLen,
          logRetainOriginalLine, customLogFormat, regex, grokPatternDefinition, grokPattern,
          enableLog4jCustomLogFormat, log4jCustomLogFormat, onParseError, maxStackTraceLines, Groups.LOG.name(),
          getFieldPathToGroupMap(fieldPathsToGroupName));
        logDataFormatValidator.validateLogFormatConfig(issues, getContext());
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.FILES.name(), "dataFormat", Errors.SPOOLDIR_10,
                                                  dataFormat));
        break;
    }

    validateDataParser(issues);

    return issues;
  }

  private void validateDir(String dir, String group, String config, List<ConfigIssue> issues) {
    if (dir.isEmpty()) {
      issues.add(getContext().createConfigIssue(group, config, Errors.SPOOLDIR_11));
    }
    File fDir = new File(dir);
    if (!fDir.exists()) {
      issues.add(getContext().createConfigIssue(group, config, Errors.SPOOLDIR_12, dir));
    } else {
      if (!fDir.isDirectory()) {
        issues.add(getContext().createConfigIssue(group, config, Errors.SPOOLDIR_13, dir));
      }
    }
  }

  private void validateFilePattern(List<ConfigIssue> issues) {
    if(filePattern == null || filePattern.trim().isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "filePattern", Errors.SPOOLDIR_32, filePattern));
    } else {
      try {
        DirectorySpooler.createPathMatcher(filePattern);
      } catch (Exception ex) {
        issues.add(getContext().createConfigIssue(Groups.FILES.name(), "filePattern", Errors.SPOOLDIR_16, filePattern,
          ex.getMessage(), ex));
      }
    }
  }

  private void validateInitialFileToProcess(List<ConfigIssue> issues) {
    if (initialFileToProcess != null && !initialFileToProcess.isEmpty()) {
      try {
        PathMatcher pathMatcher = DirectorySpooler.createPathMatcher(filePattern);
        if (!pathMatcher.matches(new File(initialFileToProcess).toPath())) {
          issues.add(getContext().createConfigIssue(Groups.FILES.name(), "initialFileToProcess", Errors.SPOOLDIR_18,
                                                    initialFileToProcess, filePattern));
        }
      } catch (Exception ex) {
      }
    }
  }

  private void validateDataParser(List<ConfigIssue> issues) {
    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(getContext(), dataFormat.getParserFormat());


    try {
      fileCharset = Charset.forName(charset);
    } catch (UnsupportedCharsetException ex) {
      // setting it to a valid one so the parser factory can be configured and tested for more errors
      fileCharset = Charset.forName("UTF-8");
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "charset", Errors.SPOOLDIR_00, charset));
    }
    builder.setCharset(fileCharset);
    builder.setOverRunLimit(overrunLimit);

    switch (dataFormat) {
      case TEXT:
        builder.setMaxDataLen(textMaxLineLen);
        break;
      case JSON:
        builder.setMaxDataLen(jsonMaxObjectLen).setMode(jsonContent);
        break;
      case DELIMITED:
        builder.setMaxDataLen(csvMaxObjectLen).setMode(csvFileFormat).setMode(csvHeader);
        break;
      case XML:
        builder.setMaxDataLen(xmlMaxObjectLen).setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, xmlRecordElement);
        break;
      case SDC_JSON:
        builder.setMaxDataLen(-1);
        initialFileToProcess = "";
        maxSpoolFiles = 10000;
        break;
      case LOG:
        logDataFormatValidator.populateBuilder(builder);
        break;
    }
    try {
      parserFactory = builder.build();
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(null, null, Errors.SPOOLDIR_24, ex.getMessage(), ex));
    }
  }

  private Map<String, Integer> getFieldPathToGroupMap(List<RegExConfig> fieldPathsToGroupName) {
    if(fieldPathsToGroupName == null) {
      return new HashMap<>();
    }
    Map<String, Integer> fieldPathToGroup = new HashMap<>();
    for(RegExConfig r : fieldPathsToGroupName) {
      fieldPathToGroup.put(r.fieldPath, r.group);
    }
    return fieldPathToGroup;
  }

  @Override
  protected void init() throws StageException {
    super.init();

    if (getContext().isPreview()) {
      poolingTimeoutSecs = 1;
    }

    DirectorySpooler.Builder builder = DirectorySpooler.builder().setDir(spoolDir).setFilePattern(filePattern).
        setMaxSpoolFiles(maxSpoolFiles).
        setPostProcessing(DirectorySpooler.FilePostProcessing.valueOf(postProcessing.name()));
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
          LOG.debug("No new file available in spool directory after '{}' secs, producing empty batch",
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
    String sourceFile = file.getName();
    try {
      if (parser == null) {
        parser = parserFactory.getParser(file, offset);
      }
      for (int i = 0; i < maxBatchSize; i++) {
        try {
          Record record = parser.parse();
          if (record != null) {
            batchMaker.addRecord(record);
            offset = parser.getOffset();
          } else {
            parser.close();
            parser = null;
            offset = -1;
            break;
          }
        } catch (ObjectLengthException ex) {
          long exOffset = offset;
          offset = -1;
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              getContext().reportError(Errors.SPOOLDIR_02, sourceFile, exOffset);
              break;
            case STOP_PIPELINE:
              throw new StageException(Errors.SPOOLDIR_02, sourceFile, exOffset);
            default:
              throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                                                           getContext().getOnErrorRecord(), ex));
          }
        }
      }
    } catch (IOException ex) {
      offset = -1;
      long exOffset;
      if (ex instanceof OverrunException) {
        exOffset = ((OverrunException)ex).getStreamOffset();
      } else {
        exOffset = (parser != null) ? parser.getOffset() : -1;
      }
      switch (getContext().getOnErrorRecord()) {
        case DISCARD:
          break;
        case TO_ERROR:
          throw new BadSpoolFileException(file.getAbsolutePath(), exOffset, ex);
        case STOP_PIPELINE:
          getContext().reportError(Errors.SPOOLDIR_04, sourceFile, exOffset, ex.getMessage());
          throw new StageException(Errors.SPOOLDIR_04, sourceFile, exOffset, ex.getMessage());
        default:
          throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                                                       getContext().getOnErrorRecord(), ex));
      }
    } finally {
      if (offset == -1) {
        if (parser != null) {
          try {
            parser.close();
            parser = null;
          } catch (IOException ex) {
            //NOP
          }
        }
      }
    }
    return offset;
  }

}
