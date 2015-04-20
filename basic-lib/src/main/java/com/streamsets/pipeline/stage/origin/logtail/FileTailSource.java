/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.lib.io.FileLine;
import com.streamsets.pipeline.lib.io.LiveDirectoryScanner;
import com.streamsets.pipeline.lib.io.LiveFile;
import com.streamsets.pipeline.lib.io.LiveFileChunk;
import com.streamsets.pipeline.lib.io.LiveFileReader;
import com.streamsets.pipeline.lib.io.RollMode;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.lib.util.ThreadUtil;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileTailSource extends BaseSource {

  private final static long MAX_YIELD_TIME = Integer.parseInt(System.getProperty("FileTailSource.yield.ms", "500"));

  private final DataFormat dataFormat;
  private final String charset;
  private final String dirName;
  private final String fileName;
  private final String firstRolledFile;
  private final int maxLineLength;
  private final FilesRollMode filesRollMode;
  private final String periodicFileRegEx;
  private final int batchSize;
  private final int maxWaitTimeSecs;
  private final LogMode logMode;
  private final boolean logRetainOriginalLine;
  private final String customLogFormat;
  private final String regex;
  private final String grokPatternDefinition;
  private final String grokPattern;
  private final List<RegExConfig> fieldPathsToGroupName;
  private final boolean enableLog4jCustomLogFormat;
  private final String log4jCustomLogFormat;

  public FileTailSource(DataFormat dataFormat, String charset, String dirName, String fileName, String firstRolledFile,
      FilesRollMode filesRollMode, String periodicFileRegEx, int maxLineLength, int batchSize, int maxWaitTimeSecs,
      LogMode logMode,
      boolean retainOriginalLine, String customLogFormat, String regex,
      List<RegExConfig> fieldPathsToGroupName,
      String grokPatternDefinition, String grokPattern, boolean enableLog4jCustomLogFormat,
      String log4jCustomLogFormat) {
    this.dataFormat = dataFormat;
    this.charset = charset;
    this.dirName = dirName;
    this.fileName = fileName;
    this.firstRolledFile = firstRolledFile;
    this.filesRollMode = filesRollMode;
    this.periodicFileRegEx = periodicFileRegEx;
    this.maxLineLength = maxLineLength;
    this.batchSize = batchSize;
    this.maxWaitTimeSecs = maxWaitTimeSecs;
    this.logMode = logMode;
    this.logRetainOriginalLine = retainOriginalLine;
    this.customLogFormat = customLogFormat;
    this.regex = regex;
    this.fieldPathsToGroupName = fieldPathsToGroupName;
    this.grokPatternDefinition = grokPatternDefinition;
    this.grokPattern = grokPattern;
    this.enableLog4jCustomLogFormat = enableLog4jCustomLogFormat;
    this.log4jCustomLogFormat = log4jCustomLogFormat;
  }

  private LogDataFormatValidator logDataFormatValidator;

  private long maxWaitTimeMillis;

  private RollMode rollMode;
  private LiveDirectoryScanner scanner;
  private LiveFile currentFile;
  private LiveFileReader reader;

  private CharDataParserFactory parserFactory;

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();
    File dir = new File(dirName);
    if (!dir.exists()) {
      issues.add(getContext().createConfigIssue(Groups.FILE.name(), "dirName", Errors.TAIL_00, dir));
    } else if (!dir.isDirectory()) {
      issues.add(getContext().createConfigIssue(Groups.FILE.name(), "dirName", Errors.TAIL_01, dir));
    }
    rollMode = filesRollMode.createRollMode(periodicFileRegEx);
    try {
      scanner = new LiveDirectoryScanner(dirName, fileName, firstRolledFile, rollMode);
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(Groups.FILE.name(), "", Errors.TAIL_02, ex.getMessage()));
    }
    switch (dataFormat) {
      case TEXT:
      case JSON:
        break;
      case LOG:
        logDataFormatValidator = new LogDataFormatValidator(logMode, maxLineLength,
          logRetainOriginalLine, customLogFormat, regex, grokPatternDefinition, grokPattern,
          enableLog4jCustomLogFormat, log4jCustomLogFormat,
          OnParseError.ERROR, 0, Groups.LOG.name(), getFieldPathToGroupMap(fieldPathsToGroupName));
        logDataFormatValidator.validateLogFormatConfig(issues, getContext());
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.FILE.name(), "dataFormat", Errors.TAIL_03, dataFormat,
                                                  Arrays.asList(DataFormat.TEXT, DataFormat.JSON)));
    }
    return issues;
  }

  @Override
  protected void init() throws StageException {
    super.init();

    maxWaitTimeMillis = maxWaitTimeSecs * 1000;

    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(getContext(), dataFormat.getParserFormat())
        .setCharset(Charset.defaultCharset()).setMaxDataLen(-1);
    switch (dataFormat) {
      case TEXT:
        break;
      case JSON:
        builder.setMode(JsonMode.MULTIPLE_OBJECTS);
        break;
      case LOG:
        logDataFormatValidator.populateBuilder(builder);
        break;
      default:
        throw new StageException(Errors.TAIL_02, "dataFormat", dataFormat);
    }
    parserFactory = builder.build();
  }

  @Override
  public void destroy() {
    super.destroy();
  }

  //extract and deserialize the LiveFile from the offset kept by the SDC
  private LiveFile getLiveFile(String sourceOffset) throws StageException{
    LiveFile file = null;
    if (sourceOffset != null && !sourceOffset.isEmpty()) {
      String[] split = sourceOffset.split("::", 2);
      try {
        file = LiveFile.deserialize(split[1]);
      } catch (IOException ex) {
        throw new StageException(Errors.TAIL_05, sourceOffset, ex.getMessage(), ex);
      }
    }
    return file;
  }

  //extract and deserialize the offset in file from the offset kept by the SDC
  private long getFileOffset(String sourceOffset) throws StageException{
    long fileOffset = 0;
    if (sourceOffset != null && !sourceOffset.isEmpty()) {
      String[] split = sourceOffset.split("::", 2);
      try {
        fileOffset = Long.parseLong(split[0]);
      } catch (NumberFormatException ex) {
        throw new StageException(Errors.TAIL_05, sourceOffset, ex.getMessage(), ex);
      }
    }
    return fileOffset;
  }

  //encode LiveFile and offset to give to SDC
  private String createSourceOffset(LiveFile file, long offset) {
    return offset + "::" + file.serialize();
  }

  // if we are in batch timeout
  private boolean isTimeout(long startTime) {
    return (System.currentTimeMillis() - startTime) > maxWaitTimeMillis;
  }

  // remaining time till batch timeout, return zero if already in timeout
  private long getRemainingWaitTime(long startTime) {
    long remaining = maxWaitTimeMillis - (System.currentTimeMillis() - startTime);
    return (remaining > 0) ? remaining : 0;
  }

  // obtains the reader if available.
  // TODO: this is in preparation for tailing multiple files concurrently
  private LiveFileReader obtainReader(String lastSourceOffset) throws StageException {
    if (reader == null) {
      currentFile = getLiveFile(lastSourceOffset);
      long fileOffset = getFileOffset(lastSourceOffset);

      boolean needsToScan = currentFile == null || fileOffset == Long.MAX_VALUE;
      if (needsToScan) {
        try {
          if (currentFile != null) {
            // we need to refresh the file in case the name changed before scanning as the scanner does not refresh
            currentFile = currentFile.refresh();
          }
          currentFile = scanner.scan(currentFile);
          fileOffset = 0;
        } catch (IOException ex) {
          throw new StageException(Errors.TAIL_06, ex.getMessage(), ex);
        }
      }
      if (currentFile != null) {
        try {
          reader = new LiveFileReader(rollMode, currentFile, Charset.forName(charset), fileOffset, maxLineLength);
        } catch (IOException ex) {
          throw new StageException(Errors.TAIL_07, currentFile.getPath(), ex.getMessage(), ex);
        }
      }
    }
    return reader;
  }

  // returns the reader
  // TODO: this is in preparation for tailing multiple files concurrently
  private String releaseReader(LiveFileReader reader) throws IOException {
    String newSourceOffset;
    if (!reader.hasNext()) {
      // reached EOF
      reader.close();
      this.reader = null;
      //using Long.MAX_VALUE to signal we reach the end of the file and next iteration should get the next file.
      newSourceOffset = createSourceOffset(currentFile, Long.MAX_VALUE);
    } else {
      newSourceOffset = createSourceOffset(currentFile, reader.getOffset());
    }
    return newSourceOffset;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int recordCounter = 0;
    long batchStartTime = System.currentTimeMillis();
    maxBatchSize = Math.min(batchSize, maxBatchSize);

    boolean exit = false;
    while (!exit) {
      LiveFileReader reader = obtainReader(lastSourceOffset);
      if (reader != null) {
        try {
          // even if in timeout, we do one non-blocking read on the reader
          while (!exit && recordCounter < maxBatchSize && reader.hasNext()) {
            // non blocking read waiting up to the remaining batch timeout if not data arrives
            LiveFileChunk chunk = reader.next(getRemainingWaitTime(batchStartTime));
            if (chunk != null) {
              String liveFileStr = currentFile.serialize();
              for (FileLine line : chunk.getLines()) {
                String sourceId = liveFileStr + "::" + line.getFileOffset();
                try (DataParser parser = parserFactory.getParser(sourceId, line.getChunkBuffer(), line.getOffset(),
                                                                 line.getLength())) {
                  Record record = parser.parse();
                  if (record != null) {
                    batchMaker.addRecord(record);
                    recordCounter++;
                  }
                } catch (IOException|DataParserException ex) {
                  handleException(sourceId, ex);
                }
              }
            }
            exit = isTimeout(batchStartTime);
          }
          lastSourceOffset = releaseReader(reader);
          exit = true;
        } catch (IOException ex) {
          throw new StageException(Errors.TAIL_08, reader.getLiveFile().getPath(), ex.getMessage(), ex);
        }
      } else {
        // yielding CPU before looking to see if there is a reader again. MAX_YIELD_TIME default is 500 ms
        // if we didn't sleep the requested time it means we were interrupted.
        exit = !ThreadUtil.sleep(Math.min(getRemainingWaitTime(batchStartTime), MAX_YIELD_TIME));
      }
      exit = exit || isTimeout(batchStartTime);
    }
    // we are returning "" instead of NULL because if the dir is empty when starting there will be no offset
    // until there is a file and that will trigger a pipeline stop.
    return (lastSourceOffset == null) ? "" : lastSourceOffset;
  }

  private void handleException(String sourceId, Exception ex) throws StageException {
    switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        getContext().reportError(Errors.TAIL_04, sourceId, ex.getMessage(), ex);
        break;
      case STOP_PIPELINE:
        if (ex instanceof StageException) {
          throw (StageException) ex;
        } else {
          throw new StageException(Errors.TAIL_04, sourceId, ex.getMessage(), ex);
        }
      default:
        throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
          getContext().getOnErrorRecord(), ex));
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
}
