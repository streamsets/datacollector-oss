/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.streamsets.pipeline.lib.io.LiveFileChunk;
import com.streamsets.pipeline.lib.io.MultiDirectoryReader;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileTailSource extends BaseSource {
  private final DataFormat dataFormat;
  private final String charset;
  private final List<FileInfo> fileInfos;
  private final int maxLineLength;
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

  public FileTailSource(DataFormat dataFormat, String charset, List<FileInfo> fileInfos, int maxLineLength,
      int batchSize, int maxWaitTimeSecs,
      LogMode logMode,
      boolean retainOriginalLine, String customLogFormat, String regex,
      List<RegExConfig> fieldPathsToGroupName,
      String grokPatternDefinition, String grokPattern, boolean enableLog4jCustomLogFormat,
      String log4jCustomLogFormat) {
    this.dataFormat = dataFormat;
    this.charset = charset;
    this.fileInfos = fileInfos;
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

  private MultiDirectoryReader multiDirReader;

  private LogDataFormatValidator logDataFormatValidator;

  private long maxWaitTimeMillis;

  private DataParserFactory parserFactory;

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();
    if (fileInfos.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.FILE.name(), "fileInfos", Errors.TAIL_01));
    } else {
      Set<String> dirNames = new LinkedHashSet<>();
      List<MultiDirectoryReader.DirectoryInfo> dirInfos = new ArrayList<>();
      for (FileInfo fileInfo : fileInfos) {
        dirInfos.add(new MultiDirectoryReader.DirectoryInfo(fileInfo.dirName,
                                                            fileInfo.fileRollMode.createRollMode(fileInfo.periodicFileRegEx),
                                                            fileInfo.file, fileInfo.firstFile));
        if (dirNames.contains(fileInfo.dirName)) {
          issues.add(getContext().createConfigIssue(Groups.FILE.name(), "fileInfos", Errors.TAIL_04, fileInfo.dirName));
        }
        dirNames.add(fileInfo.dirName);
      }
      try {
        multiDirReader = new MultiDirectoryReader(dirInfos, Charset.forName(charset), maxLineLength);
      } catch (IOException ex) {
        issues.add(getContext().createConfigIssue(Groups.FILE.name(), "fileInfos", Errors.TAIL_02, ex.getMessage(), ex));
      }
    }
    switch (dataFormat) {
      case TEXT:
      case JSON:
        break;
      case LOG:
        logDataFormatValidator = new LogDataFormatValidator(logMode, maxLineLength, logRetainOriginalLine,
                                                            customLogFormat, regex, grokPatternDefinition, grokPattern,
                                                            enableLog4jCustomLogFormat, log4jCustomLogFormat,
                                                            OnParseError.ERROR, 0, Groups.LOG.name(),
                                                            getFieldPathToGroupMap(fieldPathsToGroupName));
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
        throw new StageException(Errors.TAIL_03, "dataFormat", dataFormat);
    }
    parserFactory = builder.build();
  }

  @Override
  public void destroy() {
    super.destroy();
  }


  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @SuppressWarnings("unchecked")
  private Map<String, String> deserializeOffsetMap(String lastSourceOffset) throws StageException {
    Map<String, String> map;
    if (lastSourceOffset == null) {
      map = new HashMap<>();
    } else {
      try {
        map = OBJECT_MAPPER.readValue(lastSourceOffset, Map.class);
      } catch (IOException ex) {
        throw new StageException(Errors.TAIL_10, ex.getMessage(), ex);
      }
    }
    return map;
  }

  private String serializeOffsetMap(Map<String, String> map) throws StageException {
    try {
      return OBJECT_MAPPER.writeValueAsString(map);
    } catch (IOException ex) {
      throw new StageException(Errors.TAIL_13, ex.getMessage(), ex);
    }
  }

  // if we are in timeout
  private boolean isTimeout(long startTime) {
    return (System.currentTimeMillis() - startTime) > maxWaitTimeMillis;
  }

  // remaining time till  timeout, return zero if already in timeout
  private long getRemainingWaitTime(long startTime) {
    long remaining = maxWaitTimeMillis - (System.currentTimeMillis() - startTime);
    return (remaining > 0) ? remaining : 0;
  }

  /*
    When we start with a file (empty or not) the file offset is zero.
    If the file is a rolled file, the file will be EOF immediately triggering a close of the reader and setting the
    offset to Long.MAX_VALUE (this happens in the MultiDirectoryReader class). This is the signal that in the next
    read a directory scan should be triggered to get the next rolled file or the live file if we were scanning the last
    rolled file.
    If the file you are starting is the live file, we don't get an EOF as we expect data to be appended. We just return
    null chunks while there is no data. If the file is rolled we'll detect that and then do what is described in the
    previous paragraph.

   When offset for  file is "" it means we never processed things in the directory, at that point we start from the
   first file (according to the defined order) in the directory, or if a 'first file' as been set in the configuration,
   we start from that file.

   We encode in lastSourceOffset the current file and offset from all directories in JSON.
  */
  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int recordCounter = 0;
    long startTime = System.currentTimeMillis();
    maxBatchSize = Math.min(batchSize, maxBatchSize);

    // deserializing offsets of all directories
    Map<String, String> offsetMap = deserializeOffsetMap(lastSourceOffset);

    try {
      multiDirReader.setOffsets(offsetMap);
    } catch (IOException ex) {
      throw new StageException(Errors.TAIL_10, ex.getMessage(), ex);
    }


    while (recordCounter < maxBatchSize && !isTimeout(startTime)) {
      try {
        LiveFileChunk chunk = multiDirReader.next(getRemainingWaitTime(startTime));

        if (chunk != null) {
          String liveFileStr = chunk.getFile().serialize();
          for (FileLine line : chunk.getLines()) {
            String sourceId = liveFileStr + "::" + line.getFileOffset();
            try (DataParser parser = parserFactory.getParser(sourceId, line.getChunkBuffer(), line.getOffset(),
                                                             line.getLength())) {
              Record record = parser.parse();
              if (record != null) {
                batchMaker.addRecord(record);
                recordCounter++;
              }
            } catch (IOException | DataParserException ex) {
              handleException(sourceId, ex);
            }
          }
        }
      } catch (IOException ex) {
        throw new StageException(Errors.TAIL_11, ex.getMessage(), ex);
      }
    }

    try {
      offsetMap = multiDirReader.getOffsets();
    } catch (IOException ex) {
      throw new StageException(Errors.TAIL_13, ex.getMessage(), ex);
    }

    // serializing offsets of all directories
    return serializeOffsetMap(offsetMap);
  }

  private void handleException(String sourceId, Exception ex) throws StageException {
    switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        getContext().reportError(Errors.TAIL_12, sourceId, ex.getMessage(), ex);
        break;
      case STOP_PIPELINE:
        if (ex instanceof StageException) {
          throw (StageException) ex;
        } else {
          throw new StageException(Errors.TAIL_12, sourceId, ex.getMessage(), ex);
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
