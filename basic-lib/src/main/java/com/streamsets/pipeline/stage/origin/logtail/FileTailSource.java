/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.origin.logtail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.FileRollMode;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.io.FileEvent;
import com.streamsets.pipeline.lib.io.FileFinder;
import com.streamsets.pipeline.lib.io.FileLine;
import com.streamsets.pipeline.lib.io.LiveFile;
import com.streamsets.pipeline.lib.io.LiveFileChunk;
import com.streamsets.pipeline.lib.io.MultiFileInfo;
import com.streamsets.pipeline.lib.io.MultiFileReader;
import com.streamsets.pipeline.lib.io.RollMode;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class FileTailSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(FileTailSource.class);

  private final FileTailConfigBean conf;
  private final int scanIntervalSecs;

  public FileTailSource(FileTailConfigBean conf) {
    this(conf, 20);
  }

  FileTailSource(FileTailConfigBean conf, int scanIntervalSecs) {
    this.conf = conf;
    this.scanIntervalSecs = scanIntervalSecs;
  }

  private MultiFileReader multiDirReader;

  private long maxWaitTimeMillis;

  private DataParserFactory parserFactory;
  private String outputLane;
  private String metadataLane;

  private boolean validateFileInfo(FileInfo fileInfo, List<ConfigIssue> issues) {
    boolean ok = true;
    String fileName = Paths.get(fileInfo.fileFullPath).getFileName().toString();
    String token = fileInfo.fileRollMode.getTokenForPattern();
    if (!token.isEmpty() && !fileName.contains(token)) {
      ok = false;
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "fileInfo", Errors.TAIL_08, fileInfo.fileFullPath,
                                                fileInfo.fileRollMode.getTokenForPattern(), fileName));
    }
    String fileParentDir = Paths.get(fileInfo.fileFullPath).getParent().toString();
    if (!token.isEmpty() && fileParentDir.contains(token)) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "fileInfo", Errors.TAIL_16, fileInfo.fileFullPath,
                                                fileInfo.fileRollMode.getTokenForPattern()));
    }
    if (fileInfo.fileRollMode == FileRollMode.PATTERN) {
      if (fileInfo.patternForToken == null || fileInfo.patternForToken.isEmpty()) {
        ok = false;
        issues.add(getContext().createConfigIssue(Groups.FILES.name(), "fileInfo", Errors.TAIL_08, fileInfo.fileFullPath));
      } else {
        try {
          Pattern.compile(fileInfo.patternForToken);
        } catch (PatternSyntaxException ex) {
          ok = false;
          issues.add(getContext().createConfigIssue(Groups.FILES.name(), "fileInfo", Errors.TAIL_09,
                                                    fileInfo.fileFullPath, fileInfo.patternForToken,
                                                    ex.toString()));
        }
        ELVars elVars = getContext().createELVars();
        elVars.addVariable("PATTERN", "");
        ELEval elEval = getContext().createELEval("fileFullPath");
        try {
          String pathWithoutPattern = elEval.eval(elVars, fileInfo.fileFullPath, String.class);
          if (FileFinder.hasGlobWildcard(pathWithoutPattern)) {
            ok = false;
            issues.add(getContext().createConfigIssue(Groups.FILES.name(), "fileInfo", Errors.TAIL_17,
                                                      fileInfo.fileFullPath));
          }
        } catch (ELEvalException ex) {
          ok = false;
          issues.add(getContext().createConfigIssue(Groups.FILES.name(), "fileInfo", Errors.TAIL_18,
                                                    fileInfo.fileFullPath, ex.toString()));
        }
      }
      if (ok && fileInfo.firstFile != null && !fileInfo.firstFile.isEmpty()) {
        RollMode rollMode = fileInfo.fileRollMode.createRollMode(fileInfo.fileFullPath, fileInfo.patternForToken);
        if (!rollMode.isFirstAcceptable(fileInfo.firstFile)) {
          ok = false;
          issues.add(getContext().createConfigIssue(Groups.FILES.name(), "fileInfo", Errors.TAIL_19,
                                                    fileInfo.fileFullPath));
        }
      }
    }
    return ok;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    if (conf.postProcessing == PostProcessingOptions.ARCHIVE) {
      if (conf.archiveDir == null || conf.archiveDir.isEmpty()) {
        issues.add(getContext().createConfigIssue(Groups.POST_PROCESSING.name(), "archiveDir", Errors.TAIL_05));
      } else {
        File dir = new File(conf.archiveDir);
        if (!dir.exists()) {
          issues.add(getContext().createConfigIssue(Groups.POST_PROCESSING.name(), "archiveDir", Errors.TAIL_06));
        }
        if (!dir.isDirectory()) {
          issues.add(getContext().createConfigIssue(Groups.POST_PROCESSING.name(), "archiveDir", Errors.TAIL_07));
        }
      }
    }
    if (conf.fileInfos.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "fileInfos", Errors.TAIL_01));
    } else {
      Set<String> fileKeys = new LinkedHashSet<>();
      List<MultiFileInfo> dirInfos = new ArrayList<>();
      for (FileInfo fileInfo : conf.fileInfos) {
        if (validateFileInfo(fileInfo, issues)) {
          MultiFileInfo directoryInfo = new MultiFileInfo(
              fileInfo.tag,
              fileInfo.fileFullPath,
              fileInfo.fileRollMode,
              fileInfo.patternForToken,
              fileInfo.firstFile,
              conf.multiLineMainPattern
          );
          dirInfos.add(directoryInfo);
          if (fileKeys.contains(directoryInfo.getFileKey())) {
            issues.add(getContext().createConfigIssue(
                Groups.FILES.name(),
                "fileInfos",
                Errors.TAIL_04,
                fileInfo.fileFullPath
            ));
          }
          fileKeys.add(directoryInfo.getFileKey());
        }
      }
      if (!dirInfos.isEmpty()) {
        try {
          int maxLineLength = Integer.MAX_VALUE;
          if (conf.dataFormat == DataFormat.TEXT) {
            maxLineLength = conf.dataFormatConfig.textMaxLineLen;
          } else if (conf.dataFormat == DataFormat.JSON) {
            maxLineLength = conf.dataFormatConfig.jsonMaxObjectLen;
          } else if (conf.dataFormat == DataFormat.LOG) {
            maxLineLength = conf.dataFormatConfig.logMaxObjectLen;
          }
          int scanIntervalSecs = (getContext().isPreview()) ? 0 : this.scanIntervalSecs;
          multiDirReader = new MultiFileReader(
              dirInfos,
              Charset.forName(conf.dataFormatConfig.charset),
              maxLineLength,
              conf.postProcessing,
              conf.archiveDir,
              true,
              scanIntervalSecs
          );
        } catch (IOException ex) {
          issues.add(getContext().createConfigIssue(Groups.FILES.name(), "fileInfos", Errors.TAIL_02, ex.toString(), ex));
        }
      }
    }

    conf.dataFormatConfig.init(
        getContext(),
        conf.dataFormat,
        Groups.FILES.name(),
        !conf.multiLineMainPattern.isEmpty(),
        issues
    );
    parserFactory = conf.dataFormatConfig.getParserFactory();

    maxWaitTimeMillis = conf.maxWaitTimeSecs * 1000;
    outputLane = getContext().getOutputLanes().get(0);
    metadataLane = getContext().getOutputLanes().get(1);

    return issues;
  }

  @Override
  public void destroy() {
    IOUtils.closeQuietly(multiDirReader);
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
        throw new StageException(Errors.TAIL_10, ex.toString(), ex);
      }
    }
    return map;
  }

  private String serializeOffsetMap(Map<String, String> map) throws StageException {
    try {
      return OBJECT_MAPPER.writeValueAsString(map);
    } catch (IOException ex) {
      throw new StageException(Errors.TAIL_13, ex.toString(), ex);
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
    maxBatchSize = Math.min(conf.batchSize, maxBatchSize);

    // deserializing offsets of all directories
    Map<String, String> offsetMap = deserializeOffsetMap(lastSourceOffset);

    boolean offsetSet = false;
    while (!offsetSet) {
      try {
        multiDirReader.setOffsets(offsetMap);
        offsetSet = true;
      } catch (IOException ex) {
        LOG.warn("Error while creating reading previous offset: {}", ex.toString(), ex);
        multiDirReader.purge();
      }
    }

    while (recordCounter < maxBatchSize && !isTimeout(startTime)) {
      LiveFileChunk chunk = multiDirReader.next(getRemainingWaitTime(startTime));

      if (chunk != null) {
        String tag = chunk.getTag();
        tag = (tag != null && tag.isEmpty()) ? null : tag;
        String liveFileStr = chunk.getFile().serialize();

        List<FileLine> lines = chunk.getLines();
        int truncatedLine = chunk.isTruncated() ? lines.size()-1 : -1;

        for(int i = 0; i < lines.size(); i++) {
          FileLine line = lines.get(i);
          String sourceId = liveFileStr + "::" + line.getFileOffset();
          try (DataParser parser = parserFactory.getParser(sourceId, line.getText())) {
            if(i == truncatedLine) {
              //set truncated
              parser.setTruncated();
            }
            Record record = parser.parse();
            if (record != null) {
              if (tag != null) {
                record.getHeader().setAttribute("tag", tag);
              }
              record.getHeader().setAttribute("file", chunk.getFile().getPath().toString());
              batchMaker.addRecord(record, outputLane);
              recordCounter++;
            }
          } catch (IOException | DataParserException ex) {
            handleException(sourceId, ex);
          }
        }
      }
    }

    boolean metadataGenerationFailure = false;
    Date now = new Date(startTime);
    for (FileEvent event : multiDirReader.getEvents()) {
      try {
        LiveFile file = event.getFile().refresh();
        Record metadataRecord = getContext().createRecord("");
        Map<String, Field> map = new HashMap<>();
        map.put("fileName", Field.create(file.getPath().toString()));
        map.put("inode", Field.create(file.getINode()));
        map.put("time", Field.createDate(now));
        map.put("event", Field.create((event.getAction().name())));
        metadataRecord.set(Field.create(map));
        batchMaker.addRecord(metadataRecord, metadataLane);
      } catch (IOException ex) {
        LOG.warn("Error while creating metadata records: {}", ex.toString(), ex);
        metadataGenerationFailure = true;
      }
    }
    if (metadataGenerationFailure) {
      multiDirReader.purge();
    }

    boolean offsetExtracted = false;
    while (!offsetExtracted) {
      try {
        offsetMap = multiDirReader.getOffsets();
        offsetExtracted = true;
      } catch (IOException ex) {
        LOG.warn("Error while creating creating new offset: {}", ex.toString(), ex);
        multiDirReader.purge();
      }
    }

    // serializing offsets of all directories
    return serializeOffsetMap(offsetMap);
  }

  private void handleException(String sourceId, Exception ex) throws StageException {
    switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        getContext().reportError(Errors.TAIL_12, sourceId, ex.toString(), ex);
        break;
      case STOP_PIPELINE:
        if (ex instanceof StageException) {
          throw (StageException) ex;
        } else {
          throw new StageException(Errors.TAIL_12, sourceId, ex.toString(), ex);
        }
      default:
        throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
          getContext().getOnErrorRecord(), ex));
    }
  }

}
