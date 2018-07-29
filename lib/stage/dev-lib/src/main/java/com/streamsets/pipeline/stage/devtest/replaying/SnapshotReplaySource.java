/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.devtest.replaying;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SnapshotReplaySource extends BaseSource  {
  private final String snapshotInputFilePath;
  private InputStream snapshotInputStream;
  private final String stageInstanceName;
  private final ObjectMapper objectMapper;

  private final List<Record> allRecords = new LinkedList<>();

  private int recordIndex = 0;

  //10100000
  static final byte BASE_MAGIC_NUMBER = (byte) 0xa0;
  //10100001
  static final byte JSON1_MAGIC_NUMBER = BASE_MAGIC_NUMBER | (byte) 0x01;

  public SnapshotReplaySource(
      String snapshotFilePath,
      String stageInstanceName
  ) {
    this(snapshotFilePath, null, stageInstanceName);
  }

  public SnapshotReplaySource(
      String snapshotInputFilePath,
      InputStream snapshotInputStream,
      String stageInstanceName
  ) {
    this.snapshotInputFilePath = snapshotInputFilePath;
    this.snapshotInputStream = snapshotInputStream;
    this.stageInstanceName = stageInstanceName;


    objectMapper = new ObjectMapper();
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = new ArrayList<>();

    if (snapshotInputStream == null && snapshotInputFilePath != null && !snapshotInputFilePath.isEmpty()) {
      Path path = Paths.get(snapshotInputFilePath);
      if (!Files.exists(path)) {
        issues.add(getContext().createConfigIssue(
            SnapshotReplaySourceGroups.REPLAY.getLabel(),
            "snapshotFilePath",
            Errors.REPLAY_04,
            snapshotInputFilePath
        ));
      }

      try {
        snapshotInputStream = new FileInputStream(snapshotInputFilePath);
      } catch (FileNotFoundException e) {
        issues.add(getContext().createConfigIssue(
            SnapshotReplaySourceGroups.REPLAY.getLabel(),
            "snapshotFilePath",
            Errors.REPLAY_03,
            snapshotInputFilePath,
            e.getMessage(),
            e
        ));
      }
    }

    if (issues.isEmpty()) {
      try {
        //JsonMapper jsonMapper = DataCollectorServices.instance().get(JsonMapper.SERVICE_KEY);
        final Map<String, List<List<Map<String, ?>>>> snapshotData = objectMapper.readValue(snapshotInputStream, Map.class);

        final List<Map<String, ?>> snapshotBatches = snapshotData.get("snapshotBatches").get(0);

        boolean foundStage = false;
        Iterator<Map<String, ?>> snapshotBatchIter = snapshotBatches.iterator();
        while (!foundStage && snapshotBatchIter.hasNext()) {
          Map<String, ?> snapshotBatch = snapshotBatchIter.next();
          if (stageInstanceName.isEmpty() || snapshotBatch.get("instanceName").equals(stageInstanceName)) {
            Map<String, List<Map<?, ?>>> outputRecords = (Map<String, List<Map<?, ?>>>) snapshotBatch.get("output");
            List<Map<?, ?>> records = outputRecords.values().iterator().next();
            for (Map<? ,?> record : records) {

              final StringWriter sw = new StringWriter();
              final ByteArrayOutputStream baos = new ByteArrayOutputStream();
              objectMapper.writer().writeValue(baos, record);
              final byte[] inputBytes = new byte[baos.size() + 1];
              inputBytes[0] = JSON1_MAGIC_NUMBER;
              System.arraycopy(baos.toByteArray(), 0, inputBytes, 1, baos.size());
              RecordReader recordReader = ((ContextExtensions) getContext()).createRecordReader(
                  new ByteArrayInputStream(inputBytes),
                  0L,
                  Integer.MAX_VALUE
              );
              Record recordObj = recordReader.readRecord();
              allRecords.add(recordObj);
            }
            break;
          }
        }
      } catch (IOException e) {
        issues.add(getContext().createConfigIssue(
            SnapshotReplaySourceGroups.REPLAY.getLabel(),
            "snapshotFilePath",
            Errors.REPLAY_03,
            e.getMessage(),
            e
        ));
      }
    }

    return issues;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int numProduced = 0;
    while (numProduced++ < maxBatchSize) {
      batchMaker.addRecord(allRecords.get(recordIndex++));
      if (recordIndex > allRecords.size() - 1) {
        // wrap around
        recordIndex = 0;
      }
    }

    return String.valueOf(recordIndex);
  }

}
