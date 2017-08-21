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
package com.streamsets.pipeline.stage.devtest.rawdata;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.origin.lib.DataFormatParser;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.ArrayList;
import java.util.List;

public class RawDataSource extends BaseSource  {
  private final DataFormatParser parser;
  private final String rawData;
  private final boolean stopAfterFirstBatch;

  public RawDataSource(
      DataFormat dataFormat,
      DataParserFormatConfig dataFormatConfig,
      String rawData,
      boolean stopAfterFirstBatch
  ) {
    this.rawData = rawData;
    this.parser = new DataFormatParser(RawDataSourceGroups.RAW.name(), dataFormat, dataFormatConfig, null);
    this.stopAfterFirstBatch = stopAfterFirstBatch;
  }

  @Override
  public List<Stage.ConfigIssue> init() {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    issues.addAll(parser.init(this.getContext()));
    return issues;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    List<Record> records = parser.parse(this.getContext(), "rawData", rawData.getBytes(parser.getCharset()));
    for(Record record: records) {
      batchMaker.addRecord(record);
    }

    if(stopAfterFirstBatch) {
      return null;
    } else {
      return "rawData";
    }
  }

}
