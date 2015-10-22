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
package com.streamsets.pipeline.stage.devtest.rawdata;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.pipeline.stage.origin.lib.DataFormatParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RawDataSource extends BaseSource  {
  private static final Logger LOG = LoggerFactory.getLogger(RawDataSource.class);
  private final DataFormatParser parser;
  private final String rawData;

  public RawDataSource(DataFormat dataFormat, DataParserFormatConfig dataFormatConfig, String rawData) {
    this.rawData = rawData;
    this.parser = new DataFormatParser(RawDataSourceGroups.RAW.name(), dataFormat, dataFormatConfig, null);
  }

  @Override
  public List<Stage.ConfigIssue> init() {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    issues.addAll(parser.init(this.getContext()));
    return issues;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    List<Record> records = parser.parse(this.getContext(), "rawData", rawData.getBytes());
    for(Record record: records) {
      batchMaker.addRecord(record);
    }
    return "rawData";
  }

}
