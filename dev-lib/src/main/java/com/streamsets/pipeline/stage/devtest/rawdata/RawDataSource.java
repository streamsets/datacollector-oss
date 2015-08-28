/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.devtest.rawdata;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.stage.origin.lib.DataFormatConfig;
import com.streamsets.pipeline.stage.origin.lib.DataFormatParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RawDataSource extends BaseSource  {
  private static final Logger LOG = LoggerFactory.getLogger(RawDataSource.class);
  private final DataFormatParser parser;
  private final String rawData;

  public RawDataSource(DataFormatConfig dataFormatConfig, String rawData) {
    this.rawData = rawData;
    this.parser = new DataFormatParser(RawDataSourceGroups.RAW.name(), dataFormatConfig, null);
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
