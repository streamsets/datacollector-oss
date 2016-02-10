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
package com.streamsets.pipeline.stage.processor.xmlparser;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;

import java.io.IOException;
import java.util.List;

public class XmlParserProcessor extends SingleLaneRecordProcessor {

  private final XmlParserConfig configs;
  private DataParserFactory parserFactory;

  public XmlParserProcessor(XmlParserConfig configs) {
    this.configs = configs;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    if (configs.init(getContext(), issues)) {
      parserFactory = configs.getParserFactory(getContext());
    }
    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Field field = record.get(configs.fieldPathToParse);
    if (field != null) {
      try {
        Record xmlRecord = parserFactory.getParser("", field.getValueAsString()).parse();
        record.set(configs.parsedFieldPath, xmlRecord != null ? xmlRecord.get() : Field.create(field.getType(), null));
      } catch (IOException|DataParserException ex) {
        throw new OnRecordErrorException(
            Errors.XMLP_01,
            configs.fieldPathToParse,
            record.getHeader().getSourceId(),
            ex.toString(),
            ex
        );
      }
    }
    batchMaker.addRecord(record);
  }

}
