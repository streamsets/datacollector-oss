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
package com.streamsets.pipeline.stage.processor.xmlparser;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;

import java.io.IOException;
import java.util.LinkedList;
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
        final DataParser parser = parserFactory.getParser("", field.getValueAsString());

        Record xmlRecord = parser.parse();
        switch (configs.multipleValuesBehavior) {
          case FIRST_ONLY:
            record.set(
                configs.parsedFieldPath,
                xmlRecord != null ? xmlRecord.get() : Field.create(field.getType(), null)
            );
            batchMaker.addRecord(record);
            break;
          case ALL_AS_LIST:
            List<Field> multipleFieldValues = new LinkedList<>();
            while (xmlRecord != null) {
              multipleFieldValues.add(xmlRecord.get());
              xmlRecord = parser.parse();
            }
            record.set(configs.parsedFieldPath, Field.create(multipleFieldValues));
            batchMaker.addRecord(record);
            break;
          case SPLIT_INTO_MULTIPLE_RECORDS:
            List<Record> splitRecords = new LinkedList<>();
            while (xmlRecord != null) {
              Record splitRecord = getContext().cloneRecord(record);
              splitRecord.set(configs.parsedFieldPath, xmlRecord.get());
              splitRecords.add(splitRecord);
              xmlRecord = parser.parse();
            }

            for (Record splitRecord : splitRecords) {
              batchMaker.addRecord(splitRecord);
            }

            break;
        }
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
  }

}
