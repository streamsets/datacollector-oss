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
package com.streamsets.pipeline.stage.processor.parser;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.stage.origin.lib.DataFormatParser;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

public class DataParserProcessor extends SingleLaneRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(DataParserProcessor.class);

  private final DataParserConfig configs;
  private final DataFormatParser parser;

  public DataParserProcessor(DataParserConfig configs) {
    this.configs = configs;
    this.parser = new DataFormatParser(Groups.PARSER.name(), configs.dataFormat, configs.dataFormatConfig, null);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    issues.addAll(parser.init(getContext(), "configs."));
    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Field field = record.get(configs.fieldPathToParse);
    final String typeName = configs.dataFormat.name();
    if (field != null) {
      try {
        final String parserId = String.format(
            "%s_%s_%s_%s",
            getContext().getStageInfo().getInstanceName(),
            typeName,
            record.getHeader().getSourceId(),
            configs.fieldPathToParse
        );
        byte[] fieldData;
        switch (field.getType()) {
          case STRING:
            try {
              fieldData = field.getValueAsString().getBytes(configs.dataFormatConfig.charset);
            } catch (UnsupportedEncodingException e) {
              throw new OnRecordErrorException(
                  Errors.DATAPARSER_03,
                  e.getClass().getSimpleName(),
                  configs.fieldPathToParse,
                  typeName,
                  record.getHeader().getSourceId(),
                  e.toString(),
                  e
              );
            }
            break;
          case BYTE_ARRAY:
            fieldData = field.getValueAsByteArray();
            break;
          case FILE_REF:
            try {
              final InputStream inputStream = field.getValueAsFileRef().createInputStream(getContext(), InputStream.class);
              fieldData = IOUtils.toByteArray(inputStream);
            } catch (IOException e) {
              throw new OnRecordErrorException(
                  record,
                  Errors.DATAPARSER_04,
                  configs.fieldPathToParse,
                  record.getHeader().getSourceId(),
                  e.getMessage(),
                  e
              );
            }
            break;
          default:
            throw new OnRecordErrorException(
                Errors.DATAPARSER_02,
                configs.fieldPathToParse,
                typeName,
                record.getHeader().getSourceId(),
                field.getType().name()
            );

        }
        List<Record> parsedRecords = parser.parse(getContext(), parserId, fieldData);
        if (parsedRecords == null || parsedRecords.isEmpty()) {
          LOG.warn(
              "No records of {} format were parsed from field {} of record {}",
              typeName,
              configs.fieldPathToParse,
              record.getHeader().getSourceId()
          );
          batchMaker.addRecord(record);
          return;
        }
        switch (configs.multipleValuesBehavior) {
          case FIRST_ONLY:
            final Record first = parsedRecords.get(0);
            record.set(
                configs.parsedFieldPath,
                first.get()
            );
            batchMaker.addRecord(record);
            break;
          case ALL_AS_LIST:
            List<Field> multipleFieldValues = new LinkedList<>();
            parsedRecords.forEach(parsedRecord -> multipleFieldValues.add(parsedRecord.get()));
            record.set(configs.parsedFieldPath, Field.create(multipleFieldValues));
            batchMaker.addRecord(record);
            break;
          case SPLIT_INTO_MULTIPLE_RECORDS:
            final String recordIdSuffix = configs.fieldPathToParse.replaceFirst("^/", "").replaceAll("/", "_");
            IntStream.range(0, parsedRecords.size()).forEach(idx -> {
              Record parsedRecord = parsedRecords.get(idx);
              Record splitRecord = getContext().cloneRecord(
                  record,
                  String.format("%s_%d", recordIdSuffix, idx)
              );
              splitRecord.set(configs.parsedFieldPath, parsedRecord.get());
              batchMaker.addRecord(splitRecord);
            });
            break;
        }
      } catch (DataParserException ex) {
        throw new OnRecordErrorException(
            Errors.DATAPARSER_01,
            configs.fieldPathToParse,
            typeName,
            record.getHeader().getSourceId(),
            ex.toString(),
            ex
        );
      }
    } else {
      throw new OnRecordErrorException(
          record,
          Errors.DATAPARSER_05,
          configs.fieldPathToParse,
          record.getHeader().getSourceId(),
          typeName
      );
    }
  }

}
