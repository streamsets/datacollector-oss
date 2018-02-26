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
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.api.service.dataformats.DataParser;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

public class DataParserProcessor extends SingleLaneRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(DataParserProcessor.class);

  private final DataParserConfig configs;

  public DataParserProcessor(DataParserConfig configs) {
    this.configs = configs;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Field field = record.get(configs.fieldPathToParse);
    if (field != null) {
      try {
        final String parserId = String.format(
            "%s_%s_%s",
            getContext().getStageInfo().getInstanceName(),
            record.getHeader().getSourceId(),
            configs.fieldPathToParse
        );
        List<Record> parsedRecords;
        switch (field.getType()) {
          case STRING:
            try(DataParser parser = getContext().getService(DataFormatParserService.class).getParser(parserId, field.getValueAsString())) {
              parsedRecords = parser.parseAll();
            }
            break;
          case BYTE_ARRAY:
            try(DataParser parser = getContext().getService(DataFormatParserService.class).getParser(parserId, field.getValueAsByteArray())) {
              parsedRecords = parser.parseAll();
            }
            break;
          case FILE_REF:
            try {
              final InputStream inputStream = field.getValueAsFileRef().createInputStream(getContext(), InputStream.class);
              byte []fieldData = IOUtils.toByteArray(inputStream);

              try(DataParser parser = getContext().getService(DataFormatParserService.class).getParser(parserId, fieldData)) {
                parsedRecords = parser.parseAll();
              }
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
                record,
                Errors.DATAPARSER_02,
                configs.fieldPathToParse,
                field.getType().name()
            );

        }

        if (parsedRecords == null || parsedRecords.isEmpty()) {
          LOG.warn(
              "No records were parsed from field {} of record {}",
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
      } catch (DataParserException | IOException ex) {
        throw new OnRecordErrorException(
            record,
            Errors.DATAPARSER_01,
            configs.fieldPathToParse,
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
          record.getHeader().getSourceId()
      );
    }
  }

}
