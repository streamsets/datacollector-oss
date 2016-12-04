/**
 * Copyright 2016 StreamSets Inc.
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

package com.streamsets.pipeline.stage.destination.datalake.writer;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class RecordWriter {
  private final static Logger LOG = LoggerFactory.getLogger(RecordWriter.class);

  private Map<String, DataGenerator> generators;
  private ADLStoreClient client;
  private DataFormat dataFormat;
  private DataGeneratorFormatConfig dataFormatConfig;
  private String uniquePrefix;
  private String fileNameEL;
  private TimeZone timeZone;
  private ELEval dirPathTemplateEval;
  private ELVars dirPathTemplateVars;
  private WholeFileExistsAction wholeFileExistsAction;
  private OutputStreamHelper outputStreamHelper;

  public RecordWriter(
      ADLStoreClient client,
      DataFormat dataFormat,
      DataGeneratorFormatConfig dataFormatConfig,
      String uniquePrefix,
      String fileNameEL,
      ELEval dirPathTemplateEval,
      ELVars dirPathTemplateVars,
      String timeZoneID,
      WholeFileExistsAction wholeFileExistsAction
  ) {
    this.client = client;
    this.dataFormat = dataFormat;
    this.dataFormatConfig = dataFormatConfig;
    this.uniquePrefix = uniquePrefix;
    this.fileNameEL = fileNameEL;
    this.dirPathTemplateEval = dirPathTemplateEval;
    this.dirPathTemplateVars = dirPathTemplateVars;
    this.timeZone = TimeZone.getTimeZone(timeZoneID);
    this.wholeFileExistsAction = wholeFileExistsAction;

    generators = new HashMap<>();
    outputStreamHelper = getStream();
  }

  private DataGenerator get(String filePath) throws StageException, IOException{
    DataGenerator generator = generators.get(filePath);
    if (generator == null) {
      generator = createDataGenerator(filePath);
      generators.put(filePath, generator);
    }
    return generator;
  }

  public void write(String filePath, Record record) throws StageException, IOException {
    DataGenerator generator = get(filePath);
    generator.write(record);
  }

  /*
  return the full filePath for a record
   */
  public String getFilePath(
      String dirPathTemplate,
      Record record,
      Date recordTime
  ) throws ELEvalException {
    String dirPath = resolvePath(dirPathTemplateEval, dirPathTemplateVars, dirPathTemplate, recordTime, record);
    return outputStreamHelper.getFilePath(dirPath, record, recordTime);
  }

  private DataGenerator createDataGenerator(String filePath) throws StageException, IOException{
    return dataFormatConfig.getDataGeneratorFactory().getGenerator(outputStreamHelper.getStream(filePath));
  }

  private OutputStreamHelper getStream() {
    OutputStreamHelper outputStream;
    if (dataFormat != DataFormat.WHOLE_FILE) {
      outputStream = new DefaultOutputStreamHandler(client, uniquePrefix);
    } else {
      outputStream = new WholeFileFormatOutputStreamHandler(client, uniquePrefix, fileNameEL, dirPathTemplateEval, dirPathTemplateVars, wholeFileExistsAction);
    }
    return outputStream;
  }

  public void close() throws IOException {
    for (String key : generators.keySet()) {
      generators.get(key).close();
    }
    generators.clear();
    outputStreamHelper.clearStatus();
  }

  public static Date getRecordTime(
      ELEval elEvaluator,
      ELVars variables,
      String expression,
      Record record
  ) throws OnRecordErrorException {
    try {
      TimeNowEL.setTimeNowInContext(variables, new Date());
      RecordEL.setRecordInContext(variables, record);
      return elEvaluator.eval(variables, expression, Date.class);
    } catch (ELEvalException e) {
      LOG.error("Failed to evaluate expression '{}' : ", expression, e.toString(), e);
      throw new OnRecordErrorException(record, e.getErrorCode(), e.getParams());
    }
  }

  private String resolvePath(
      ELEval dirPathTemplateEval,
      ELVars dirPathTemplateVars,
      String dirPathTemplate,
      Date date,
      Record record
  ) throws ELEvalException {
    RecordEL.setRecordInContext(dirPathTemplateVars, record);
    if (date != null) {
      Calendar calendar = Calendar.getInstance(timeZone);
      calendar.setTime(date);
      TimeEL.setCalendarInContext(dirPathTemplateVars, calendar);
    }
    return dirPathTemplateEval.eval(dirPathTemplateVars, dirPathTemplate, String.class);
  }
}
