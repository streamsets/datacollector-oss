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

package com.streamsets.pipeline.stage.destination.datalake;

import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.FakeRecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.datalake.writer.RecordWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

public class DataLakeTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(DataLakeTarget.class);
  private final DataLakeConfigBean conf;
  private static final String EL_PREFIX = "${";
  private ADLStoreClient client;
  private ELEval dirPathTemplateEval;
  private ELVars dirPathTemplateVars;
  private ELEval timeDriverEval;
  private ELVars timeDriverVars;
  private Calendar calendar;
  private RecordWriter writer;

  private ErrorRecordHandler errorRecordHandler;

  public DataLakeTarget(DataLakeConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    conf.init(getContext(), issues);
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    dirPathTemplateEval = getContext().createELEval("dirPathTemplate");
    dirPathTemplateVars = getContext().createELVars();
    timeDriverEval = getContext().createELEval("timeDriver");
    timeDriverVars = getContext().createELVars();

    calendar = Calendar.getInstance(TimeZone.getTimeZone(conf.timeZoneID));

    if (conf.dirPathTemplate.startsWith(EL_PREFIX)) {
      TimeEL.setCalendarInContext(dirPathTemplateVars, calendar);
      TimeNowEL.setTimeNowInContext(dirPathTemplateVars, new Date());

      // Validate Evals
      ELUtils.validateExpression(
          dirPathTemplateEval,
          getContext().createELVars(),
          conf.dirPathTemplate,
          getContext(),
          Groups.DATALAKE.getLabel(),
          DataLakeConfigBean.ADLS_CONFIG_BEAN_PREFIX + "dirPathTemplate",
          Errors.ADLS_00,
          String.class,
          issues
      );
    }

    if (conf.timeDriver.startsWith(EL_PREFIX)) {
      TimeEL.setCalendarInContext(timeDriverVars, calendar);
      TimeNowEL.setTimeNowInContext(timeDriverVars, new Date());

      ELUtils.validateExpression(
          timeDriverEval,
          timeDriverVars,
          conf.timeDriver,
          getContext(),
          Groups.DATALAKE.getLabel(),
          DataLakeConfigBean.ADLS_CONFIG_BEAN_PREFIX + "timeDriver",
          Errors.ADLS_01,
          Date.class,
          issues
      );
    }

    if (issues.isEmpty()) {
      // connect to ADLS
      try {
        client = createClient(conf.authTokenEndpoint, conf.clientId, conf.clientKey, conf.accountFQDN);

        if (conf.checkPermission) {
          validatePermission();
        }
      } catch (ELEvalException ex0) {
        issues.add(getContext().createConfigIssue(
            Groups.DATALAKE.name(),
            DataLakeConfigBean.ADLS_CONFIG_BEAN_PREFIX + "dirPathTemplate",
            Errors.ADLS_00,
            ex0.toString()
        ));
      } catch (IOException ex1) {
        String errorMessage = ex1.toString();
        if (ex1 instanceof ADLException) {
          errorMessage = ((ADLException) ex1).remoteExceptionMessage;
        }
        issues.add(getContext().createConfigIssue(
            Groups.DATALAKE.name(),
            DataLakeConfigBean.ADLS_CONFIG_BEAN_PREFIX + "clientId",
            Errors.ADLS_02,
            errorMessage
        ));
      }
    }

    if (issues.isEmpty()) {
      writer = new RecordWriter(
          client,
          conf.dataFormat,
          conf.dataFormatConfig,
          conf.uniquePrefix,
          conf.dataFormatConfig.fileNameEL,
          dirPathTemplateEval,
          dirPathTemplateVars,
          conf.timeZoneID,
          conf.dataFormatConfig.wholeFileExistsAction
      );
    }

    return issues;
  }

  ADLStoreClient createClient(String authTokenEndpoint, String clientId, String clientKey, String accountFQDN)
      throws IOException {
    AzureADToken token = AzureADAuthenticator.getTokenUsingClientCreds(authTokenEndpoint, clientId, clientKey);

    return ADLStoreClient.createClient(accountFQDN, token);
  }

  @Override
  public void destroy() {
    super.destroy();
    try {
      if(writer != null) {
        writer.close();
      }
    } catch (IOException ex) {
      LOG.error(Errors.ADLS_04.getMessage(), ex.toString(), ex);
    }
  }

  @Override
  public void write(Batch batch) throws StageException {
    Record record = null;
    Iterator<Record> recordIterator = batch.getRecords();

    try {
      while (recordIterator.hasNext()) {
        record = recordIterator.next();
        try {
          Date recordTime = writer.getRecordTime(timeDriverEval, timeDriverVars, conf.timeDriver, record);
          String filePath = writer.getFilePath(
              conf.dirPathTemplate,
              record,
              recordTime
          );
          writer.write(filePath, record);
        } catch (ELEvalException ex0) {
          LOG.error(Errors.ADLS_00.getMessage(), ex0.toString(), ex0);
          errorRecordHandler.onError(new OnRecordErrorException(
              record,
              Errors.ADLS_00,
              ex0.toString(),
              ex0
          ));
        } catch (IOException ex1) {
          String errorMessage = ex1.toString();
          if (ex1 instanceof ADLException) {
            errorMessage = ((ADLException) ex1).remoteExceptionMessage;
          }
          if (record == null) {
            // possible permission error to the directory or connection issues, then throw stage exception
            LOG.error(Errors.ADLS_02.getMessage(), errorMessage, ex1);
            throw new StageException(Errors.ADLS_02, errorMessage, ex1);
          } else {
            LOG.error(Errors.ADLS_03.getMessage(), errorMessage, ex1);
            errorRecordHandler.onError(new OnRecordErrorException(record, Errors.ADLS_03, errorMessage, ex1));
          }
        }
      }
    } finally {
      try {
        writer.close();
      } catch (IOException ex) {
        //no-op
        LOG.error(Errors.ADLS_04.getMessage(), ex.toString(), ex);
      }
    }
  }

  private void validatePermission() throws ELEvalException, IOException {
    ELVars vars = getContext().createELVars();
    ELEval elEval = getContext().createELEval("dirPathTemplate", FakeRecordEL.class);
    TimeEL.setCalendarInContext(vars, calendar);
    String dirPath = elEval.eval(vars, conf.dirPathTemplate, String.class);
    String filePath = dirPath + "/" + UUID.randomUUID();

    if(client.checkExists(filePath)) {
      client.createFile(filePath, null, "rwx", true);
      client.delete(filePath);
    }
  }
}
