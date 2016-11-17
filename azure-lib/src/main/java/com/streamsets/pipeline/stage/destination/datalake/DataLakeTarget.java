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

import com.google.common.collect.Multimap;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
  private ByteArrayOutputStream baos;
  private ADLStoreClient client;
  private ELEval dirPathTemplateEval;
  private ELVars dirPathTemplateVars;
  private ELEval timeDriverEval;
  private ELVars timeDriverVars;
  private Calendar calendar;
  private String filePath;

  private ErrorRecordHandler errorRecordHandler;

  public DataLakeTarget(DataLakeConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    conf.init(getContext(), issues);
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    baos = new ByteArrayOutputStream();

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
      } catch (IOException ex) {
        issues.add(getContext().createConfigIssue(
            Groups.DATALAKE.name(),
            DataLakeConfigBean.ADLS_CONFIG_BEAN_PREFIX + "clientId",
            Errors.ADLS_02,
            ex.toString()
        ));
      }
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
  }

  @Override
  public void write(Batch batch) throws StageException {
    Multimap<String, Record> partitions = ELUtils.partitionBatchByExpression(
        dirPathTemplateEval,
        dirPathTemplateVars,
        conf.dirPathTemplate,
        timeDriverEval,
        timeDriverVars,
        conf.timeDriver,
        calendar,
        batch
    );

    OutputStream stream = null;
    Record record = null;
    DataGenerator generator = null;

    for (String key : partitions.keySet()) {
      Iterator<Record> iterator = partitions.get(key).iterator();
      // for uniqueness of the file name
      filePath = key + conf.uniquePrefix + "-" + UUID.randomUUID();

      try {
        if (!client.checkExists(filePath)) {
          stream = client.createFile(filePath, IfExists.FAIL);
        } else {
          stream = client.getAppendStream(filePath);
        }

        while (iterator.hasNext()) {
          record = iterator.next();
          baos.reset();
          generator = conf.dataFormatConfig.getDataGeneratorFactory().getGenerator(baos);
          generator.write(record);
          generator.close();
          stream.write(baos.toByteArray());
        }
      } catch (IOException ex) {
        if(record == null) {
          // possible permission error to the directory or connection issues, then throw stage exception
          LOG.error(Errors.ADLS_02.getMessage(), ex.toString(), ex);
          throw new StageException(Errors.ADLS_02, ex, ex);
        } else {
          LOG.error(Errors.ADLS_03.getMessage(), ex.toString(), ex);
          errorRecordHandler.onError(new OnRecordErrorException(record, Errors.ADLS_03, ex.toString(), ex));
        }
      } finally {
        try {
          if (generator != null) {
            generator.close();
          }

          if (stream != null) {
            stream.close();
          }
        } catch (IOException ex2) {
          //no-op
          LOG.error("fail to close stream or generator: {}. reason: {}", ex2.toString(), ex2);
        }
      }
    }
  }
}
