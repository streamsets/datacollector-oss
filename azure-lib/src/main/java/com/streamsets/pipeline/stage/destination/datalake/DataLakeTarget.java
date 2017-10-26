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
package com.streamsets.pipeline.stage.destination.datalake;

import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.FakeRecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.datalake.writer.DataLakeWriterThread;
import com.streamsets.pipeline.stage.destination.datalake.writer.RecordWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class DataLakeTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(DataLakeTarget.class);
  private static final int MEGA_BYTE = 1024 * 1024;

  private final DataLakeConfigBean conf;
  private final int threadPoolSize = 1;
  private static final String EL_PREFIX = "${";
  private ADLStoreClient client;
  private ELEval dirPathTemplateEval;
  private ELVars dirPathTemplateVars;
  private ELEval timeDriverEval;
  private ELVars timeDriverVars;
  private Calendar calendar;
  private RecordWriter writer;
  private ErrorRecordHandler errorRecordHandler;
  private SafeScheduledExecutorService scheduledExecutor;
  private long idleTimeSecs = -1;


  public static final String TARGET_DIRECTORY_HEADER = "targetDirectory";

  public DataLakeTarget(DataLakeConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    conf.init(getContext(), issues);
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    scheduledExecutor = new SafeScheduledExecutorService(threadPoolSize, "data-lake-target");

    dirPathTemplateEval = getContext().createELEval("dirPathTemplate");
    dirPathTemplateVars = getContext().createELVars();
    timeDriverEval = getContext().createELEval("timeDriver");
    timeDriverVars = getContext().createELVars();

    calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of(conf.timeZoneID)));

    if (conf.idleTimeout != null && !conf.idleTimeout.isEmpty()) {
      idleTimeSecs = initTimeConfigs(getContext(), "idleTimeout", conf.idleTimeout, Groups.OUTPUT,
          true, Errors.ADLS_06, issues);
    }

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

    if (conf.dataFormat != DataFormat.WHOLE_FILE) {
      if (!conf.fileNameSuffix.isEmpty() && (conf.fileNameSuffix.startsWith(".") || conf.fileNameSuffix.contains("/"))) {

        issues.add(getContext().createConfigIssue(Groups.DATALAKE.name(),
            DataLakeConfigBean.ADLS_CONFIG_BEAN_PREFIX + "fileNameSuffix",
            Errors.ADLS_08
        ));
      }

      if (conf.maxRecordsPerFile < 0) {
        issues.add(getContext().createConfigIssue(Groups.DATALAKE.name(),
            DataLakeConfigBean.ADLS_CONFIG_BEAN_PREFIX + "maxRecordsPerFile",
            Errors.ADLS_09
        ));
      }
    }

    String authEndPoint = resolveCredentialValue(conf.authTokenEndpoint, DataLakeConfigBean.ADLS_CONFIG_AUTH_ENDPOINT, issues);
    String clientId = resolveCredentialValue(conf.clientId, DataLakeConfigBean.ADLS_CONFIG_CLIENT_ID, issues);
    String clientKey = resolveCredentialValue(conf.clientKey, DataLakeConfigBean.ADLS_CONFIG_CLIENT_KEY, issues);
    String accountFQDN = resolveCredentialValue(conf.accountFQDN, DataLakeConfigBean.ADLS_CONFIG_ACCOUNT_FQDN, issues);

    if (issues.isEmpty()) {
      // connect to ADLS
      try {
        client = createClient(
          authEndPoint,
          clientId,
          clientKey,
          accountFQDN
        );

        if (conf.checkPermission && !conf.dirPathTemplateInHeader) {
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
          errorMessage = ex1.getMessage();
          if (errorMessage == null) {
            errorMessage = ((ADLException) ex1).remoteExceptionMessage;
          }
        }

        LOG.error(Errors.ADLS_02.getMessage(), errorMessage, ex1);
        issues.add(getContext().createConfigIssue(
            Groups.DATALAKE.name(),
            DataLakeConfigBean.ADLS_CONFIG_BEAN_PREFIX,
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
          conf.fileNameSuffix,
          conf.dataFormatConfig.fileNameEL,
          conf.dirPathTemplateInHeader,
          getContext(),
          conf.rollIfHeader,
          conf.rollHeaderName,
          conf.maxRecordsPerFile,
          conf.maxFileSize * MEGA_BYTE,
          conf.dataFormatConfig.wholeFileExistsAction,
          authEndPoint,
          clientId,
          clientKey,
          idleTimeSecs
      );
    }

    return issues;
  }

  private String resolveCredentialValue(CredentialValue credentialValue, String configName, List<ConfigIssue> issues) {
    try {
      return credentialValue.get();
    } catch (StageException e) {
      LOG.error(Errors.ADLS_15.getMessage(), e.toString(), e);
      issues.add(getContext().createConfigIssue(
          Groups.DATALAKE.name(),
          configName,
          Errors.ADLS_15,
          e.toString()
      ));
    }

    return null;
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
        try {
          writer.close();
        } catch (StageException | IOException ex) {
          String errorMessage = ex.toString();
          if (ex instanceof ADLException) {
            errorMessage = ex.getMessage();
            if (errorMessage == null) {
              errorMessage = ((ADLException) ex).remoteExceptionMessage;
            }
          }
          LOG.error(Errors.ADLS_04.getMessage(), errorMessage, ex);
        }
      }

      if (scheduledExecutor != null && !scheduledExecutor.isTerminated()) {
        scheduledExecutor.shutdown();

        while (!scheduledExecutor.isShutdown()) {
          Thread.sleep(100);
        }
      }
    } catch (InterruptedException ex) {
      LOG.error("interrupted: {}", ex.toString(), ex);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void write(Batch batch) throws StageException {
    List<Future<List<OnRecordErrorException>>> futures = new ArrayList<>();
    List<OnRecordErrorException> errorRecords = new ArrayList<>();

    // First, get the file path per records
    Map<String, List<Record>> recordsPerFile = getRecordsPerFile(batch);

    // Write the record to the respective file path
    for (Map.Entry<String, List<Record>> entry : recordsPerFile.entrySet()) {
      String filePath = entry.getKey();
      List<Record> records = entry.getValue();
      Callable<List<OnRecordErrorException>> worker = new DataLakeWriterThread(writer, filePath, records);
      Future<List<OnRecordErrorException>> future = scheduledExecutor.submit(worker);
      futures.add(future);
    }

    // Wait for proper execution finish
    for (Future<List<OnRecordErrorException>> f : futures) {
      try {
        List<OnRecordErrorException> result = f.get();
        errorRecords.addAll(result);
      } catch (InterruptedException e) {
        LOG.error("Interrupted data generation thread", e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof StageException) {
          throw (StageException) e.getCause();
        } else{
          throw new StageException(Errors.ADLS_11, e.toString(), e);
        }
      }
    }

    for (OnRecordErrorException errorRecord : errorRecords) {
      LOG.error(errorRecord.getErrorCode().getMessage(), errorRecord.toString());
      errorRecordHandler.onError(errorRecord);
    }

    try {
      writer.issueCachedEvents();
    } catch (IOException ex) {
      throw new StageException(Errors.ADLS_12, String.valueOf(ex), ex);
    }

  }

  private Map<String, List<Record>> getRecordsPerFile(Batch batch) throws StageException {
    Iterator<Record> recordIterator = batch.getRecords();

    Map<String, List<Record>> recordsPerFile = new HashMap<>();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();

      try {
        Date recordTime = writer.getRecordTime(timeDriverEval, timeDriverVars, conf.timeDriver, record);

        if (recordTime == null) {
          LOG.error(Errors.ADLS_07.getMessage(), conf.timeDriver);
          errorRecordHandler.onError(new OnRecordErrorException(record, Errors.ADLS_07, conf.timeDriver));
        }

        String filePath = writer.getFilePath(
            conf.dirPathTemplate,
            record,
            recordTime
        );
        List<Record> records = recordsPerFile.get(filePath);
        if (records == null) {
          records = new ArrayList<>();
        }
        records.add(record);
        recordsPerFile.put(filePath, records);
      } catch (ELEvalException ex0) {
        LOG.error(Errors.ADLS_00.getMessage(), ex0.toString(), ex0);
        errorRecordHandler.onError(new OnRecordErrorException(record, Errors.ADLS_00, ex0.toString(), ex0));
      }
    }
    return recordsPerFile;
  }

  private void validatePermission() throws ELEvalException, IOException {
    ELVars vars = getContext().createELVars();
    ELEval elEval = getContext().createELEval("dirPathTemplate", FakeRecordEL.class);
    TimeEL.setCalendarInContext(vars, calendar);
    String dirPath = elEval.eval(vars, conf.dirPathTemplate, String.class);
    String filePath = dirPath + "/" + UUID.randomUUID();

    final String octalPermission = "0770";
    if(!client.checkExists(filePath)) {
      client.createFile(filePath, null, octalPermission, true);
      client.delete(filePath);
    }
  }

  private long initTimeConfigs(
      Stage.Context context,
      String configName,
      String configuredValue,
      Groups configGroup,
      boolean allowNegOne,
      Errors errorCode,
      List<Stage.ConfigIssue> issues) {
    long timeInSecs = 0;
    try {
      ELEval timeEvaluator = context.createELEval(configName);
      context.parseEL(configuredValue);
      timeInSecs = timeEvaluator.eval(context.createELVars(),
          configuredValue, Long.class);
      if (timeInSecs <= 0 && (!allowNegOne || timeInSecs != -1)) {
        issues.add(
            context.createConfigIssue(
                configGroup.name(),
                DataLakeConfigBean.ADLS_CONFIG_BEAN_PREFIX + configName,
                errorCode
            )
        );
      }
    } catch (Exception ex) {
      issues.add(
          context.createConfigIssue(
              configGroup.name(),
              DataLakeConfigBean.ADLS_CONFIG_BEAN_PREFIX + configName,
              Errors.ADLS_10,
              configuredValue,
              ex.toString(),
              ex
          )
      );
    }
    return timeInSecs;
  }
}
