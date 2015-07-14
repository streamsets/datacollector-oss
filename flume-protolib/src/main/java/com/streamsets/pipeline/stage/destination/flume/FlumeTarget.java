/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.flume;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.Errors;
import com.streamsets.pipeline.lib.FlumeUtil;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.avro.AvroDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.text.TextDataGeneratorFactory;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class FlumeTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(FlumeTarget.class);

  private static final String HOSTS_KEY = "hosts";
  private static final String BATCH_SIZE_KEY = "batch-size";
  private static final String CONNECTION_TIMEOUT_KEY = "connect-timeout";
  private static final String REQUEST_TIMEOUT_KEY = "request-timeout";
  private static final String CLIENT_TYPE_KEY = "client.type";
  private static final String CLIENT_TYPE_DEFAULT_FAILOVER = "default_failover";
  private static final String MAX_ATTEMPTS_KEY = "max-attempts";
  private static final String CLIENT_TYPE_DEFAULT_LOADBALANCING = "default_loadbalance";
  private static final String BACKOFF_KEY = "backoff";
  private static final String MAX_BACKOFF_KEY = "maxBackoff";
  private static final String HOST_SELECTOR_KEY = "host-selector";
  private static final String HOST_SELECTOR_RANDOM = "random";
  private static final String HOST_SELECTOR_ROUND_ROBIN = "round_robin";
  private static final String CHARSET_UTF8 = "UTF-8";
  private static final String HEADER_CHARSET_KEY = "charset";


  private final Map<String, String> flumeHostsConfig;
  private final DataFormat dataFormat;
  private final boolean singleEventPerBatch;
  private final CsvMode csvFileFormat;
  private final CsvHeader csvHeader;
  private final boolean csvReplaceNewLines;
  private final JsonMode jsonMode;
  private final String textFieldPath;
  private final boolean textEmptyLineIfNull;
  private final ClientType clientType;
  private final boolean backOff;
  private final HostSelectionStrategy hostSelectionStrategy;
  private final int maxBackOff;
  private final int batchSize;
  private final int connectionTimeout;
  private final int requestTimeout;
  private final int maxRetryAttempts;
  private final long waitBetweenRetries;
  private final String avroSchema;
  private final boolean includeSchema;

  private String charset;
  private ByteArrayOutputStream baos;

  public FlumeTarget(Map<String, String> flumeHostsConfig, DataFormat dataFormat, String charset,
                     boolean singleEventPerBatch,
                     CsvMode csvFileFormat, CsvHeader csvHeader, boolean csvReplaceNewLines, JsonMode jsonMode,
                     String textFieldPath, boolean textEmptyLineIfNull, ClientType clientType, boolean backOff,
                     HostSelectionStrategy hostSelectionStrategy, int maxBackOff, int batchSize,
                     int connectionTimeout, int requestTimeout, int maxRetryAttempts, long waitBetweenRetries,
                     String avroSchema, boolean includeSchema) {
    this.flumeHostsConfig = flumeHostsConfig;
    this.dataFormat = dataFormat;
    this.singleEventPerBatch = singleEventPerBatch;
    this.csvFileFormat = csvFileFormat;
    this.csvHeader = csvHeader;
    this.csvReplaceNewLines = csvReplaceNewLines;
    this.jsonMode = jsonMode;
    this.textFieldPath = textFieldPath;
    this.textEmptyLineIfNull = textEmptyLineIfNull;
    this.clientType = clientType;
    this.backOff = backOff;
    this.hostSelectionStrategy = hostSelectionStrategy;
    this.maxBackOff = maxBackOff;
    this.batchSize = batchSize;
    this.connectionTimeout = connectionTimeout;
    this.requestTimeout = requestTimeout;
    this.charset = charset;
    this.maxRetryAttempts = maxRetryAttempts;
    this.waitBetweenRetries = waitBetweenRetries;
    this.avroSchema = avroSchema;
    this.includeSchema = includeSchema;
    baos = new ByteArrayOutputStream(1024);
  }

  private DataGeneratorFactory generatorFactory;
  private RpcClient client;
  private long recordCounter = 0;
  private Map<String, String> headers;

  @Override
  protected List<ConfigIssue> validateConfigs() {
    List<ConfigIssue> issues = super.validateConfigs();
    FlumeUtil.validateHostConfig(issues, flumeHostsConfig, Groups.FLUME.name(), "flumeHostsConfig", getContext());
    if(batchSize < 1) {
      issues.add(getContext().createConfigIssue(Groups.FLUME.name(), "batchSize",
        Errors.FLUME_104, "batchSize", 1));
    }
    if(clientType == ClientType.AVRO_LOAD_BALANCING && backOff && maxBackOff < 0) {
      issues.add(getContext().createConfigIssue(Groups.FLUME.name(), "maxBackOff",
        Errors.FLUME_104, "maxBackOff", 0));
    }
    if(connectionTimeout < 1000) {
      issues.add(getContext().createConfigIssue(Groups.FLUME.name(), "connectionTimeout",
        Errors.FLUME_104, "connectionTimeout", 1000));
    }
    if(requestTimeout < 1000) {
      issues.add(getContext().createConfigIssue(Groups.FLUME.name(), "requestTimeout",
        Errors.FLUME_104, "requestTimeout", 1000));
    }
    if(maxRetryAttempts < 0) {
      issues.add(getContext().createConfigIssue(Groups.FLUME.name(), "maxRetryAttempts",
        Errors.FLUME_104, "maxRetryAttempts", 0));
    }
    if(waitBetweenRetries < 0) {
      issues.add(getContext().createConfigIssue(Groups.FLUME.name(), "waitBetweenRetries",
        Errors.FLUME_104, "waitBetweenRetries", 0));
    }
    return issues;
  }

  @Override
  public void init() throws StageException {
    connect();
    generatorFactory = createDataGeneratorFactory();
    headers = new HashMap<>();
    headers.put(HEADER_CHARSET_KEY, charset);
  }

  private void connect() {
    Properties props = new Properties();
    StringBuilder hosts = new StringBuilder();
    int numFlumeHosts = 0;
    for(Map.Entry<String, String> e : flumeHostsConfig.entrySet()) {
      hosts.append(e.getKey()).append(" ");
      props.setProperty(HOSTS_KEY + "." + e.getKey(), e.getValue());
      numFlumeHosts++;
    }
    props.setProperty(HOSTS_KEY, hosts.toString().trim());
    props.setProperty(BATCH_SIZE_KEY, String.valueOf(batchSize));
    props.setProperty(CONNECTION_TIMEOUT_KEY, String.valueOf(connectionTimeout));
    props.setProperty(REQUEST_TIMEOUT_KEY, String.valueOf(requestTimeout));

    switch(clientType) {
      case THRIFT:
        this.client = RpcClientFactory.getThriftInstance(props);
        break;
      case AVRO_FAILOVER:
        props.put(CLIENT_TYPE_KEY, CLIENT_TYPE_DEFAULT_FAILOVER);
        props.setProperty(MAX_ATTEMPTS_KEY, String.valueOf(numFlumeHosts));
        this.client = RpcClientFactory.getInstance(props);
        break;
      case AVRO_LOAD_BALANCING:
        props.put(CLIENT_TYPE_KEY, CLIENT_TYPE_DEFAULT_LOADBALANCING);
        props.setProperty(BACKOFF_KEY, String.valueOf(backOff));
        props.setProperty(MAX_BACKOFF_KEY, String.valueOf(maxBackOff));
        props.setProperty(HOST_SELECTOR_KEY, getHostSelector(hostSelectionStrategy));
        this.client = RpcClientFactory.getInstance(props);
        break;
      default:
        throw new IllegalStateException("Unsupported client type - cannot happen");
    }
  }

  private String getHostSelector(HostSelectionStrategy hostSelectionStrategy) {
    switch(hostSelectionStrategy) {
      case RANDOM:
        return HOST_SELECTOR_RANDOM;
      case ROUND_ROBIN:
        return HOST_SELECTOR_ROUND_ROBIN;
      default :
        throw new IllegalStateException("Unexpected Host Selection Strategy");
    }
  }

  private DataGeneratorFactory createDataGeneratorFactory() {
    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(getContext(),
      dataFormat.getGeneratorFormat());
    if(charset == null || charset.trim().isEmpty()) {
      charset = CHARSET_UTF8;
    }
    builder.setCharset(Charset.forName(charset));
    switch (dataFormat) {
      case SDC_JSON:
        break;
      case DELIMITED:
        builder.setMode(csvFileFormat);
        builder.setMode(csvHeader);
        builder.setConfig(DelimitedDataGeneratorFactory.REPLACE_NEWLINES_KEY, csvReplaceNewLines);
        break;
      case TEXT:
        builder.setConfig(TextDataGeneratorFactory.FIELD_PATH_KEY, textFieldPath);
        builder.setConfig(TextDataGeneratorFactory.EMPTY_LINE_IF_NULL_KEY, textEmptyLineIfNull);
        break;
      case JSON:
        builder.setMode(jsonMode);
        break;
      case AVRO:
        builder.setConfig(AvroDataGeneratorFactory.SCHEMA_KEY, avroSchema);
        builder.setConfig(AvroDataGeneratorFactory.INCLUDE_SCHEMA_KEY, includeSchema);
        break;
    }
    return builder.build();
  }

  @Override
  public void write(Batch batch) throws StageException {
    if(singleEventPerBatch) {
      writeOneEventPerBatch(batch);
    } else {
      writeOneEventPerRecord(batch);
    }
  }

  @Override
  public void destroy() {
    LOG.info("Wrote {} number of records to Flume Agent", recordCounter);
    if(client != null) {
      client.close();
    }
  }

  private void writeOneEventPerRecord(Batch batch) throws StageException {
    Iterator<Record> records = batch.getRecords();
    List<Event> events = new ArrayList<>();
    while (records.hasNext()) {
      Record record = records.next();
      try {
        baos.reset();
        DataGenerator generator = generatorFactory.getGenerator(baos);
        generator.write(record);
        generator.close();
        events.add(EventBuilder.withBody(baos.toByteArray(), headers));
      } catch (Exception ex) {
        handleException(ex, record);
      }
    }
    writeToFlume(events);
    LOG.debug("Wrote {} records in this batch.", events.size());
  }

  private void writeOneEventPerBatch(Batch batch) throws StageException {
    int count = 0;
    Iterator<Record> records = batch.getRecords();
    baos.reset();
    Record currentRecord = null;
    List<Event> events = new ArrayList<>();
    try {
      DataGenerator generator = generatorFactory.getGenerator(baos);
      while(records.hasNext()) {
        Record record = records.next();
        currentRecord = record;
        generator.write(record);
        count++;
      }
      currentRecord = null;
      generator.close();
      events.add(EventBuilder.withBody(baos.toByteArray()));
    } catch (Exception ex) {
      handleException(ex, currentRecord);
    }
    writeToFlume(events);
    LOG.debug("Wrote {} records in this batch.", count);
  }

  private void handleException(Exception ex, Record record) throws StageException {
    switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        getContext().toError(record, ex);
        break;
      case STOP_PIPELINE:
        if (ex instanceof StageException) {
          throw (StageException) ex;
        } else {
          throw new StageException(Errors.FLUME_50, record.getHeader().getSourceId(), ex.getMessage(), ex);
        }
      default:
        throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
          getContext().getOnErrorRecord()));
    }
  }

  private void writeToFlume(List<Event> events) throws StageException {
    int retries = 0;
    Exception ex = null;
    while (retries <= maxRetryAttempts) {
      if(retries != 0) {
        LOG.info("Retry attempt number {}", retries);
      }
      try {
        client.appendBatch(events);
        recordCounter += events.size();
        return;
      } catch (EventDeliveryException e) {
        ex = e;
        retries++;
        LOG.info("Encountered exception while sending data to flume : {}", e.getMessage(), e);
        if(!ThreadUtil.sleep(waitBetweenRetries)) {
          break;
        }
        reconnect();
      }
    }
    throw new StageException(Errors.FLUME_51, ex.getMessage(), ex);
  }

  private void reconnect() {
    client.close();
    client = null;
    connect();
  }

}
