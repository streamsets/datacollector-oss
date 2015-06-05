/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.flume;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.FlumeTestUtil;
import com.streamsets.pipeline.lib.util.SdcAvroTestUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AvroSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestFlumeFailoverTarget {

  private AvroSource source;
  private Channel ch;

  @Before
  public void setUp() {
    source = new AvroSource();
    ch = new MemoryChannel();
    Configurables.configure(ch, new Context());

    Context context = new Context();
    context.put("port", String.valueOf(9050));
    context.put("bind", "localhost");
    Configurables.configure(source, context);

    List<Channel> channels = new ArrayList<>();
    channels.add(ch);
    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);
    source.setChannelProcessor(new ChannelProcessor(rcs));
    source.start();
  }

  @After
  public void tearDown(){
    source.stop();
    ch.stop();
  }

  @Test
  public void testFlumeConfig() throws StageException {

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_FAILOVER)
      .addConfiguration("maxRetryAttempts", -1)
      .addConfiguration("waitBetweenRetries", -1)
      .addConfiguration("batchSize", 0)
      .addConfiguration("connectionTimeout", 500)
      .addConfiguration("requestTimeout", 500)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("singleEventPerBatch", false)
      .addConfiguration("charset", "UTF-8")
      .build();

    try {
      targetRunner.runInit();
      Assert.fail("Expected Stage Exception as configuration options are not valid");
    } catch (StageException e) {

    }
  }

  @Test
  public void testWriteStringRecords() throws StageException {

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_FAILOVER)
      .addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .addConfiguration("batchSize", 1)
      .addConfiguration("connectionTimeout", 1000)
      .addConfiguration("requestTimeout", 1000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("singleEventPerBatch", false)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    for(Record r : logRecords) {
      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      Assert.assertEquals(r.get().getValueAsString(), new String(event.getBody()).trim());
      Assert.assertTrue(event.getHeaders().containsKey("charset"));
      Assert.assertEquals("UTF-8", event.getHeaders().get("charset"));
      transaction.commit();
      transaction.close();
    }
  }

  @Test
  public void testWriteStringRecordsOneEventPerBatch() throws StageException {

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_FAILOVER)
      .addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .addConfiguration("batchSize", 1)
      .addConfiguration("connectionTimeout", 1000)
      .addConfiguration("requestTimeout", 1000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("singleEventPerBatch", true)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    int totalFlumeEvents = 0;
    Transaction transaction = ch.getTransaction();
    transaction.begin();
    while(ch.take() != null) {
      totalFlumeEvents++;
    }
    transaction.commit();
    transaction.close();

    Assert.assertEquals(1, totalFlumeEvents);
  }

  @Test
  public void testWriteStringRecordsFromJSON() throws InterruptedException, StageException, IOException {

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_FAILOVER)
      .addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .addConfiguration("batchSize", 1)
      .addConfiguration("connectionTimeout", 1000)
      .addConfiguration("requestTimeout", 1000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("textFieldPath", "/name")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("singleEventPerBatch", false)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    for(Record r : logRecords) {
      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      Assert.assertEquals(r.get().getValueAsMap().get("name").getValueAsString(), new String(event.getBody()).trim());
      Assert.assertTrue(event.getHeaders().containsKey("charset"));
      Assert.assertEquals("UTF-8", event.getHeaders().get("charset"));
      transaction.commit();
      transaction.close();
    }
  }

  @Test
  public void testWriteStringRecordsFromJSON2() throws InterruptedException, StageException, IOException {

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_FAILOVER)
      .addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .addConfiguration("batchSize", 1)
      .addConfiguration("connectionTimeout", 1000)
      .addConfiguration("requestTimeout", 1000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("textFieldPath", "/lastStatusChange") //this is number field, should be converted to string
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("singleEventPerBatch", false)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    for(Record r : logRecords) {
      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      Assert.assertEquals(r.get().getValueAsMap().get("lastStatusChange").getValueAsString(), new String(event.getBody()).trim());
      Assert.assertTrue(event.getHeaders().containsKey("charset"));
      Assert.assertEquals("UTF-8", event.getHeaders().get("charset"));
      transaction.commit();
      transaction.close();
    }
  }

  @Test
  public void testWriteStringRecordsFromJSON3() throws InterruptedException, StageException, IOException {

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_FAILOVER)
      .addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .addConfiguration("batchSize", 1)
      .addConfiguration("connectionTimeout", 1000)
      .addConfiguration("requestTimeout", 1000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("textFieldPath", "/") //this is map field, should not be converted to string
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("singleEventPerBatch", false)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);

    //All records must be sent to error
    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());

    targetRunner.runDestroy();

    Transaction transaction = ch.getTransaction();
    transaction.begin();
    Event event = ch.take();
    Assert.assertNull(event);
    transaction.commit();
    transaction.close();

  }

  @Test
  public void testWriteJsonRecords() throws InterruptedException, StageException, IOException {

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_FAILOVER)
      .addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .addConfiguration("batchSize", 1)
      .addConfiguration("connectionTimeout", 1000)
      .addConfiguration("requestTimeout", 1000)
      .addConfiguration("dataFormat", DataFormat.SDC_JSON)
      .addConfiguration("singleEventPerBatch", false)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    ContextExtensions ctx = (ContextExtensions) ContextInfoCreator.createTargetContext("", false, OnRecordError.TO_ERROR);
    for(Record r : logRecords) {
      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      ByteArrayInputStream bais = new ByteArrayInputStream(event.getBody());
      RecordReader rr = ctx.createRecordReader(bais, 0, Integer.MAX_VALUE);
      Assert.assertEquals(r, rr.readRecord());
      Assert.assertTrue(event.getHeaders().containsKey("charset"));
      Assert.assertEquals("UTF-8", event.getHeaders().get("charset"));
      rr.close();
      transaction.commit();
      transaction.close();
    }
  }

  @Test
  public void testWriteCsvRecords() throws InterruptedException, StageException, IOException {

    //Test DELIMITED is - "2010,NLDS1,PHI,NL,CIN,NL,3,0,0"

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_FAILOVER)
      .addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .addConfiguration("batchSize", 1)
      .addConfiguration("connectionTimeout", 1000)
      .addConfiguration("requestTimeout", 1000)
      .addConfiguration("dataFormat", DataFormat.DELIMITED)
      .addConfiguration("csvFileFormat", CsvMode.CSV)
      .addConfiguration("csvHeader", CsvHeader.NO_HEADER)
      .addConfiguration("csvReplaceNewLines", false)
      .addConfiguration("singleEventPerBatch", false)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createCsvRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    for(Record r : logRecords) {
      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      transaction.commit();
      transaction.close();
    }
  }

  @Test
  public void testWriteAvroRecords() throws InterruptedException, StageException, IOException {

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_FAILOVER)
      .addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .addConfiguration("batchSize", 1)
      .addConfiguration("connectionTimeout", 1000)
      .addConfiguration("requestTimeout", 1000)
      .addConfiguration("dataFormat", DataFormat.AVRO)
      .addConfiguration("csvFileFormat", CsvMode.CSV)
      .addConfiguration("csvHeader", CsvHeader.NO_HEADER)
      .addConfiguration("csvReplaceNewLines", false)
      .addConfiguration("singleEventPerBatch", false)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("avroSchema", SdcAvroTestUtil.AVRO_SCHEMA1)
      .addConfiguration("includeSchema", true)
      .build();

    targetRunner.runInit();
    List<Record> records = SdcAvroTestUtil.getRecords1();
    targetRunner.runWrite(records);
    targetRunner.runDestroy();

    List<GenericRecord> genericRecords = new ArrayList<>();
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(); //Reader schema argument is optional

    Transaction transaction = ch.getTransaction();
    transaction.begin();
    Event event = ch.take();
    while(event != null) {
      DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
        new SeekableByteArrayInput(event.getBody()), datumReader);
      while(dataFileReader.hasNext()) {
        genericRecords.add(dataFileReader.next());
      }
      event = ch.take();
    }
    transaction.commit();
    transaction.close();

    Assert.assertEquals(3, genericRecords.size());
    SdcAvroTestUtil.compare1(genericRecords);
  }

  @Test
  public void testWriteAvroRecordsSingleEvent() throws InterruptedException, StageException, IOException {

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_FAILOVER)
      .addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .addConfiguration("batchSize", 1)
      .addConfiguration("connectionTimeout", 1000)
      .addConfiguration("requestTimeout", 1000)
      .addConfiguration("dataFormat", DataFormat.AVRO)
      .addConfiguration("csvFileFormat", CsvMode.CSV)
      .addConfiguration("csvHeader", CsvHeader.NO_HEADER)
      .addConfiguration("csvReplaceNewLines", false)
      .addConfiguration("singleEventPerBatch", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("avroSchema", SdcAvroTestUtil.AVRO_SCHEMA1)
      .addConfiguration("includeSchema", true)
      .build();

    targetRunner.runInit();
    List<Record> records = SdcAvroTestUtil.getRecords1();
    targetRunner.runWrite(records);
    targetRunner.runDestroy();

    List<GenericRecord> genericRecords = new ArrayList<>();
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(); //Reader schema argument is optional

    int eventCounter = 0;
    Transaction transaction = ch.getTransaction();
    transaction.begin();
    Event event = ch.take();
    while(event != null) {
      eventCounter++;
      DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
        new SeekableByteArrayInput(event.getBody()), datumReader);
      while(dataFileReader.hasNext()) {
        genericRecords.add(dataFileReader.next());
      }
      event = ch.take();
    }
    transaction.commit();
    transaction.close();

    Assert.assertEquals(1, eventCounter);
    Assert.assertEquals(3, genericRecords.size());
    SdcAvroTestUtil.compare1(genericRecords);
  }

  @Test
  public void testWriteAvroRecordsDropSchema() throws InterruptedException, StageException, IOException {

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_FAILOVER)
      .addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .addConfiguration("batchSize", 1)
      .addConfiguration("connectionTimeout", 1000)
      .addConfiguration("requestTimeout", 1000)
      .addConfiguration("dataFormat", DataFormat.AVRO)
      .addConfiguration("csvFileFormat", CsvMode.CSV)
      .addConfiguration("csvHeader", CsvHeader.NO_HEADER)
      .addConfiguration("csvReplaceNewLines", false)
      .addConfiguration("singleEventPerBatch", false)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("avroSchema", SdcAvroTestUtil.AVRO_SCHEMA1)
      .addConfiguration("includeSchema", false)
      .build();

    targetRunner.runInit();
    List<Record> records = SdcAvroTestUtil.getRecords1();
    targetRunner.runWrite(records);
    targetRunner.runDestroy();

    List<GenericRecord> genericRecords = new ArrayList<>();
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(); //Reader schema argument is optional
    datumReader.setSchema(new Schema.Parser().parse(SdcAvroTestUtil.AVRO_SCHEMA1));

    Transaction transaction = ch.getTransaction();
    transaction.begin();
    Event event = ch.take();
    while(event != null) {
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(event.getBody(), null);
      GenericRecord read = datumReader.read(null, decoder);
      genericRecords.add(read);
      event = ch.take();
    }
    transaction.commit();
    transaction.close();

    Assert.assertEquals(3, genericRecords.size());
    SdcAvroTestUtil.compare1(genericRecords);
  }

  @Test
  public void testWriteAvroRecordsDropSchemaSingleEvent() throws InterruptedException, StageException, IOException {

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_FAILOVER)
      .addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .addConfiguration("batchSize", 1)
      .addConfiguration("connectionTimeout", 1000)
      .addConfiguration("requestTimeout", 1000)
      .addConfiguration("dataFormat", DataFormat.AVRO)
      .addConfiguration("csvFileFormat", CsvMode.CSV)
      .addConfiguration("csvHeader", CsvHeader.NO_HEADER)
      .addConfiguration("csvReplaceNewLines", false)
      .addConfiguration("singleEventPerBatch", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("avroSchema", SdcAvroTestUtil.AVRO_SCHEMA1)
      .addConfiguration("includeSchema", false)
      .build();

    targetRunner.runInit();
    List<Record> records = SdcAvroTestUtil.getRecords1();
    targetRunner.runWrite(records);
    targetRunner.runDestroy();

    List<GenericRecord> genericRecords = new ArrayList<>();
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(); //Reader schema argument is optional
    datumReader.setSchema(new Schema.Parser().parse(SdcAvroTestUtil.AVRO_SCHEMA1));

    int eventCounter = 0;
    Transaction transaction = ch.getTransaction();
    transaction.begin();
    Event event = ch.take();
    while(event != null) {
      eventCounter++;
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(event.getBody(), null);
      GenericRecord read = datumReader.read(null, decoder);
      while(read != null) {
        genericRecords.add(read);
        try {
          read = datumReader.read(null, decoder);
        } catch (EOFException e) {
          break;
        }
      }
      event = ch.take();
    }
    transaction.commit();
    transaction.close();

    Assert.assertEquals(1, eventCounter);
    Assert.assertEquals(3, genericRecords.size());
    SdcAvroTestUtil.compare1(genericRecords);
  }
}
