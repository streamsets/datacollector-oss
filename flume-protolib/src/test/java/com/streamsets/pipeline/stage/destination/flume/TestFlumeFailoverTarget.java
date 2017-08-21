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
package com.streamsets.pipeline.stage.destination.flume;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.config.AvroCompression;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.FlumeTestUtil;
import com.streamsets.pipeline.lib.util.SdcAvroTestUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.testing.NetworkUtils;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.config.DestinationAvroSchemaSource.INLINE;

public class TestFlumeFailoverTarget {

  private AvroSource source;
  private Channel ch;
  private int port;

  @Before
  public void setUp() throws Exception {
    port = NetworkUtils.getRandomPort();
    source = new AvroSource();
    ch = new MemoryChannel();
    Configurables.configure(ch, new Context());

    Context context = new Context();
    context.put("port", String.valueOf(port));
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
    flumeHostsConfig.put("h1", "localhost:" + port);

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    FlumeConfig flumeConfig = new FlumeConfig();
    flumeConfig.backOff = false;
    flumeConfig.batchSize = 0;
    flumeConfig.clientType = ClientType.AVRO_FAILOVER;
    flumeConfig.connectionTimeout = 500;
    flumeConfig.flumeHostsConfig = flumeHostsConfig;
    flumeConfig.hostSelectionStrategy = HostSelectionStrategy.RANDOM;
    flumeConfig.maxBackOff = 0;
    flumeConfig.maxRetryAttempts = 0;
    flumeConfig.requestTimeout = 500;
    flumeConfig.singleEventPerBatch = false;
    flumeConfig.waitBetweenRetries = 0;

    FlumeConfigBean flumeConfigBean = new FlumeConfigBean();
    flumeConfigBean.flumeConfig = flumeConfig;
    flumeConfigBean.dataFormat = DataFormat.TEXT;
    flumeConfigBean.dataGeneratorFormatConfig = dataGeneratorFormatConfig;
    FlumeTarget flumeTarget = new FlumeTarget(flumeConfigBean);
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

    try {
      targetRunner.runInit();
      Assert.fail("Expected Stage Exception as configuration options are not valid");
    } catch (StageException e) {
      //batch size cannot be less than one
    }
  }

  @Test
  public void testWriteStringRecords() throws StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createDefaultFlumeConfig(port, false),
      DataFormat.TEXT,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

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

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createDefaultFlumeConfig(port, true),
      DataFormat.TEXT,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

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

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/name";
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createDefaultFlumeConfig(port, false),
      DataFormat.TEXT,
      dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

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

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/lastStatusChange";
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createDefaultFlumeConfig(port, false),
      DataFormat.TEXT,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

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

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/"; //MAP
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createDefaultFlumeConfig(port, false),
      DataFormat.TEXT,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget)
      .setOnRecordError(OnRecordError.TO_ERROR)
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

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createDefaultFlumeConfig(port, false),
      DataFormat.SDC_JSON,
      dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

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
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.csvFileFormat = CsvMode.CSV;
    dataGeneratorFormatConfig.csvHeader = CsvHeader.NO_HEADER;
    dataGeneratorFormatConfig.csvReplaceNewLines = false;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createDefaultFlumeConfig(port, false),
      DataFormat.DELIMITED,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

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

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.avroSchema = SdcAvroTestUtil.AVRO_SCHEMA1;
    dataGeneratorFormatConfig.avroSchemaSource = INLINE;
    dataGeneratorFormatConfig.includeSchema = true;
    dataGeneratorFormatConfig.avroCompression = AvroCompression.NULL;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createDefaultFlumeConfig(port, false),
      DataFormat.AVRO,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

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

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.avroSchema = SdcAvroTestUtil.AVRO_SCHEMA1;
    dataGeneratorFormatConfig.avroSchemaSource = INLINE;
    dataGeneratorFormatConfig.includeSchema = true;
    dataGeneratorFormatConfig.avroCompression = AvroCompression.NULL;
    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createDefaultFlumeConfig(port, true),
      DataFormat.AVRO,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

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

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.avroSchema = SdcAvroTestUtil.AVRO_SCHEMA1;
    dataGeneratorFormatConfig.avroSchemaSource = INLINE;
    dataGeneratorFormatConfig.includeSchema = false;
    dataGeneratorFormatConfig.avroCompression = AvroCompression.NULL;
    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createDefaultFlumeConfig(port, false),
      DataFormat.AVRO,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

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

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.avroSchema = SdcAvroTestUtil.AVRO_SCHEMA1;
    dataGeneratorFormatConfig.avroSchemaSource = INLINE;
    dataGeneratorFormatConfig.includeSchema = false;
    dataGeneratorFormatConfig.avroCompression = AvroCompression.NULL;
    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createDefaultFlumeConfig(port, true),
      DataFormat.AVRO,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

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

  @Test
  public void testWriteBinaryRecords() throws StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.binaryFieldPath = "/data";
    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
        FlumeTestUtil.createDefaultFlumeConfig(port, false),
        DataFormat.BINARY,
        dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createBinaryRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    for(Record r : logRecords) {
      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      Assert.assertTrue(Arrays.equals(r.get("/data").getValueAsByteArray(), event.getBody()));
      Assert.assertTrue(event.getHeaders().containsKey("charset"));
      Assert.assertEquals("UTF-8", event.getHeaders().get("charset"));
      transaction.commit();
      transaction.close();
    }
  }

}
