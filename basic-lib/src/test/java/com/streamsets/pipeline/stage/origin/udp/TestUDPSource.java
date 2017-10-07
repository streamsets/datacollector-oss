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
package com.streamsets.pipeline.stage.origin.udp;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DatagramMode;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowDataParserFactory;
import com.streamsets.pipeline.lib.parser.net.raw.RawDataMode;
import com.streamsets.pipeline.lib.parser.udp.ParserConfig;
import com.streamsets.pipeline.lib.parser.udp.ParserConfigKey;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.testing.NetworkUtils;
import io.netty.channel.epoll.Epoll;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.CHARSET;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.NETFLOW_MAX_TEMPLATE_CACHE_SIZE;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.NETFLOW_OUTPUT_VALUES_MODE;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.NETFLOW_TEMPLATE_CACHE_TIMEOUT_MS;
import static org.junit.Assume.assumeTrue;

public class TestUDPSource {
  private static final String TEN_PACKETS_RESOURCE = "netflow-v5-file-1";
  private static final Logger LOG = LoggerFactory.getLogger(TestUDPSource.class);

  @Before
  public void setup() throws Exception {
  }

  public static class TUDPSource extends UDPSource {

    boolean produceCalled;

    public TUDPSource(List<String> ports, boolean enableEpoll, int numThreads, ParserConfig parserConfig, DatagramMode dataFormat, int maxBatchSize, long maxWaitTime) {
      super(ports, enableEpoll, numThreads, parserConfig, dataFormat, maxBatchSize, maxWaitTime);
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      produceCalled = true;
      return super.produce(lastSourceOffset, maxBatchSize, batchMaker);
    }
  }

  /**
   * This test runs three times because UDP is delivery is not
   * guaranteed. If it fails all three times, then we actually fail.
   */
  @Test
  public void testBasic() throws Exception {
    int maxRuns = 3;
    List<AssertionError> failures = new ArrayList<>();
    for (int i = 0; i < maxRuns; i++) {
      try {
        doBasicTest(DatagramMode.NETFLOW, false, 1);
        doBasicTest(DatagramMode.SYSLOG, false, 1);
        doBasicTest(DatagramMode.RAW_DATA, false, 1);
      } catch (Exception ex) {
        // we don't expect exceptions to be thrown,
        // even when udp messages are lost
        throw ex;
      } catch (AssertionError failure) {
        String msg = "Test failed on iteration: " + i;
        LOG.error(msg, failure);
        failures.add(failure);
        Assert.assertTrue("Interrupted while sleeping", ThreadUtil.sleep(10 * 1000));
      }
    }
    if (failures.size() >= maxRuns) {
      throw failures.get(0);
    }
  }

  @Test
  public void testBasicEpoll() throws Exception {
    assumeTrue(Epoll.isAvailable());
    int maxRuns = 3;
    List<AssertionError> failures = new ArrayList<>();
    for (int i = 0; i < maxRuns; i++) {
      try {
        doBasicTest(DatagramMode.NETFLOW, true, 2);
        doBasicTest(DatagramMode.SYSLOG, true, 2);
      } catch (Exception ex) {
        // we don't expect exceptions to be thrown,
        // even when udp messages are lost
        throw ex;
      } catch (AssertionError failure) {
        String msg = "Test failed on iteration: " + i;
        LOG.error(msg, failure);
        failures.add(failure);
        Assert.assertTrue("Interrupted while sleeping", ThreadUtil.sleep(10 * 1000));
      }
    }
    if (failures.size() >= maxRuns) {
      throw failures.get(0);
    }
  }

  @Test
  public void testPrivilegedPort() throws Exception {
    List<String> ports = Lists.newArrayList("514", "10000");
    ParserConfig parserConfig = new ParserConfig();
    parserConfig.put(CHARSET, "UTF-8");
    TUDPSource source = new TUDPSource(ports, false, 1, parserConfig, DatagramMode.SYSLOG, 20, 100L);
    SourceRunner runner = new SourceRunner.Builder(TUDPSource.class, source).addOutputLane("lane").build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    if (System.getProperty("user.name").equals("root")) {
      Assert.assertEquals(0, issues.size());
    } else {
      Assert.assertEquals(1, issues.size());
    }
  }

  private void doBasicTest(DatagramMode dataFormat, boolean enableEpoll, int numThreads) throws Exception {
    List<String> ports = NetworkUtils.getRandomPorts(2);
    ParserConfig parserConfig = new ParserConfig();
    parserConfig.put(CHARSET, "UTF-8");
    SourceRunner runner = null;
    try {
      byte[] bytes = null;
      switch (dataFormat) {
        case NETFLOW:
          parserConfig.put(NETFLOW_OUTPUT_VALUES_MODE, NetflowDataParserFactory.DEFAULT_OUTPUT_VALUES_MODE);
          parserConfig.put(NETFLOW_MAX_TEMPLATE_CACHE_SIZE, NetflowDataParserFactory.DEFAULT_MAX_TEMPLATE_CACHE_SIZE);
          parserConfig.put(
              NETFLOW_TEMPLATE_CACHE_TIMEOUT_MS,
              NetflowDataParserFactory.DEFAULT_TEMPLATE_CACHE_TIMEOUT_MS
          );
          InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(TEN_PACKETS_RESOURCE);
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          IOUtils.copy(is, baos);
          is.close();
          baos.close();
          bytes = baos.toByteArray();
          break;
        case SYSLOG:
          bytes = "<34>1 2003-10-11T22:14:15.003Z mymachine.example.com some syslog data".
            getBytes(StandardCharsets.UTF_8);
          break;
        case RAW_DATA:
          bytes = "some,separated,data".getBytes(StandardCharsets.UTF_8);
          parserConfig.put(ParserConfigKey.RAW_DATA_MODE, RawDataMode.CHARACTER);
          parserConfig.put(
              ParserConfigKey.RAW_DATA_MULTIPLE_VALUES_BEHAVIOR,
              MultipleValuesBehavior.SPLIT_INTO_MULTIPLE_RECORDS
          );
          parserConfig.put(ParserConfigKey.RAW_DATA_SEPARATOR_BYTES, ",".getBytes(StandardCharsets.UTF_8));
          parserConfig.put(ParserConfigKey.RAW_DATA_OUTPUT_FIELD_PATH, "/data");

          break;
        default:
          Assert.fail("Unknown data format: " + dataFormat);
      }
      TUDPSource source = new TUDPSource(ports, enableEpoll, numThreads, parserConfig, dataFormat, 20, 100L);
      runner = new SourceRunner.Builder(TUDPSource.class, source).addOutputLane("lane").build();
      runner.runInit();

      for (String port : ports) {
        DatagramSocket clientSocket = new DatagramSocket();
        InetAddress address = InetAddress.getLoopbackAddress();
        DatagramPacket sendPacket = new DatagramPacket(bytes, bytes.length, address, Integer.parseInt(port));
        clientSocket.send(sendPacket);
        clientSocket.close();
      }

      StageRunner.Output output = runner.runProduce(null, 6);
      Assert.assertTrue(source.produceCalled);
      List<Record> records = output.getRecords().get("lane");
      switch (dataFormat) {
        case NETFLOW:
          Assert.assertEquals(String.valueOf(records), 6, records.size());
          break;
        case SYSLOG:
          Assert.assertEquals(String.valueOf(records), ports.size(), records.size());
          break;
        case RAW_DATA:
          Assert.assertEquals(String.valueOf(records), 3 * ports.size(), records.size());
          break;
        default:
          Assert.fail("Unknown data format: " + dataFormat);
      }
      output = runner.runProduce(null, 14);
      Assert.assertTrue(source.produceCalled);
      records = output.getRecords().get("lane");
      switch (dataFormat) {
        case NETFLOW:
          Assert.assertEquals(String.valueOf(records), 14, records.size());
          break;
        case SYSLOG:
          Assert.assertEquals(String.valueOf(records), 0, records.size());
          break;
        case RAW_DATA:
          Assert.assertEquals(String.valueOf(records), 0, records.size());
          break;
        default:
          Assert.fail("Unknown data format: " + dataFormat);
      }
      output = runner.runProduce(null, 1);
      Assert.assertTrue(source.produceCalled);
      records = output.getRecords().get("lane");
      Assert.assertEquals(String.valueOf(records), 0, records.size());
    } finally {
      if (runner != null) {
        runner.runDestroy();
      }
    }
  }
}
