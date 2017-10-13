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

import com.google.common.base.Charsets;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DatagramMode;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowTestUtil;
import com.streamsets.pipeline.lib.parser.net.raw.RawDataMode;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogMessage;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.testing.NetworkUtils;
import io.netty.channel.epoll.Epoll;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.junit.Assume.assumeTrue;

public abstract class BaseUDPSourceTest {

  private static final String TEN_PACKETS_RESOURCE = "netflow-v5-file-1";
  private static final Logger LOG = LoggerFactory.getLogger(TestMultithreadedUDPSource.class);

  protected static final String OUTPUT_LANE = "lane";

  public static final int NUM_NETFLOW_RECORDS_PER_MSG = 10;

  private static final String RAW_SYSLOG_STR = "<34>1 2003-10-11T22:14:15.003Z mymachine.example.com some syslog data";

  private static final String[] RAW_DATA_VALUES = new String[] {"some", "separated", "data"};
  private static final String RAW_DATA_SEPARATOR = ",";
  private static final String RAW_DATA_OUTPUT_FIELD = "/data";

  protected final String UTF8 = Charsets.UTF_8.name();

  /**
   * This test runs three times because UDP is delivery is not
   * guaranteed. If it fails all three times, then we actually fail.
   */
  @Test
  public void testBasic() throws Exception {
    testIterator(1, false);
  }

  public void testIterator(int numThreads, boolean epoll) throws Exception {
    int maxRuns = 3;
    List<AssertionError> failures = new ArrayList<>();

    EnumSet<DatagramMode> remainingFormats = EnumSet.of(
        DatagramMode.NETFLOW,
        DatagramMode.SYSLOG,
        DatagramMode.RAW_DATA
    );

    for (int i = 0; i < maxRuns; i++) {
      try {
        EnumSet<DatagramMode> succeededFormats = EnumSet.noneOf(DatagramMode.class);
        for (DatagramMode mode : remainingFormats) {
          doBasicTest(mode, epoll, numThreads);
          succeededFormats.add(mode);
        }
        remainingFormats.removeAll(succeededFormats);
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
    testIterator(2, true);
  }

  private void doBasicTest(DatagramMode dataFormat, boolean enableEpoll, int numThreads) throws Exception {

    List<String> ports = NetworkUtils.getRandomPorts(2);
    final UDPSourceConfigBean conf = new UDPSourceConfigBean();

    conf.ports = ports;
    conf.enableEpoll = enableEpoll;
    conf.numThreads = 1;
    conf.dataFormat = dataFormat;
    conf.maxWaitTime = 1000;
    conf.batchSize = 1000;
    conf.collectdCharset = UTF8;
    conf.syslogCharset = UTF8;
    conf.rawDataCharset = UTF8;

    try {
      byte[] bytes = null;
      switch (dataFormat) {
        case NETFLOW:
          InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(TEN_PACKETS_RESOURCE);
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          IOUtils.copy(is, baos);
          is.close();
          baos.close();
          bytes = baos.toByteArray();
          break;
        case SYSLOG:
          bytes = RAW_SYSLOG_STR.getBytes(UTF8);

          conf.syslogCharset = UTF8;
          break;
        case RAW_DATA:
          bytes = StringUtils.join(RAW_DATA_VALUES, RAW_DATA_SEPARATOR).getBytes(UTF8);

          conf.rawDataSeparatorBytes = RAW_DATA_SEPARATOR;
          conf.rawDataOutputField = RAW_DATA_OUTPUT_FIELD;
          conf.rawDataCharset = UTF8;
          conf.rawDataMode = RawDataMode.CHARACTER;
          break;
        default:
          Assert.fail("Unknown data format: " + dataFormat);
      }

      initializeRunner(conf, numThreads);

      for (String port : ports) {
        DatagramSocket clientSocket = new DatagramSocket();
        InetAddress address = InetAddress.getLoopbackAddress();
        DatagramPacket sendPacket = new DatagramPacket(bytes, bytes.length, address, Integer.parseInt(port));
        clientSocket.send(sendPacket);
        clientSocket.close();
      }

      runProduce(dataFormat, ports);

    } finally {
      destroyRunner();
    }
  }

  protected int getNumTotalExpectedRecords(DatagramMode dataFormat, List<String> ports) {
    switch (dataFormat) {
      case NETFLOW:
        // each packet contains ten records
        return ports.size() * NUM_NETFLOW_RECORDS_PER_MSG;
      case SYSLOG:
        // each packet has only one record
        return ports.size();
      case RAW_DATA:
        // each packet has however many raw data values there are
        return ports.size() * RAW_DATA_VALUES.length;
      default:
        Assert.fail("Unknown data format: " + dataFormat);
    }
    return -1;
  }

  protected void assertRecordsInBatch(DatagramMode dataFormat, int startingRecordNum, List<Record> records) {
    switch (dataFormat) {
      case NETFLOW:
        NetflowTestUtil.assertRecordsForTenPackets(records, startingRecordNum, records.size());
        break;
      case SYSLOG:
        // all records are identical, the single Syslog message
        for (Record record : records) {
          Assert.assertEquals(RAW_SYSLOG_STR, record.get("/" + SyslogMessage.FIELD_RAW).getValueAsString());
        }
        break;
      case RAW_DATA:
        // records cycle between the raw values strings
        for (int i = 0; i < records.size(); i++) {
          final int recordIndex = i + startingRecordNum;
          Assert.assertEquals(
              RAW_DATA_VALUES[recordIndex % RAW_DATA_VALUES.length],
              records.get(i).get(RAW_DATA_OUTPUT_FIELD).getValueAsString()
          );
        }
        break;
      default:
        Assert.fail("Unknown data format: " + dataFormat);
    }
  }

  protected List<Record> getOutputRecords(StageRunner.Output output) {
    return output.getRecords().get(OUTPUT_LANE);
  }

  protected abstract void initializeRunner(UDPSourceConfigBean conf, int numThreads) throws StageException;

  protected abstract void runProduce(
      DatagramMode dataFormat,
      List<String> ports
  ) throws Exception;

  protected abstract void destroyRunner() throws StageException;

}
