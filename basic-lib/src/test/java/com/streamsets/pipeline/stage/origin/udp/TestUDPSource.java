/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class TestUDPSource {
  private static final File TEN_PACKETS = new File(System.getProperty("user.dir") +
    "/src/test/resources/netflow-v5-file-1");
  private static final Logger LOG = LoggerFactory.getLogger(TestUDPSource.class);

  @Before
  public void setup() throws Exception {
  }

  private List<String> genPorts() throws Exception {
    List<String> ports = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      ServerSocket socket = new ServerSocket(0);
      ports.add(String.valueOf(socket.getLocalPort()));
      socket.close();;
    }
    return ports;
  }

  public static class TUDPSource extends UDPSource {

    boolean produceCalled;

    public TUDPSource(List<String> ports, int maxBatchSize, long maxWaitTime) {
      super(ports, maxBatchSize, maxWaitTime);
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
        doBasicTest();
      } catch (Exception ex) {
        // we don't expect exceptions to be thrown,
        // even when udp messages are lost
        throw ex;
      } catch (AssertionError failure) {
        String msg = "Test failed on iteration: " + i;
        LOG.error(msg, failure);
        failures.add(failure);
        Assert.assertTrue("Interrupted while sleeping", ThreadUtil.sleep(30 * 1000));
      }
    }
    if (failures.size() >= maxRuns) {
      throw failures.get(0);
    }
  }

  private void doBasicTest() throws Exception {
    List<String> ports = genPorts();
    TUDPSource source = new TUDPSource(ports, 20, 100L);
    SourceRunner runner = new SourceRunner.Builder(TUDPSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      for (String port : ports) {
        DatagramSocket clientSocket = new DatagramSocket();
        InetAddress address = InetAddress.getLoopbackAddress();
        byte[] bytes = Files.readAllBytes(TEN_PACKETS.toPath());
        DatagramPacket sendPacket = new DatagramPacket(bytes, bytes.length, address, Integer.parseInt(port));
        clientSocket.send(sendPacket);
        clientSocket.close();
      }
      StageRunner.Output output = runner.runProduce(null, 6);
      Assert.assertTrue(source.produceCalled);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(String.valueOf(records), 6, records.size());

      output = runner.runProduce(null, 14);
      Assert.assertTrue(source.produceCalled);
      records = output.getRecords().get("lane");
      Assert.assertEquals(String.valueOf(records), 14, records.size());

      output = runner.runProduce(null, 1);
      Assert.assertTrue(source.produceCalled);
      records = output.getRecords().get("lane");
      Assert.assertEquals(String.valueOf(records), 0, records.size());
    } finally {
      runner.runDestroy();
    }
  }
}
