/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.spooldir.PostProcessingOptions;
import com.streamsets.pipeline.stage.origin.spooldir.SpoolDirSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestUDPSource {
  private static final File TEN_PACKETS = new File(System.getProperty("user.dir") +
    "/src/test/resources/netflow-v5-file-1");
  private List<String> ports;

  @Before
  public void setup() throws Exception {
    ports = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      ServerSocket socket = new ServerSocket(0);
      ports.add(String.valueOf(socket.getLocalPort()));
      socket.close();;
    }
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

  @Test
  public void testBasic() throws Exception {
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
