/*
 * Copyright 2017 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.tcp;

import com.google.common.primitives.Bytes;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.parser.net.NetTestUtils;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogFramingMode;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogMessage;
import com.streamsets.pipeline.lib.tls.TlsConfigErrors;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.stage.util.tls.TLSTestUtils;
import com.streamsets.testing.NetworkUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.io.Charsets;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.KeyPair;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class TestTCPServerSource {

  public static final String TEN_DELIMITED_RECORDS = "one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\nnine\nten\n";
  public static final String SYSLOG_RECORD = "<42>Mar 24 17:18:10 10.1.2.34 Got an error";

  @Test
  public void syslogRecords() {

    Charset charset = Charsets.ISO_8859_1;

    final TCPServerSourceConfig configBean = createConfigBean(charset);
    TCPServerSource source = new TCPServerSource(configBean);

    List<Stage.ConfigIssue> issues = new LinkedList<>();
    EmbeddedChannel ch = new EmbeddedChannel(source.buildByteBufToMessageDecoderChain(issues).toArray(new ChannelHandler[0]));

    ch.writeInbound(Unpooled.copiedBuffer(SYSLOG_RECORD + configBean.nonTransparentFramingSeparatorCharStr, charset));

    assertSyslogRecord(ch);
    assertFalse(ch.finishAndReleaseAll());

    configBean.syslogFramingMode = SyslogFramingMode.OCTET_COUNTING;
    EmbeddedChannel ch2 = new EmbeddedChannel(source.buildByteBufToMessageDecoderChain(issues).toArray(new ChannelHandler[0]));

    ch2.writeInbound(Unpooled.copiedBuffer(SYSLOG_RECORD.length() + " " + SYSLOG_RECORD, charset));

    assertSyslogRecord(ch2);
    assertFalse(ch2.finishAndReleaseAll());
  }

  private void assertSyslogRecord(EmbeddedChannel ch) {
    Object in1 = ch.readInbound();
    assertThat(in1, notNullValue());
    assertThat(in1, instanceOf(SyslogMessage.class));
    SyslogMessage msg1 = (SyslogMessage) in1;
    assertThat(msg1.getHost(), equalTo("10.1.2.34"));
    assertThat(msg1.getRemainingMessage(), equalTo("Got an error"));
    assertThat(msg1.getPriority(), equalTo(42));
    assertThat(msg1.getFacility(), equalTo(5));
    assertThat(msg1.getSeverity(), equalTo(2));
  }

  @Test
  public void initMethod() throws Exception {

    final TCPServerSourceConfig configBean = createConfigBean(Charsets.ISO_8859_1);

    List<Stage.ConfigIssue> issues1 = initSourceAndGetIssues(configBean);
    assertThat(issues1, hasSize(0));

    // empty ports
    configBean.ports = new LinkedList<>();
    List<Stage.ConfigIssue> issues2 = initSourceAndGetIssues(configBean);
    assertThat(issues2, hasSize(1));
    assertThat(issues2.get(0).toString(), containsString(Errors.TCP_02.getCode()));

    // invalid ports
    // too large
    configBean.ports = Arrays.asList("123456789");
    List<Stage.ConfigIssue> issues3 = initSourceAndGetIssues(configBean);
    assertThat(issues3, hasSize(1));
    assertThat(issues3.get(0).toString(), containsString(Errors.TCP_03.getCode()));
    // not a number
    configBean.ports = Arrays.asList("abcd");
    List<Stage.ConfigIssue> issues4 = initSourceAndGetIssues(configBean);
    assertThat(issues4, hasSize(1));
    assertThat(issues4.get(0).toString(), containsString(Errors.TCP_03.getCode()));

    // start TLS config tests
    configBean.ports = Arrays.asList("9876");
    configBean.tlsEnabled = true;
    configBean.tlsConfigBean.hasKeyStore = true;
    List<Stage.ConfigIssue> issues5 = initSourceAndGetIssues(configBean);
    assertThat(issues5, hasSize(1));
    assertThat(issues5.get(0).toString(), containsString(TlsConfigErrors.TLS_02.getCode()));

    configBean.tlsConfigBean.hasKeyStore = true;
    configBean.tlsConfigBean.keyStoreFilePath = "non-existent-file-path";
    List<Stage.ConfigIssue> issues6 = initSourceAndGetIssues(configBean);
    assertThat(issues6, hasSize(1));
    assertThat(issues6.get(0).toString(), containsString(TlsConfigErrors.TLS_01.getCode()));

    File blankTempFile = File.createTempFile("blank", "txt");
    blankTempFile.deleteOnExit();
    configBean.tlsConfigBean.keyStoreFilePath = blankTempFile.getAbsolutePath();
    List<Stage.ConfigIssue> issues7 = initSourceAndGetIssues(configBean);
    assertThat(issues7, hasSize(1));
    assertThat(issues7.get(0).toString(), containsString(TlsConfigErrors.TLS_21.getCode()));

    // now, try with real keystore
    String hostname = TLSTestUtils.getHostname();
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    testDir.deleteOnExit();
    final File keyStore = new File(testDir, "keystore.jks");
    keyStore.deleteOnExit();
    Assert.assertTrue(testDir.mkdirs());
    final String keyStorePassword = "keystore";
    KeyPair keyPair = TLSTestUtils.generateKeyPair();
    Certificate cert = TLSTestUtils.generateCertificate("CN=" + hostname, keyPair, 30);
    TLSTestUtils.createKeyStore(keyStore.toString(), keyStorePassword, "web", keyPair.getPrivate(), cert);

    configBean.tlsConfigBean.keyStoreFilePath = keyStore.getAbsolutePath();
    configBean.tlsConfigBean.keyStorePassword = "invalid-password";

    List<Stage.ConfigIssue> issues9 = initSourceAndGetIssues(configBean);
    assertThat(issues9, hasSize(1));
    assertThat(issues9.get(0).toString(), containsString(TlsConfigErrors.TLS_21.getCode()));

    // finally, a valid certificate/config
    configBean.tlsConfigBean.keyStorePassword = keyStorePassword;
    List<Stage.ConfigIssue> issues10 = initSourceAndGetIssues(configBean);
    assertThat(issues10, hasSize(0));
  }

  @Test
  public void runTextRecordsWithAck() throws StageException, IOException, ExecutionException, InterruptedException {

    final Charset charset = Charsets.ISO_8859_1;
    final TCPServerSourceConfig configBean = createConfigBean(charset);
    configBean.dataFormat = DataFormat.TEXT;
    configBean.tcpMode = TCPMode.DELIMITED_RECORDS;
    configBean.recordSeparatorStr = "\n";
    configBean.ports = NetworkUtils.getRandomPorts(1);
    configBean.recordProcessedAckMessage = "record_ack_${record:id()}";
    configBean.batchCompletedAckMessage = "batch_ack_${batchSize}";

    final TCPServerSource source = new TCPServerSource(configBean);
    final String outputLane = "lane";
    final PushSourceRunner runner = new PushSourceRunner.Builder(TCPServerDSource.class, source)
        .addOutputLane(outputLane)
        .build();

    final String[] expectedRecords = TEN_DELIMITED_RECORDS.split(configBean.recordSeparatorStr);
    final int batchSize = expectedRecords.length;

    final List<Record> records = new LinkedList<>();
    runner.runInit();

    EventLoopGroup workerGroup = new NioEventLoopGroup();

    ChannelFuture channelFuture = startTcpClient(
        configBean,
        workerGroup,
        TEN_DELIMITED_RECORDS.getBytes(charset),
        true
    );

    runner.runProduce(new HashMap<>(), batchSize, output -> {
      records.addAll(output.getRecords().get(outputLane));
      runner.setStop();
    });
    runner.waitOnProduce();

    // Wait until the connection is closed.
    final Channel channel = channelFuture.channel();
    TCPServerSourceClientHandler clientHandler = channel.pipeline().get(TCPServerSourceClientHandler.class);

    final List<String> responses = new LinkedList<>();
    for (int i = 0; i < batchSize + 1; i++) {
      // one for each record, plus one for the batch
      responses.add(clientHandler.getResponse());
    }

    channel.close();

    workerGroup.shutdownGracefully();

    assertThat(records, hasSize(batchSize));
    for (int i = 0; i < records.size(); i++) {
      // validate the output record value
      assertThat(records.get(i).get("/text").getValueAsString(), equalTo(expectedRecords[i]));
      // validate the record-level ack
      assertThat(responses.get(i), equalTo(String.format("record_ack_%s", records.get(i).getHeader().getSourceId())));
    }
    // validate the batch-level ack
    assertThat(responses.get(10), equalTo(String.format("batch_ack_%d", batchSize)));
  }

  @Test
  public void errorHandling() throws StageException, IOException, ExecutionException, InterruptedException {

    final Charset charset = Charsets.ISO_8859_1;
    final TCPServerSourceConfig configBean = createConfigBean(charset);
    configBean.dataFormat = DataFormat.JSON;
    configBean.tcpMode = TCPMode.DELIMITED_RECORDS;
    configBean.recordSeparatorStr = "\n";
    configBean.ports = NetworkUtils.getRandomPorts(1);

    final TCPServerSource source = new TCPServerSource(configBean);
    final String outputLane = "lane";
    final PushSourceRunner toErrorRunner = new PushSourceRunner.Builder(TCPServerDSource.class, source)
        .addOutputLane(outputLane)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    final List<Record> records = new LinkedList<>();
    final List<Record> errorRecords = new LinkedList<>();
    runAndCollectRecords(
        toErrorRunner,
        configBean,
        records,
        errorRecords,
        1,
        outputLane,
        "{\"invalid_json\": yes}\n".getBytes(charset),
        true,
        false
    );

    assertThat(records, empty());
    assertThat(errorRecords, hasSize(1));
    assertThat(
        errorRecords.get(0).getHeader().getErrorCode(),
        equalTo(com.streamsets.pipeline.lib.parser.Errors.DATA_PARSER_04.getCode())
    );

    final PushSourceRunner discardRunner = new PushSourceRunner.Builder(TCPServerDSource.class, source)
        .addOutputLane(outputLane)
        .setOnRecordError(OnRecordError.DISCARD)
        .build();
    records.clear();
    errorRecords.clear();

    configBean.ports = NetworkUtils.getRandomPorts(1);
    runAndCollectRecords(
        discardRunner,
        configBean,
        records,
        errorRecords,
        1,
        outputLane,
        "{\"invalid_json\": yes}\n".getBytes(charset),
        true,
        false
    );
    assertThat(records, empty());
    assertThat(errorRecords, empty());

    configBean.ports = NetworkUtils.getRandomPorts(1);
    final PushSourceRunner stopPipelineRunner = new PushSourceRunner.Builder(TCPServerDSource.class, source)
        .addOutputLane(outputLane)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    records.clear();
    errorRecords.clear();
    try {
      runAndCollectRecords(
          stopPipelineRunner,
          configBean,
          records,
          errorRecords,
          1,
          outputLane,
          "{\"invalid_json\": yes}\n".getBytes(charset),
          true,
          true
      );
      Assert.fail("ExecutionException should have been thrown");
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(RuntimeException.class));
      final RuntimeException runtimeException = (RuntimeException) e.getCause();
      assertThat(runtimeException.getCause(), instanceOf(StageException.class));
      final StageException stageException = (StageException) runtimeException.getCause();
      assertThat(stageException.getErrorCode().getCode(), equalTo(Errors.TCP_06.getCode()));
    }
  }

  private void runAndCollectRecords(
      PushSourceRunner runner,
      TCPServerSourceConfig configBean,
      List<Record> records,
      List<Record> errorRecords,
      int batchSize,
      String outputLane,
      byte[] data,
      boolean randomlySlice,
      boolean runEmptyProduceAtEnd
  ) throws StageException, InterruptedException, ExecutionException {

    runner.runInit();

    EventLoopGroup workerGroup = new NioEventLoopGroup();

    runner.runProduce(new HashMap<>(), batchSize, output -> {
      records.addAll(output.getRecords().get(outputLane));
      if (!runEmptyProduceAtEnd) {
        runner.setStop();
      }
    });

    ChannelFuture channelFuture = startTcpClient(
        configBean,
        workerGroup,
        data,
        randomlySlice
    );

    // Wait until the connection is closed.
    channelFuture.channel().closeFuture().sync();

    // wait for the push source runner produce to complete
    runner.waitOnProduce();

    errorRecords.addAll(runner.getErrorRecords());

    if (runEmptyProduceAtEnd) {
      runner.runProduce(new HashMap<>(), 0, output -> {
        runner.setStop();
      });
      runner.waitOnProduce();
    }

    runner.runDestroy();
    workerGroup.shutdownGracefully();
  }

  private ChannelFuture startTcpClient(
      TCPServerSourceConfig configBean,
      EventLoopGroup workerGroup,
      byte[] data,
      boolean randomlySlice
  ) throws
      InterruptedException {
    ChannelFuture channelFuture;
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup);
    bootstrap.channel(NioSocketChannel.class);
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.handler(new ChannelInitializer() {
      @Override
      protected void initChannel(Channel ch) throws Exception {
        ch.pipeline().addLast(new TCPServerSourceClientHandler(randomlySlice, data));
      }
    });

    // Start the client.
    channelFuture = bootstrap.connect("localhost", Integer.parseInt(configBean.ports.get(0))).sync();

    return channelFuture;
  }

  private static class TCPServerSourceClientHandler extends ChannelInboundHandlerAdapter {
    private final boolean randomlySlice;
    private final byte[] data;

    private final BlockingQueue<String> responses = new LinkedBlockingDeque<>();

    private TCPServerSourceClientHandler(boolean randomlySlice, byte[] data) {
      this.randomlySlice = randomlySlice;
      this.data = data;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      ByteBuf buf = (ByteBuf) msg;
      responses.add(buf.toString(com.google.common.base.Charsets.UTF_8));
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      if (randomlySlice) {
        for (List<Byte> slice : NetTestUtils.getRandomByteSlices(data)) {
          ctx.writeAndFlush(Unpooled.copiedBuffer(Bytes.toArray(slice)));
        }
      } else {
        ctx.writeAndFlush(Unpooled.copiedBuffer(data));
      }
    }

    private String getResponse() throws InterruptedException {
      return responses.take();
    }
  }

  private static List<Stage.ConfigIssue> initSourceAndGetIssues(TCPServerSourceConfig configBean) throws
      StageException {
    TCPServerSource source = new TCPServerSource(configBean);
    PushSourceRunner runner = new PushSourceRunner.Builder(TCPServerDSource.class, source)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    return runner.runValidateConfigs();
  }

  protected static TCPServerSourceConfig createConfigBean(Charset charset) {
    TCPServerSourceConfig config = new TCPServerSourceConfig();
    config.batchSize = 10;
    config.tlsEnabled = false;
    config.numThreads = 1;
    config.syslogCharset = charset.name();
    config.tcpMode = TCPMode.SYSLOG;
    config.syslogFramingMode= SyslogFramingMode.NON_TRANSPARENT_FRAMING;
    config.nonTransparentFramingSeparatorCharStr = "\n";
    config.maxMessageSize = 4096;
    config.ports = Arrays.asList("9876");
    return config;
  }
}
