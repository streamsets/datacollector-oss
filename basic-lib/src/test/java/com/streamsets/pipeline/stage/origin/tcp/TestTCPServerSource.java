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

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogFramingMode;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogMessage;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.commons.io.Charsets;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class TestTCPServerSource {

  @Test
  public void syslogRecords() {

    Charset charset = Charsets.ISO_8859_1;

    TCPServerSource source = new TCPServerSource(createConfigBean(charset));

    List<Stage.ConfigIssue> issues = new LinkedList<>();
    EmbeddedChannel ch = new EmbeddedChannel(source.buildByteBufToMessageDecoderChain(issues).toArray(new ChannelHandler[0]));

    ch.writeInbound(Unpooled.copiedBuffer("<42>Mar 24 17:18:10 10.1.2.34 Got an error\n", charset));

    Object in1 = ch.readInbound();
    assertThat(in1, notNullValue());
    assertThat(in1, instanceOf(SyslogMessage.class));
    SyslogMessage msg1 = (SyslogMessage) in1;
    assertThat(msg1.getHost(), equalTo("10.1.2.34"));
    assertThat(msg1.getRemainingMessage(), equalTo("Got an error"));
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
    return config;
  }
}
