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
package com.streamsets.datacollector.email;

import com.google.common.collect.ImmutableList;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.GreenMailUtil;
import com.icegreen.greenmail.util.ServerSetup;
import com.streamsets.datacollector.util.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.mail.internet.MimeMultipart;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;

public class TestEmailSender {
  private static GreenMail server;

  @BeforeClass
  public static void setUp() throws Exception {
    ServerSetup serverSetup = new ServerSetup(getFreePort(), "localhost", "smtp");
    server = new GreenMail(serverSetup);
    server.setUser("user@x", "user", "password");
    server.start();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    server.stop();
  }

  public static int getFreePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();
    return port;
  }

  @Test
  public void testSendEmailNoAuth() throws Exception {
    Configuration conf = new Configuration();
    conf.set("mail.smtp.host", "localhost");
    conf.set("mail.smtp.port", Integer.toString(server.getSmtp().getPort()));
    EmailSender sender = new EmailSender(conf);
    sender.send(ImmutableList.of("foo", "bar"), "SUBJECT", "BODY");
    String headers = GreenMailUtil.getHeaders(server.getReceivedMessages()[0]);
    Assert.assertTrue(headers.contains("To: foo, bar"));
    Assert.assertTrue(headers.contains("Subject: SUBJECT"));
    Assert.assertTrue(headers.contains("From: sdc@localhost"));
    Assert.assertTrue(headers.contains("Content-Type: text/html; charset=UTF-8"));
    Assert.assertEquals("BODY", GreenMailUtil.getBody(server.getReceivedMessages()[0]));
    server.reset();
  }

  @Test
  public void testSentEmailWithAttachment() throws Exception {
    Configuration conf = new Configuration();
    conf.set("mail.smtp.host", "localhost");
    conf.set("mail.smtp.port", Integer.toString(server.getSmtp().getPort()));
    conf.set("mail.transport.protocol", "smtp");
    conf.set("mail.smtp.auth", "false");
    EmailSender sender = new EmailSender(conf);
    sender.send(ImmutableList.of("foo", "bar"), "SUBJECT", "BODY",
        Arrays.asList(
            new Attachment("hello.txt", new ByteArrayInputStream("hello world".getBytes()), "text/*"),
            new Attachment("world.txt", new ByteArrayInputStream("world hello".getBytes()), "text/*")
        )
    );
    String headers = GreenMailUtil.getHeaders(server.getReceivedMessages()[0]);
    Assert.assertTrue(headers.contains("To: foo, bar"));
    Assert.assertTrue(headers.contains("Subject: SUBJECT"));
    Assert.assertTrue(headers.contains("From: sdc@localhost"));
    Assert.assertTrue(headers.contains("Content-Type: multipart/mixed"));
    Assert.assertEquals("BODY", ((MimeMultipart)(server.getReceivedMessages()[0]).getContent()).getBodyPart(0).getContent());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ((MimeMultipart)(server.getReceivedMessages()[0]).getContent()).getBodyPart(1).getDataHandler().writeTo(os);
    Assert.assertEquals("hello world", os.toString());
    Assert.assertEquals("hello.txt", ((MimeMultipart)(server.getReceivedMessages()[0]).getContent()).getBodyPart(1).getDataHandler().getDataSource().getName());
    os.reset();
    ((MimeMultipart)(server.getReceivedMessages()[0]).getContent()).getBodyPart(2).getDataHandler().writeTo(os);
    Assert.assertEquals("world hello", os.toString());
    Assert.assertEquals("world.txt", ((MimeMultipart)(server.getReceivedMessages()[0]).getContent()).getBodyPart(2).getDataHandler().getDataSource().getName());
    server.reset();
  }
}
