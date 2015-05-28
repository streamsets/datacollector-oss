/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.email;

import com.google.common.collect.ImmutableList;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.GreenMailUtil;
import com.streamsets.pipeline.util.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEmailSender {
  private static GreenMail server;

  @BeforeClass
  public static void setUp() throws Exception {
    server = new GreenMail();
    server.setUser("user@x", "user", "password");
    server.start();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    server.stop();
  }

  @Test
  public void testSendEmailNoAuth() throws Exception {
    Configuration conf = new Configuration();
    conf.set("mail.smtp.host", "localhost");
    conf.set("mail.smtp.port", Integer.toString(server.getSmtp().getPort()));
    EmailSender sender = new EmailSender(conf);
    sender.send(ImmutableList.of("foo", "bar"), "SUBJECT", "BODY");
    String headers =GreenMailUtil.getHeaders(server.getReceivedMessages()[0]);
    Assert.assertTrue(headers.contains("To: foo, bar"));
    Assert.assertTrue(headers.contains("Subject: SUBJECT"));
    Assert.assertTrue(headers.contains("From: sdc@localhost"));
    Assert.assertTrue(headers.contains("Content-Type: text/plain; charset=UTF-8"));
    Assert.assertEquals("BODY", GreenMailUtil.getBody(server.getReceivedMessages()[0]));
  }

}
