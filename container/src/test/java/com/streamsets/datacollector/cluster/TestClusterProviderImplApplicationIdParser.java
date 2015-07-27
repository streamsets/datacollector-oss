/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.cluster;

import org.junit.Assert;
import org.junit.Test;

import com.streamsets.datacollector.cluster.ClusterProviderImpl;

import java.util.regex.Matcher;

public class TestClusterProviderImplApplicationIdParser {


  @Test
  public void testInvalidYARNAppId() throws Exception {
    Matcher matcher;
    matcher = ClusterProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher("");
    Assert.assertFalse(matcher.find());
    matcher = ClusterProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher("application_1429587312661_0024");
    Assert.assertFalse(matcher.find());
    matcher = ClusterProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher("_application_1429587312661_0024_");
    Assert.assertFalse(matcher.find());
    matcher = ClusterProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher(" pplication_1429587312661_0024 ");
    Assert.assertFalse(matcher.find());
    matcher = ClusterProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher(" application_1429587312661_00a24 ");
    Assert.assertFalse(matcher.find());
  }

  @Test
  public void testValidYARNAppId() throws Exception {
    Matcher matcher;
    matcher = ClusterProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher("15/04/21 21:15:20 INFO Client: Application report for application_1429587312661_0024 (state: RUNNING)");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
    matcher = ClusterProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher(" application_1429587312661_0024 ");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
    matcher = ClusterProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher("\tapplication_1429587312661_0024\t");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
    matcher = ClusterProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher(" application_11111111111111111_9999924 ");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_11111111111111111_9999924", matcher.group(1));
    matcher = ClusterProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher("\tapplication_1429587312661_0024");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
  }
}
