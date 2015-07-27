/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestPipelineRuntimeException {

  @Test
  @SuppressWarnings("unchecked")
  public void testConstructor1() {
    Issues issues = new Issues();
    PipelineRuntimeException ex = new PipelineRuntimeException(issues);
    Assert.assertEquals(issues, ex.getIssues());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConstructor2() {
    PipelineRuntimeException ex = new PipelineRuntimeException(ContainerError.CONTAINER_0151, "foo");
    Assert.assertNull(ex.getIssues());
  }

  @Test
  public void testErrorMessage() {
    Assert.assertNotNull(ContainerError.CONTAINER_0151.getMessage());
  }

}
