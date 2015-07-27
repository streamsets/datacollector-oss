/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.IssuesJson;
import com.streamsets.datacollector.validation.Issues;

import org.junit.Assert;
import org.junit.Test;

public class TestIssuesBean {

  @Test
  public void testIssuesBean() {

    com.streamsets.datacollector.validation.Issues issues = new Issues();
    IssuesJson issuesJsonBean = new IssuesJson(issues);

    Assert.assertEquals(issues.getIssueCount(), issuesJsonBean.getIssueCount());
    Assert.assertEquals(issues.getPipelineIssues(), BeanHelper.unwrapIssues(issuesJsonBean.getPipelineIssues()));
    Assert.assertEquals(issues.getStageIssues(), BeanHelper.unwrapIssuesMap(issuesJsonBean.getStageIssues()));

    //test underlying
    Assert.assertEquals(issues.getIssueCount(), issuesJsonBean.getIssues().getIssueCount());
    Assert.assertEquals(issues.getPipelineIssues(), issuesJsonBean.getIssues().getPipelineIssues());
    Assert.assertEquals(issues.getStageIssues(), issuesJsonBean.getIssues().getStageIssues());

  }
}
