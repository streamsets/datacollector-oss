/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.streamsets.pipeline.validation.Issues;
import org.junit.Assert;
import org.junit.Test;

public class TestIssuesBean {

  @Test
  public void testIssuesBean() {

    com.streamsets.pipeline.validation.Issues issues = new Issues();
    IssuesJson issuesJsonBean = new IssuesJson(issues);

    Assert.assertEquals(issues.getIssueCount(), issuesJsonBean.getIssueCount());
    Assert.assertEquals(issues.getPipelineIssues(), BeanHelper.unwrapIssues(issuesJsonBean.getPipelineIssues()));
    Assert.assertEquals(issues.getStageIssues(), BeanHelper.unwrapStageIssuesMap(issuesJsonBean.getStageIssues()));

    //test underlying
    Assert.assertEquals(issues.getIssueCount(), issuesJsonBean.getIssues().getIssueCount());
    Assert.assertEquals(issues.getPipelineIssues(), issuesJsonBean.getIssues().getPipelineIssues());
    Assert.assertEquals(issues.getStageIssues(), issuesJsonBean.getIssues().getStageIssues());

  }
}
