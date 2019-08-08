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
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TestIssuesBean {

  @Test
  public void testIssuesBean() {

    Issues issues = new Issues();
    Issue stageIssue = new Issue(
        "s1",
        "serviceName",
        "group1",
        "config1",
        1,
        new ErrorMessage("errorCode", "message", System.currentTimeMillis()),
        Collections.emptyMap(),
        Collections.emptyList()
    );
    issues.add(stageIssue);
    IssuesJson issuesJsonBean = new IssuesJson(issues);

    Assert.assertEquals(issues.getIssueCount(), issuesJsonBean.getIssueCount());
    Assert.assertEquals(issues.getPipelineIssues(), BeanHelper.unwrapIssues(issuesJsonBean.getPipelineIssues()));
    Assert.assertEquals(issues.getStageIssues(), BeanHelper.unwrapIssuesMap(issuesJsonBean.getStageIssues()));

    // test underlying
    Assert.assertEquals(issues.getIssueCount(), issuesJsonBean.getIssues().getIssueCount());
    Assert.assertEquals(issues.getPipelineIssues(), issuesJsonBean.getIssues().getPipelineIssues());
    Assert.assertEquals(issues.getStageIssues(), issuesJsonBean.getIssues().getStageIssues());

    IssuesJson issuesJson = BeanHelper.wrapIssues(issues);
    Issues backToIssues = BeanHelper.unwrapIssues(issuesJson);
    Assert.assertEquals(backToIssues.getIssueCount(), issuesJson.getIssues().getIssueCount());
    Assert.assertEquals(backToIssues.getPipelineIssues(), issuesJson.getIssues().getPipelineIssues());
    Assert.assertEquals(backToIssues.getStageIssues(), issuesJson.getIssues().getStageIssues());

  }
}
