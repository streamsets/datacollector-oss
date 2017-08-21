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
package com.streamsets.datacollector.creation;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.streamsets.datacollector.MiniSDC;
import com.streamsets.datacollector.MiniSDCTestingUtility;
import com.streamsets.pipeline.validation.ValidationIssue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestPipelineImportIT {

  private static final String PIPELINE_ERROR_HANDLING_UNDEFINED = "CREATION_009";
  private static final String PIPELINE_FILES_PACKAGE = "com/streamsets/testing/pipelines/";
  private static final Set<String> ACCEPTABLE_ERROR_CODES = Sets.newHashSet(PIPELINE_ERROR_HANDLING_UNDEFINED);

  // Will change with each test
  private String testPipeline;

  @Parameterized.Parameters(name = "{0}")
  public static Object[] data() throws Exception {
    Reflections reflections = new Reflections(PIPELINE_FILES_PACKAGE, new ResourcesScanner());
    Set<String> resourceList = reflections.getResources(Pattern.compile("pipeline\\.json"));
    return  resourceList.toArray(new String[resourceList.size()]);
  }

  public TestPipelineImportIT(String testPipeline) {
    this.testPipeline = testPipeline;
  }


  private static MiniSDCTestingUtility miniSDCTestingUtility;
  private static MiniSDC miniSDC;

  @BeforeClass
  public static void setUp() throws Exception {
    System.setProperty("sdc.testing-mode", "true");
    miniSDCTestingUtility = new MiniSDCTestingUtility();
    miniSDC = miniSDCTestingUtility.createMiniSDC(MiniSDC.ExecutionMode.STANDALONE);
    miniSDC.startSDC();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(miniSDCTestingUtility != null) {
      miniSDCTestingUtility.stopMiniSDC();
    }
  }

  @Test
  public void testPipelineImportIT() throws Exception {
      String pipelineJson = Resources.toString(Resources.getResource(testPipeline), StandardCharsets.UTF_8);
      final String pipelineName = getPipelineName(testPipeline);
      List<? extends ValidationIssue> issues = miniSDC.validatePipeline(pipelineName, pipelineJson);

      if (issues != null && issues.size() > 0) {
        Iterable<? extends ValidationIssue> filtered = Iterables.filter(issues, new Predicate<ValidationIssue>() {
          @Override
          public boolean apply(@Nullable ValidationIssue input) {
            return !ACCEPTABLE_ERROR_CODES.contains(input.getErrorCode());
          }
        });

        StringBuilder failedReason = new StringBuilder();
        boolean thisFailed = false;
        for (ValidationIssue issue : filtered) {
          if (failedReason.length() > 0) {
            failedReason.append(",\n");
          }
          failedReason.append(issue.getMessage());
          if (!Strings.isNullOrEmpty(issue.getConfigGroup())) {
            failedReason.append(", configGroup: ");
            failedReason.append(issue.getConfigGroup());
          }
          if (!Strings.isNullOrEmpty(issue.getConfigName())) {
            failedReason.append(", configName: ");
            failedReason.append(issue.getConfigName());
          }
          thisFailed = true;
        }

        if(thisFailed) {
          fail(failedReason.toString());
        }
      }

  }

  private String getPipelineName(String pipelineFile) {
    return pipelineFile.replaceAll("^.*"+PIPELINE_FILES_PACKAGE+"(.*)$", "$1");
  }
}
