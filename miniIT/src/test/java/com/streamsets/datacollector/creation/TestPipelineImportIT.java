/*
 * Copyright 2016 StreamSets Inc.
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

package com.streamsets.datacollector.creation;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.streamsets.datacollector.MiniSDC;
import com.streamsets.datacollector.MiniSDCTestingUtility;
import com.streamsets.pipeline.validation.ValidationIssue;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class TestPipelineImportIT {

  private static final String PIPELINE_ERROR_HANDLING_UNDEFINED = "CREATION_009";
  private static final String PIPELINE_FILES_PACKAGE = "com/streamsets/testing/pipelines/";
  private static final Set<String> ACCEPTABLE_ERROR_CODES = Sets.newHashSet(PIPELINE_ERROR_HANDLING_UNDEFINED);

  @Test
  public void testPipelineImportIT() throws Exception {
    System.setProperty("sdc.testing-mode", "true");
    MiniSDCTestingUtility miniSDCTestingUtility = new MiniSDCTestingUtility();
    try {
      MiniSDC miniSDC = miniSDCTestingUtility.createMiniSDC(MiniSDC.ExecutionMode.STANDALONE);
      miniSDC.startSDC();
      final Set<String> allPipelines = getAllImportPipelines();
      boolean anyFailed = false;
      final List<String> allFailedReasons = new LinkedList<>();
      for (final String pipeline : allPipelines) {

        String pipelineJson = Resources.toString(Resources.getResource(pipeline), StandardCharsets.UTF_8);
        final String pipelineName = getPipelineName(pipeline);
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
            thisFailed = true;
          }
          if (thisFailed) {
            anyFailed = true;
            allFailedReasons.add(pipelineName+":\n"+failedReason.toString());
          }
        }
      }

      if (anyFailed) {
        Assert.fail("Failed to successfully import all pipelines.  Errors:\n\n"
            + StringUtils.join(allFailedReasons, "\n\n"));
      }

    } finally {
      miniSDCTestingUtility.stopMiniSDC();
    }
  }

  private Set<String> getAllImportPipelines() {
    Reflections reflections = new Reflections(PIPELINE_FILES_PACKAGE, new ResourcesScanner());
    Set<String> resourceList = reflections.getResources(Pattern.compile("pipeline\\.json"));
    return resourceList;
  }

  private String getPipelineName(String pipelineFile) {
    return pipelineFile.replaceAll("^.*"+PIPELINE_FILES_PACKAGE+"(.*)$", "$1");
  }
}
