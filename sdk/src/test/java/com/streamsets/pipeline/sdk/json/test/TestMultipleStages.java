/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.json.test;

import com.streamsets.pipeline.sdk.testBase.TestPipelineAnnotationProcessorBase;
import org.junit.Assert;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.util.Arrays;
import java.util.List;

public class TestMultipleStages extends TestPipelineAnnotationProcessorBase {

  @Override
  public List<String> getClassesToProcess() {
    return Arrays.asList(
      "com.streamsets.pipeline.sdk.testData.TwitterSource",
      "com.streamsets.pipeline.sdk.testData.TwitterProcessor",
      "com.streamsets.pipeline.sdk.testData.TwitterTarget",
      "com.streamsets.pipeline.sdk.testData.TwitterError"
    );
  }

  @Override
  public void test(List<Diagnostic<? extends JavaFileObject>> diagnostics, String compilerOutput, Boolean compilationResult) {

    //Compilation is expected to be successful
    Assert.assertTrue(compilationResult);
    //No compiler output is expected
    Assert.assertTrue(compilerOutput.isEmpty());
    //No diagnostics
    Assert.assertTrue(diagnostics.isEmpty());
    //PipelineStages.json is expected to be generated and must match
    //the contents of MultipleStages.json
    TestUtil.compareExpectedAndActualStages("MultipleStages.json");

  }
}
