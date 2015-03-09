/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.json.test;

import com.streamsets.pipeline.sdk.annotationsprocessor.testBase.TestPipelineAnnotationProcessorBase;
import org.junit.Assert;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.util.Arrays;
import java.util.List;

public class TestStageWithStageAndConfigDef extends TestPipelineAnnotationProcessorBase {

  @Override
  public List<String> getClassesToProcess() {
    return Arrays.asList(
      "com.streamsets.pipeline.sdk.annotationsprocessor.testData.SourceWithStageAndConfigDef"
    );
  }

  @Override
  public void test(List<Diagnostic<? extends JavaFileObject>> diagnostics, String compilerOutput, Boolean compilationResult) {

    //Compilation is expected to be successful
    Assert.assertTrue(compilationResult);
    //No compiler output is expected
    Assert.assertTrue(compilerOutput.isEmpty());
    //No diagnostics
    Assert.assertTrue(String.valueOf(diagnostics), diagnostics.isEmpty());
    //PipelineStages.json is expected to be generated
    TestUtil.compareExpectedAndActualStages("SourceWithStageAndConfigDef.json");

  }
}
