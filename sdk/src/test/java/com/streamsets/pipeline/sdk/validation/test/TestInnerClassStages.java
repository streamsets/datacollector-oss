/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.validation.test;

import com.streamsets.pipeline.sdk.testBase.TestPipelineAnnotationProcessorBase;
import org.junit.Assert;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class TestInnerClassStages extends TestPipelineAnnotationProcessorBase {

  @Override
  public List<String> getClassesToProcess() {
    return Arrays.asList("com.streamsets.pipeline.sdk.testData.TwitterStages");
  }

  @Override
  public void test(List<Diagnostic<? extends JavaFileObject>> diagnostics, String compilerOutput, Boolean compilationResult) {

    //Compilation is expected to be successful
    Assert.assertFalse(compilationResult);
    //No compiler output is expected
    Assert.assertTrue(compilerOutput.isEmpty());
    //No diagnostics
    Assert.assertTrue(diagnostics.size() == 3);

    List<String> expectedErrors = new ArrayList<String>(3);
    expectedErrors.add("Stage TwitterTarget is an inner class. Inner class Stage implementations are not supported");
    expectedErrors.add("Stage TwitterProcessor is an inner class. Inner class Stage implementations are not supported");
    expectedErrors.add("Stage TwitterSource is an inner class. Inner class Stage implementations are not supported");

    for(Diagnostic d : diagnostics) {
      System.out.println(d.getMessage(Locale.ENGLISH));
      Assert.assertTrue(expectedErrors.contains(d.getMessage(Locale.ENGLISH)));
    }
  }
}