package com.streamsets.pipeline.sdk.test;

import org.junit.Assert;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.util.Arrays;
import java.util.List;

/**
 * Created by harikiran on 10/27/14.
 */
public class TestTwitterError extends TestConfigProcessorBase {
  @Override
  public List<String> getClassesToProcess() {
    return Arrays.asList("com.streamsets.pipeline.sdk.test.TwitterError");
  }

  @Override
  public void test(List<Diagnostic<? extends JavaFileObject>> diagnostics, String compilerOutput, Boolean compilationResult) {

    //Compilation is expected to be successful
    Assert.assertTrue(compilationResult);

    //No compiler output is expected
    Assert.assertTrue(compilerOutput.isEmpty());

    //No diagnostics
    Assert.assertTrue(diagnostics.isEmpty());

    //PipelineStages.json is expected to be generated
    //TestUtil.compareExpectedAndActualStages("MultipleStages.json");

  }
}
