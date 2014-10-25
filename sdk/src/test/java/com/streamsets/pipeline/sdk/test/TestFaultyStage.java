package com.streamsets.pipeline.sdk.test;

import com.streamsets.pipeline.sdk.Constants;
import org.junit.Assert;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Created by harikiran on 10/24/14.
 */
public class TestFaultyStage extends TestConfigProcessorBase {

  @Override
  public List<String> getClassesToProcess() {
    return Arrays.asList("com.streamsets.pipeline.sdk.test.FaultySource");
  }

  @Override
  public void test(List<Diagnostic<? extends JavaFileObject>> diagnostics, String compilerOutput, Boolean compilationResult) {

    //Compilation is expected to be successful
    Assert.assertFalse(compilationResult);

    //No compiler output is expected
    Assert.assertTrue(compilerOutput.isEmpty());

    //One compiler error
    Assert.assertTrue(diagnostics.size() == 1);

    //test that no PipelineStages file is generated
    File f = new File(Constants.PIPELINE_STAGES_JSON);
    Assert.assertFalse(f.exists());
  }
}