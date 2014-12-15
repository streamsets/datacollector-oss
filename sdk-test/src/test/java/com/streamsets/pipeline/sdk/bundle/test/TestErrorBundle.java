/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.bundle.test;

import com.streamsets.pipeline.sdk.testBase.TestPipelineAnnotationProcessorBase;
import org.junit.Assert;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestErrorBundle extends TestPipelineAnnotationProcessorBase {
  @Override
  public List<String> getClassesToProcess() {
    return Arrays.asList("com.streamsets.pipeline.sdk.testData.TwitterError");
  }

  @Override
  public void test(List<Diagnostic<? extends JavaFileObject>> diagnostics,
                   String compilerOutput, Boolean compilationResult) {

    //Compilation is expected to be successful
    Assert.assertTrue(compilationResult);
    //No compiler output is expected
    Assert.assertTrue(compilerOutput.isEmpty());
    //No diagnostics
    Assert.assertTrue(diagnostics.isEmpty());
    //A bundle file "TwitterError.properties" is generated which contains 2 lines
    //INPUT_LANE_ERROR=null
    //OUTPUT_LANE_ERROR=null
    List<String> expectedStrings = new ArrayList<String>(2);
    expectedStrings.add("INPUT_LANE_ERROR=null");
    expectedStrings.add("OUTPUT_LANE_ERROR=null");

    InputStream inputStream = Thread.currentThread().getContextClassLoader().
        getResourceAsStream("TwitterError.properties");
    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
    List<String> actualStrings = new ArrayList<String>();
    String line;
    try {
      while ((line = br.readLine()) != null) {
        actualStrings.add(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    //compare expected and actual Strings
    Assert.assertTrue("The expected and actual lines in the files are different", expectedStrings.size() == actualStrings.size());
    Assert.assertTrue(expectedStrings.get(0).equals(actualStrings.get(0)));
    Assert.assertTrue(expectedStrings.get(1).equals(actualStrings.get(1)));
  }
}
