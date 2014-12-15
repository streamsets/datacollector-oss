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

/**
 * Tests that the expected bundle file for a stage is generated and that
 * it contains the required labels and values
 *
 */
public class TestStageBundle extends TestPipelineAnnotationProcessorBase {

  @Override
  public List<String> getClassesToProcess() {
    return Arrays.asList("com.streamsets.pipeline.sdk.testData.TwitterSource");
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
    //A bundle file "TwitterSource.properties" is generated which contains 2 lines
    /*
    label=twitter_source
    description=Produces twitter feeds
    username.label=username
    username.description=The user name of the twitter user
    password.label=password
    password.description=The password the twitter use
     */
    List<String> expectedStrings = new ArrayList<String>(6);
    expectedStrings.add("label=twitter_source");
    expectedStrings.add("description=Produces twitter feeds");
    expectedStrings.add("username.label=username");
    expectedStrings.add("username.description=The user name of the twitter user");
    expectedStrings.add("password.label=password");
    expectedStrings.add("password.description=The password the twitter user");


    InputStream inputStream = Thread.currentThread().getContextClassLoader().
      getResourceAsStream("TwitterSource.properties");
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
    for(int i = 0; i < expectedStrings.size(); i++) {
      Assert.assertTrue(expectedStrings.get(i).equals(actualStrings.get(i)));
    }

  }

}
