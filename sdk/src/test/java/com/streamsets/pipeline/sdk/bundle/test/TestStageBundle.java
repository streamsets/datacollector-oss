/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
    //A bundle file "TwitterSource-bundle.properties" is generated which contains 2 lines
    /*
    stage.label=twitter_source
    stage.description=Produces twitter feeds
    config.username.label=username
    config.username.description=The user name of the twitter user
    config.password.label=password
    config.password.description=The password the twitter use
     */
    List<String> expectedStrings = new ArrayList<String>(6);
    expectedStrings.add("stage.label=twitter_source");
    expectedStrings.add("stage.description=Produces twitter feeds");
    expectedStrings.add("config.username.label=username");
    expectedStrings.add("config.username.description=The user name of the twitter user");
    expectedStrings.add("config.password.label=password");
    expectedStrings.add("config.password.description=The password the twitter user");


    InputStream inputStream = Thread.currentThread().getContextClassLoader().
      getResourceAsStream("TwitterSource-bundle.properties");
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
