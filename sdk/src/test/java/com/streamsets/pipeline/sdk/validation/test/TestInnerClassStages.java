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