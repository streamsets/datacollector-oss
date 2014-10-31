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

import com.streamsets.pipeline.sdk.annotationsprocessor.Constants;
import com.streamsets.pipeline.sdk.testBase.TestPipelineAnnotationProcessorBase;
import org.junit.Assert;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.File;
import java.util.*;

public class TestFaultyStage extends TestPipelineAnnotationProcessorBase {

  @Override
  public List<String> getClassesToProcess() {
    return Arrays.asList("com.streamsets.pipeline.sdk.testData.FaultySource");
  }

  @Override
  public void test(List<Diagnostic<? extends JavaFileObject>> diagnostics, String compilerOutput, Boolean compilationResult) {

    //Compilation is expected to be successful
    Assert.assertFalse(compilationResult);

    //No compiler output is expected
    Assert.assertTrue(compilerOutput.isEmpty());

    //The following error messages are expected
    Set<String> expectedSet = new HashSet<String>();
    expectedSet.add("The field FaultySourceusername has \"ConfigDef\" annotation and is declared final. Configuration fields must not be declared final.");
    expectedSet.add("The field FaultySourcepassword has \"ConfigDef\" annotation and is declared static. Configuration fields must not be declared final.");
    expectedSet.add("The field FaultySourcestreetAddress2 has \"ConfigDef\" annotation but is not declared public. Configuration fields must be declared public.");
    expectedSet.add("The type of field FaultySourcecompany is declared as \"MODEL\". Exactly one of 'FieldSelector' or 'FieldModifier' annotation is expected.");
    expectedSet.add("The type of the field FaultySourcezip is expected to be String.");
    expectedSet.add("The type of the field FaultySourcestate is expected to be List<String>.");
    expectedSet.add("The type of the field FaultySourcestreetAddress is expected to be Map<String, String>.");
    expectedSet.add("The field FaultySourceste is annotated with both 'FieldSelector' and 'FieldModifier' annotations. Only one of those annotation is expected.");
    expectedSet.add("Stage com.streamsets.pipeline.sdk.testData.FaultySource neither extends one of BaseSource, BaseProcessor, BaseTarget classes nor implements one of Source, Processor, Target interface.");
    expectedSet.add("The Stage FaultySource has constructor with arguments but no default constructor.");

    Assert.assertTrue(diagnostics.size() == 10);
    for(Diagnostic d : diagnostics) {
      Assert.assertTrue(expectedSet.contains(d.getMessage(Locale.ENGLISH)));
    }

    //test that no PipelineStages file is generated
    File f = new File(Constants.PIPELINE_STAGES_JSON);
    Assert.assertFalse(f.exists());
  }
}