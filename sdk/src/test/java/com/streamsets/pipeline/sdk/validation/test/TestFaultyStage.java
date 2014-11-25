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
  /**
   * This class has the following issues:
   * 1. Configuration field is final
   * 2. Configuration field is static
   * 3. Configuration field is not public
   * 4. Configuration field is marked as "MODEL" but not annotated with either FieldSelector or FieldModifier annotation
   * 5. No default constructor
   * 6. Does not implement interface or extend base stage
   * 7. Data type of field does not match type indicated in the config def annotation
   * 8. Configuration field marked with FieldSelector annotation must be of type List<String>
   * 9. Configuration field marked with FieldModifier annotation must be of type Map<String, String>
   * 10. Both FieldSelector and FieldModifier annotations are present
   */
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
    expectedSet.add("The field FaultySource.username has \"ConfigDef\" annotation and is declared final. Configuration fields must not be declared final.");
    expectedSet.add("The field FaultySource.password has \"ConfigDef\" annotation and is declared static. Configuration fields must not be declared final.");
    expectedSet.add("The field FaultySource.streetAddress2 has \"ConfigDef\" annotation but is not declared public. Configuration fields must be declared public.");
    expectedSet.add("The type of field FaultySource.company is declared as \"MODEL\". Exactly one of 'FieldSelector' or 'FieldModifier' or 'DropDown' annotation is expected.");
    expectedSet.add("The type of the field FaultySource.zip is expected to be String.");
    expectedSet.add("The type of the field FaultySource.state is expected to be List<String>.");
    expectedSet.add("The type of the field FaultySource.streetAddress is expected to be Map<String, String>.");
    expectedSet.add("The field FaultySource.ste is annotated with both 'FieldSelector' and 'FieldModifier' annotations. Only one of those annotation is expected.");
    expectedSet.add("Stage com.streamsets.pipeline.sdk.testData.FaultySource neither extends one of BaseSource, BaseProcessor, BaseTarget classes nor implements one of Source, Processor, Target interface.");
    expectedSet.add("The Stage FaultySource has constructor with arguments but no default constructor.");

    for(Diagnostic d : diagnostics) {
      System.out.println(d.toString());
    }
    Assert.assertEquals(10, diagnostics.size());
    for(Diagnostic d : diagnostics) {
      Assert.assertTrue(expectedSet.contains(d.getMessage(Locale.ENGLISH)));
    }

    //test that no PipelineStages file is generated
    File f = new File(Constants.PIPELINE_STAGES_JSON);
    Assert.assertFalse(f.exists());
  }
}