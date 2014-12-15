/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.validation.test;

import com.streamsets.pipeline.sdk.annotationsprocessor.Constants;
import com.streamsets.pipeline.sdk.testBase.TestPipelineAnnotationProcessorBase;
import org.junit.Assert;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class TestFaultyStage extends TestPipelineAnnotationProcessorBase {

  @Override
  public List<String> getClassesToProcess() {
    //FaultySource stage is modeled with all kinds of validation errors
    return Arrays.asList("com.streamsets.pipeline.sdk.testData.FaultySource");
  }

  @Override
  public void test(List<Diagnostic<? extends JavaFileObject>> diagnostics, String compilerOutput, Boolean compilationResult) {

    //Compilation is expected to be successful
    Assert.assertFalse(compilationResult);

    //No compiler output is expected
    Assert.assertTrue(compilerOutput.isEmpty());

    //The following error messages are expected
    Set<String> expectedSet = new HashSet<>();
    expectedSet.add("The field FaultySource.username has \"ConfigDef\" annotation and is declared final. Configuration fields must not be declared final.");
    expectedSet.add("The field FaultySource.password has \"ConfigDef\" annotation and is declared static. Configuration fields must not be declared final.");
    expectedSet.add("The field FaultySource.streetAddress2 has \"ConfigDef\" annotation but is not declared public. Configuration fields must be declared public.");
    expectedSet.add("The type of field FaultySource.company is declared as \"MODEL\". Exactly one of '[FieldValueChooser, FieldSelector, ValueChooser, LanePredicateMapping]' annotations is expected.");
    expectedSet.add("The type of the field FaultySource.zip is expected to be String.");
    expectedSet.add("The type of the field FaultySource.state is expected to be List<String>.");
    expectedSet.add("The type of the field FaultySource.streetAddress is expected to be Map<String, String> or Map<String, Enum>.");
    expectedSet.add("ChooserValues implementation 'com.streamsets.pipeline.sdk.testData.FaultySource$MyChooserValues' is an inner class but is not declared as static. Inner class ChooserValues implementations must be declared static.");
    expectedSet.add("The type of field FaultySource.ste is declared as \"MODEL\". Exactly one of '[FieldValueChooser, FieldSelector, ValueChooser, LanePredicateMapping]' annotations is expected.");
    expectedSet.add("The type of the field FaultySource.ste is expected to be Map<String, String> or Map<String, Enum>.");
    expectedSet.add("The type of the field FaultySource.floor is expected to be String.");
    expectedSet.add("The type of the field FaultySource.extension is Integer but the default value supplied is not Integer.");
    expectedSet.add("The type of the field FaultySource.phone is Long but the default value supplied is not Long.");
    expectedSet.add("The type of the field FaultySource.callMe is Boolean but the default value supplied is not true or false.");
    expectedSet.add("The type of field FaultySource.floor is not declared as \"MODEL\". 'FieldSelector' or 'FieldValueChooser' or 'ValueChooser' annotation is not expected, but is present.");
    expectedSet.add("Stage com.streamsets.pipeline.sdk.testData.FaultySource neither extends one of BaseSource, BaseProcessor, BaseTarget classes nor implements one of Source, Processor, Target interface.");
    expectedSet.add("The Stage FaultySource has constructor with arguments but no default constructor.");
    expectedSet.add("Annotation RawSource is applied on stage com.streamsets.pipeline.sdk.testData.FaultySource which is not a \"Source\".");
    expectedSet.add("RawSourcePreviewer com.streamsets.pipeline.sdk.testData.TestRawSourcePreviewer.FaultyRawSourcePreviewer is an inner class. Inner class RawSourcePreviewer implementations are not supported.");


    for(Diagnostic d : diagnostics) {
      System.out.println(d.toString());
    }
    Assert.assertEquals(expectedSet.size(), diagnostics.size());
    for(Diagnostic d : diagnostics) {
      Assert.assertTrue(expectedSet.contains(d.getMessage(Locale.ENGLISH)));
    }

    //test that no PipelineStages file is generated
    File f = new File(Constants.PIPELINE_STAGES_JSON);
    Assert.assertFalse(f.exists());
  }
}
