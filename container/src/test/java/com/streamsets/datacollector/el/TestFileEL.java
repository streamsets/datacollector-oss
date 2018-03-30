/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.el;

import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.datacollector.definition.ELDefinitionExtractor;
import com.streamsets.pipeline.lib.el.FileEL;
import org.junit.Assert;
import org.junit.Test;

public class TestFileEL {
  private ELDefinitionExtractor elDefinitionExtractor = ConcreteELDefinitionExtractor.get();

  @Test
  public void testFileName() throws Exception {
    ELEvaluator eval = new ELEvaluator("testFileName", elDefinitionExtractor, FileEL.class);
    ELVariables variables = new ELVariables();
    // Normal file
    Assert.assertEquals("file", eval.eval(variables, "${file:fileName(\"/absolute/path/file\")}", String.class));
    // With extension
    Assert.assertEquals("file.txt", eval.eval(variables, "${file:fileName(\"/absolute/path/file.txt\")}", String.class));
    // Relative path
    Assert.assertEquals("file.txt", eval.eval(variables, "${file:fileName(\"relative/path/file.txt\")}", String.class));
    // Just file name and nothing else
    Assert.assertEquals("file.txt", eval.eval(variables, "${file:fileName(\"file.txt\")}", String.class));
  }

  @Test
  public void testParentPath() throws Exception {
    ELEvaluator eval = new ELEvaluator("testParentPath", elDefinitionExtractor, FileEL.class);
    ELVariables variables = new ELVariables();
    // Normal file
    Assert.assertEquals("/absolute/path", eval.eval(variables, "${file:parentPath(\"/absolute/path/file\")}", String.class));
    // With extension
    Assert.assertEquals("/absolute/path", eval.eval(variables, "${file:parentPath(\"/absolute/path/file.txt\")}", String.class));
    // Relative path
    Assert.assertEquals("relative/path", eval.eval(variables, "${file:parentPath(\"relative/path/file.txt\")}", String.class));
    // Just file name and nothing else
    Assert.assertEquals("", eval.eval(variables, "${file:parentPath(\"file.txt\")}", String.class));
    // Recursive call
    Assert.assertEquals("/absolute", eval.eval(variables, "${file:parentPath(file:parentPath(\"/absolute/path/file\"))}", String.class));
  }

  @Test
  public void testFileExtension() throws Exception {
    ELEvaluator eval = new ELEvaluator("testFileExtension", elDefinitionExtractor, FileEL.class);
    ELVariables variables = new ELVariables();
    // No extension on absolute path
    Assert.assertEquals("", eval.eval(variables, "${file:fileExtension(\"/absolute/path/file\")}", String.class));
    // No extension on relative path
    Assert.assertEquals("", eval.eval(variables, "${file:fileExtension(\"relative/path/file\")}", String.class));
    // Absolute path
    Assert.assertEquals("txt", eval.eval(variables, "${file:fileExtension(\"/absolute/path/file.txt\")}", String.class));
    // Relative path
    Assert.assertEquals("txt", eval.eval(variables, "${file:fileExtension(\"relative/path/file.txt\")}", String.class));
    // Just file name and nothing else
    Assert.assertEquals("txt", eval.eval(variables, "${file:fileExtension(\"file.txt\")}", String.class));
  }

  @Test
  public void testRemoveExtension() throws Exception {
    ELEvaluator eval = new ELEvaluator("testRemoveExtension", elDefinitionExtractor, FileEL.class);
    ELVariables variables = new ELVariables();
    // No extension on absolute path
    Assert.assertEquals("/absolute/path/file", eval.eval(variables, "${file:removeExtension(\"/absolute/path/file\")}", String.class));
    // No extension on relative path
    Assert.assertEquals("relative/path/file", eval.eval(variables, "${file:removeExtension(\"relative/path/file\")}", String.class));
    // Absolute path
    Assert.assertEquals("/absolute/path/file", eval.eval(variables, "${file:removeExtension(\"/absolute/path/file.txt\")}", String.class));
    // Relative path
    Assert.assertEquals("relative/path/file", eval.eval(variables, "${file:removeExtension(\"relative/path/file.txt\")}", String.class));
    // Just file name and nothing else
    Assert.assertEquals("file", eval.eval(variables, "${file:removeExtension(\"file.txt\")}", String.class));
  }

  @Test
  public void testPathElement() throws Exception {
    ELEvaluator eval = new ELEvaluator("testPathElement", elDefinitionExtractor, FileEL.class);
    ELVariables variables = new ELVariables();

    // Absolute path
    Assert.assertEquals("",         eval.eval(variables, "${file:pathElement(\"/absolute/path/file\", -500)}", String.class));
    Assert.assertEquals("",         eval.eval(variables, "${file:pathElement(\"/absolute/path/file\", -4)}", String.class));
    Assert.assertEquals("absolute", eval.eval(variables, "${file:pathElement(\"/absolute/path/file\", -3)}", String.class));
    Assert.assertEquals("path",     eval.eval(variables, "${file:pathElement(\"/absolute/path/file\", -2)}", String.class));
    Assert.assertEquals("file",     eval.eval(variables, "${file:pathElement(\"/absolute/path/file\", -1)}", String.class));

    Assert.assertEquals("absolute", eval.eval(variables, "${file:pathElement(\"/absolute/path/file\", 0)}", String.class));
    Assert.assertEquals("path",     eval.eval(variables, "${file:pathElement(\"/absolute/path/file\", 1)}", String.class));
    Assert.assertEquals("file",     eval.eval(variables, "${file:pathElement(\"/absolute/path/file\", 2)}", String.class));
    Assert.assertEquals("",         eval.eval(variables, "${file:pathElement(\"/absolute/path/file\", 3)}", String.class));
    Assert.assertEquals("",         eval.eval(variables, "${file:pathElement(\"/absolute/path/file\", 500)}", String.class));

    // Relative path
    Assert.assertEquals("",         eval.eval(variables, "${file:pathElement(\"relative/path/file\", -500)}", String.class));
    Assert.assertEquals("",         eval.eval(variables, "${file:pathElement(\"relative/path/file\", -4)}", String.class));
    Assert.assertEquals("relative", eval.eval(variables, "${file:pathElement(\"relative/path/file\", -3)}", String.class));
    Assert.assertEquals("path",     eval.eval(variables, "${file:pathElement(\"relative/path/file\", -2)}", String.class));
    Assert.assertEquals("file",     eval.eval(variables, "${file:pathElement(\"relative/path/file\", -1)}", String.class));

    Assert.assertEquals("relative", eval.eval(variables, "${file:pathElement(\"relative/path/file\", 0)}", String.class));
    Assert.assertEquals("path",     eval.eval(variables, "${file:pathElement(\"relative/path/file\", 1)}", String.class));
    Assert.assertEquals("file",     eval.eval(variables, "${file:pathElement(\"relative/path/file\", 2)}", String.class));
    Assert.assertEquals("",         eval.eval(variables, "${file:pathElement(\"relative/path/file\", 3)}", String.class));
    Assert.assertEquals("",         eval.eval(variables, "${file:pathElement(\"relative/path/file\", 500)}", String.class));
  }
}
