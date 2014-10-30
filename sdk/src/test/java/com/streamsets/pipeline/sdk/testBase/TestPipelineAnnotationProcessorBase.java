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
package com.streamsets.pipeline.sdk.testBase;

/**
 * Test case for the compile time verifier.
 *
 */

import com.streamsets.pipeline.sdk.annotationsprocessor.Constants;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.tools.*;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Base class for all the annotation process test cases.
 *
 */
public abstract class TestPipelineAnnotationProcessorBase {

    private static JavaCompiler COMPILER;
    private StandardJavaFileManager fileManager;
    private DiagnosticCollector<JavaFileObject> collector;

  @BeforeClass
  public static void initClass() throws Exception {
    COMPILER = ToolProvider.getSystemJavaCompiler();
  }

  @Before
  public void initTest() throws Exception {
    //configure the diagnostics collector.
    collector = new DiagnosticCollector<JavaFileObject>();
    fileManager = COMPILER.getStandardFileManager(collector, Locale.US, Charset.forName("UTF-8"));

    //remove previously generated PipelineStages.json
    File f = new File(Constants.PIPELINE_STAGES_JSON);
    f.delete();
  }

  @Test
  /**
   * processes the annotations present on classes provided by {@link #getClassesToProcess()}
   * method.
   *
   * Calls the {@link #test(java.util.List, String, Boolean)} method with the data collected by
   * the compiler
   */
  public void testPipelineAnnotationProcessor() throws Exception {

    //get the list of classes to process fot this test case
    List<String> classesToProcess = getClassesToProcess();

    ByteArrayOutputStream stdoutStream = new ByteArrayOutputStream();
    OutputStreamWriter stdout = new OutputStreamWriter(stdoutStream);

    JavaCompiler.CompilationTask task = COMPILER.getTask(stdout, fileManager,
      collector, Arrays.asList("-proc:only" /*compiler option to just process annotation*/),
      classesToProcess /*class files that need to be processed*/,
      null /*We don't need to compile source files*/);

    //Result of the compilation
    Boolean compilationResult = task.call();
    //Output from compiler
    String ouptputString = new String(stdoutStream.toByteArray());

    //The real test case
    test(collector.getDiagnostics(), ouptputString, compilationResult);
  }

  /**
   * @return the classes which must be "annotation processed" for the
   * overriding test case.
   */
  protected abstract List<String> getClassesToProcess();

  /**
   * Tests the expected and actual findings for the test case
   *
   * @param diagnostics the diagnostics coolected by the compiler after compiling the classes supplied by the
   *                    {@link #getClassesToProcess()} method
   * @param stdoutS additional output by the compiler
   * @param result the result of annotatio processing on classes provided by {@link #getClassesToProcess()} method
   */
  protected abstract void test(List<Diagnostic<? extends JavaFileObject>> diagnostics
    , String stdoutS, Boolean result);

}