/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.testBase;

/**
 * Test case for the compile time verifier.
 *
 */

import com.streamsets.pipeline.sdk.annotationsprocessor.PipelineAnnotationsProcessor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
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
  private static final boolean JAVA8 = System.getProperty("java.version").startsWith("1.8");
  private static final String SOURCE_SUPPORTED_WARNING = "warning: Supported source version 'RELEASE_7' from annotation processor 'com.streamsets.pipeline.sdk.annotationsprocessor.PipelineAnnotationsProcessor' less than -source '1.8'";

  private static JavaCompiler COMPILER;
  private StandardJavaFileManager fileManager;
  private DiagnosticCollectorForTests<JavaFileObject> collector;

  @BeforeClass
  public static void initClass() throws Exception {
    COMPILER = ToolProvider.getSystemJavaCompiler();
  }

  @Before
  public void initTest() throws Exception {
    //configure the diagnostics collector.
    collector = new DiagnosticCollectorForTests<>();
    fileManager = COMPILER.getStandardFileManager(collector, Locale.US, Charset.forName("UTF-8"));

    //remove previously generated files
    File f = new File(PipelineAnnotationsProcessor.STAGES_DEFINITION_RESOURCE);
    f.delete();
    f = new File(PipelineAnnotationsProcessor.EL_DEFINITION_RESOURCE);
    f.delete();
  }

  private static class DiagnosticCollectorForTests<T> implements DiagnosticListener<T> {
    private final DiagnosticCollector<T> child;
    public DiagnosticCollectorForTests() {
      child = new DiagnosticCollector<>();
    }
    public void report(Diagnostic<? extends T> diagnostic) {
      String msg = String.valueOf(diagnostic);
      if (!(JAVA8 && SOURCE_SUPPORTED_WARNING.equals(msg))) {
        child.report(diagnostic);
      }
    }
    public List<Diagnostic<? extends T>> getDiagnostics() {
      return child.getDiagnostics();
    }
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
    String outputString = new String(stdoutStream.toByteArray());

    //The real test case
    test(collector.getDiagnostics(), outputString, compilationResult);
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
   * @param result the result of annotation processing on classes provided by {@link #getClassesToProcess()} method
   */
  protected abstract void test(List<Diagnostic<? extends JavaFileObject>> diagnostics
    , String stdoutS, Boolean result);

}