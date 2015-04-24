/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.pipeline.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ConfigDef {

  public enum Type { BOOLEAN, NUMBER, STRING, LIST, MAP, MODEL, CHARACTER, TEXT, PASSWORD }

  public enum Mode {JAVA, JAVASCRIPT, JSON, PLAIN_TEXT, PYTHON, RUBY, SCALA}

  public enum Evaluation {IMPLICIT, EXPLICIT}

  Type type();

  String defaultValue() default "";

  boolean required();

  String label();

  String description() default "";

  String group() default "";

  String dependsOn() default "";

  int displayPosition() default 0;

  String[] triggeredByValue() default {};

  long min() default Long.MIN_VALUE;

  long max() default Long.MAX_VALUE;

  //O displays 1 line in UI, text box cannot be re-sized and user can enter just one line of input
  //1 displays 1 line in UI, text box can be re-sized and user can enter n lines of input
  //n indicates n lines in UI, text box can be re-sized and user can enter just one line of input
  int lines() default 0;

  Mode mode() default Mode.PLAIN_TEXT;

  Class[] elDefs() default {};

  Evaluation evaluation() default Evaluation.IMPLICIT;

}
