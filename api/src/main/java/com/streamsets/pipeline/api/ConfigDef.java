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

  public enum Type { BOOLEAN, NUMBER, STRING, LIST, MAP, MODEL, EL_STRING, EL_NUMBER, EL_DATE, EL_BOOLEAN, EL_OBJECT,
    CHARACTER }

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

}
