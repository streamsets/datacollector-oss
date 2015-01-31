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
@Target(ElementType.TYPE)
public @interface StageDef {

  public interface Label {
    public String getLabel();
  }

  public enum VariableOutputStreams implements Label {
    ;

    @Override
    public String getLabel() {
      return null;
    }
  }

    public enum DefaultOutputStream implements Label {
    OUTPUT("Output");

    private final String label;

    DefaultOutputStream(String label) {
      this.label = label;
    }

    @Override
    public String getLabel() {
      return label;
    }
  }
  String version();

  String label();

  String description() default "";

  String icon() default "";

  String outputStreamsDrivenByConfig() default ""; //selector  case

  Class<? extends Label> outputStreams() default DefaultOutputStream.class;

}
