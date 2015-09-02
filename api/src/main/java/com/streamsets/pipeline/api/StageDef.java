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

package com.streamsets.pipeline.api;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface StageDef {

  //enum for processors using LanePredicateMapping configurations

  // we are using the annotation for reference purposes only.
  // the annotation processor does not work on this maven project
  // we have a hardcoded 'datacollector-resource-bundles.json' file in resources
  @GenerateResourceBundle
  public enum VariableOutputStreams implements Label {
    ;

    @Override
    public String getLabel() {
      return null;
    }
  }

  //default enum for processors that don;'t specify 'outputStreams'

  // we are using the annotation for reference purposes only.
  // the annotation processor does not work on this maven project
  // we have a hardcoded 'datacollector-resource-bundles.json' file in resources
  @GenerateResourceBundle
  public enum DefaultOutputStreams implements Label {
    OUTPUT("Output");

    private final String label;

    DefaultOutputStreams(String label) {
      this.label = label;
    }

    @Override
    public String getLabel() {
      return label;
    }
  }

  int version();

  String label();

  String description() default "";

  String icon() default "";

  String outputStreamsDrivenByConfig() default ""; //selector  case

  Class<? extends Label> outputStreams() default DefaultOutputStreams.class;

  ExecutionMode[] execution() default { ExecutionMode.STANDALONE, ExecutionMode.CLUSTER };

  boolean recordsByRef() default false;

  boolean privateClassLoader() default false;

  Class<? extends StageUpgrader> upgrader() default StageUpgrader.Default.class;

}
