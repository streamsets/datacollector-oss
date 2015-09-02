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
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ConfigDef {

  public enum Type {
    BOOLEAN(false),
    NUMBER(0),
    STRING(""),
    LIST(Collections.emptyList()),
    MAP(Collections.emptyList()),
    MODEL(""),
    CHARACTER(' '),
    TEXT("")

    ;

    private final Object defaultValue;

    Type(Object defaultValue) {
      this.defaultValue = defaultValue;
    }

    public Object getDefault(Class variableClass) {
      Object value;
      if (variableClass.isEnum()) {
        value = variableClass.getEnumConstants()[0];
      } else if (Map.class.isAssignableFrom(variableClass)) {
        value = Collections.emptyMap();
      } else if (List.class.isAssignableFrom(variableClass)) {
        value = Collections.emptyList();
      } else {
        value = defaultValue;
      }
      return value;
    }
  }

  public enum Mode {JAVA, JAVASCRIPT, JSON, PLAIN_TEXT, PYTHON, RUBY, SCALA, SQL}

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
