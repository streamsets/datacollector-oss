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
package com.streamsets.datacollector.util;

import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.credential.ClearCredentialValue;
import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.lib.el.StringEL;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class ElUtil {

  private static final String EL_PREFIX = "${";
  private static final String CONSTANTS = "constants";

  private ElUtil() {}

  public static Object evaluate(
      String errorDescription,
      Object value,
      ConfigDefinition configDefinition,
      Map<String, Object> constants
  ) throws ELEvalException {
    if(configDefinition.getEvaluation() == ConfigDef.Evaluation.IMPLICIT) {
      if(isElString(value)) {
        //its an EL expression, try to evaluate it.
        ELEvaluator elEvaluator = createElEval(configDefinition.getName(), constants, getElDefs(configDefinition));
        Type genericType = configDefinition.getConfigField().getGenericType();
        Class<?> klass;
        if(genericType instanceof ParameterizedType) {
          //As of today the only supported parameterized types are
          //1. List<String>
          //2. Map<String, String>
          //3. List<Map<String, String>>
          //In all cases we want to return String.class
          klass = String.class;
        } else if (genericType instanceof Enum) {
          klass = String.class;
        } else {
          klass = (Class<?>) genericType;
        }
        if (configDefinition.getType() == ConfigDef.Type.CREDENTIAL) {
          value = elEvaluator.eval(new ELVariables(constants), (String) value, Object.class);
          if (value == null || value instanceof String) {
            value = new ClearCredentialValue((String)value);
          } else if (!(value instanceof CredentialValue)) {
            throw new ELEvalException(
                ContainerError.CONTAINER_01500,
                errorDescription,
                configDefinition.getName(),
                value.getClass()
            );
          }
        } else {
          value = elEvaluator.eval(new ELVariables(constants), (String) value, klass);
        }
      }
    }
    return value;
  }

  public static boolean isElString(Object value) {
    if(value instanceof String && ((String) value).contains(EL_PREFIX)) {
      return true;
    }
    return false;
  }

  public static Class<?>[] getElDefs(ConfigDefinition configDefinition) {
    List<Class> elDefs = configDefinition.getElDefs();
    if(elDefs != null && elDefs.size() > 0) {
      return elDefs.toArray(new Class[elDefs.size()]);
    }
    Class<?>[] elDefClasses = new Class[2];
    //inject RuntimeEL.class & StringEL.class into the evaluator
    elDefClasses[0] = RuntimeEL.class;
    elDefClasses[1] = StringEL.class;
    return elDefClasses;
  }

  public static Class<?>[] getElDefClassArray(List<Class> elDefs) {
    Class[] elDefClasses = new Class[elDefs.size() + 2];
    int i = 0;
    for(; i < elDefs.size(); i++) {
      elDefClasses[i] = elDefs.get(i);
    }
    elDefClasses[i++] = RuntimeEL.class;
    elDefClasses[i] = StringEL.class;
    return elDefClasses;
  }

  public static ELEvaluator createElEval(String name, Map<String, Object> constants, Class<?>... elDefs) {
    return new ELEvaluator(name, false, constants, ConcreteELDefinitionExtractor.get(), elDefs);
  }

  public static Map<String, Object> getConstants(List<Config> pipelineConf) {
    return PipelineConfigurationUtil.getFlattenedMap(CONSTANTS, pipelineConf);
  }

}
