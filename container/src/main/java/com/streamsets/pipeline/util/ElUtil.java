/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELVariables;
import com.streamsets.pipeline.el.RuntimeEL;
import com.streamsets.pipeline.lib.el.StringEL;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class ElUtil {

  private static final String EL_PREFIX = "${";
  private static final String CONSTANTS = "constants";

  public static Object evaluate(Object value, Field field, StageDefinition stageDefinition,
                                 ConfigDefinition configDefinition,
                                 Map<String, Object> constants) throws ELEvalException {
    if(configDefinition.getEvaluation() == ConfigDef.Evaluation.IMPLICIT) {
      if(isElString(value)) {
        //its an EL expression, try to evaluate it.
        ELEvaluator elEvaluator = createElEval(field.getName(), constants, getElDefs(stageDefinition,
          configDefinition));
        Type genericType = field.getGenericType();
        Class<?> klass;
        if(genericType instanceof ParameterizedType) {
          //As of today the only supported parameterized types are
          //1. List<String>
          //2. Map<String, String>
          //3. List<Map<String, String>>
          //In all cases we want to return String.class
          klass = String.class;
        } else {
          klass = (Class<?>) genericType;
        }
        return elEvaluator.evaluate(new ELVariables(constants), (String)value, klass);
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

  public static Class<?>[] getElDefs(StageDefinition stageDef, ConfigDefinition configDefinition) {
    ClassLoader cl = stageDef.getStageClassLoader();
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
    return new ELEvaluator(name, constants, elDefs);
  }

  public static Map<String, Object> getConstants(PipelineConfiguration pipelineConf) {
    return PipelineConfigurationUtil.getFlattenedMap(CONSTANTS, pipelineConf);
  }

}
