/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.el.StringEL;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

class ElUtil {

  public static Map<String, Class<?>[]> getConfigToElDefMap(Class<?> stageClass) throws Exception {
    Map<String, Class<?>[]> configToElDefMap = new HashMap<>();
    for (Field field : stageClass.getFields()) {
      if (field.isAnnotationPresent(ConfigDef.class)) {
        ConfigDef configDef = field.getAnnotation(ConfigDef.class);
        configToElDefMap.put(field.getName(), getElDefClasses(configDef.elDefs()));
        if(field.getAnnotation(ComplexField.class) != null) {
          Type genericType = field.getGenericType();
          Class<?> klass;
          if (genericType instanceof ParameterizedType) {
            Type[] typeArguments = ((ParameterizedType) genericType).getActualTypeArguments();
            klass = (Class<?>) typeArguments[0];
          } else {
            klass = (Class<?>) genericType;
          }
          for (Field f : klass.getFields()) {
            if (f.isAnnotationPresent(ConfigDef.class)) {
              ConfigDef configDefinition = f.getAnnotation(ConfigDef.class);
              configToElDefMap.put(f.getName(), getElDefClasses(configDefinition.elDefs()));
            }
          }
        }
      }
    }
    return configToElDefMap;
  }


  public static Class<?>[] getElDefClasses(Class[] elDefs) {
    Class<?>[] elDefClasses = new Class<?>[elDefs.length + 1];
    int i = 0;

    for(; i < elDefs.length; i++) {
      elDefClasses[i] = elDefs[i];
    }
    //inject RuntimeEL.class & StringEL.class into the evaluator
    //Since injecting RuntimeEL.class requires RuntimeInfo class in the classpath, not adding it for now.
    //elDefClasses[i++] = RuntimeEL.class;
    elDefClasses[i] = StringEL.class;
    return elDefClasses;
  }
}
