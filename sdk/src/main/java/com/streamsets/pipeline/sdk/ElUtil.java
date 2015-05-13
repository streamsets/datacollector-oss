/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;

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
        if(configDef.elDefs().length > 0) {
          configToElDefMap.put(field.getName(), configDef.elDefs());
        }
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
              if (configDefinition.elDefs().length > 0) {
                configToElDefMap.put(f.getName(), configDefinition.elDefs());
              }
            }
          }
        }
      }
    }
    return configToElDefMap;
  }
}
