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
package com.streamsets.pipeline.sdk;

import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.FileEL;
import com.streamsets.pipeline.lib.el.MathEL;
import com.streamsets.pipeline.lib.el.StringEL;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class ElUtil {

  private ElUtil() {}

  public static Map<String, Class<?>[]> getConfigToElDefMap(Class<?> stageClass) {
    Map<String, Class<?>[]> configToElDefMap = new HashMap<>();
    for (Field field : stageClass.getFields()) {
      if (field.isAnnotationPresent(ConfigDef.class)) {
        ConfigDef configDef = field.getAnnotation(ConfigDef.class);
        configToElDefMap.put(field.getName(), getElDefClasses(configDef.elDefs()));
        if(field.getAnnotation(ListBeanModel.class) != null) {
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
      } else if (field.isAnnotationPresent(ConfigDefBean.class)) {
        configToElDefMap.putAll(getConfigToElDefMap(field.getType()));
      }
    }
    return configToElDefMap;
  }


  public static Class<?>[] getElDefClasses(Class[] elDefs) {
    Class<?>[] elDefClasses = new Class<?>[elDefs.length + 3];
    int i = 0;

    for(; i < elDefs.length; i++) {
      elDefClasses[i] = elDefs[i];
    }
    //inject few standard ELs, so that they are available everywhere
    //Since injecting RuntimeEL.class requires RuntimeInfo class in the classpath, not adding it for now.
    //elDefClasses[i++] = RuntimeEL.class;
    elDefClasses[i++] = StringEL.class;
    elDefClasses[i++] = MathEL.class;
    elDefClasses[i++] = FileEL.class;
    return elDefClasses;
  }

  public static ELEval createElEval(String configName, Class ...elDefs) {
    return new ELEvaluator(configName, getElDefClasses(elDefs));
  }

  public static ELVars createELVars() {
    return new ELVariables();
  }
}
