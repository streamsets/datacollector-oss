/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.el.ElFunctionArgumentDefinition;
import com.streamsets.pipeline.el.ElFunctionDefinition;
import com.streamsets.pipeline.el.JvmEL;
import com.streamsets.pipeline.el.RuntimeEL;
import com.streamsets.pipeline.lib.el.StringEL;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

public abstract class ELDefinitionExtractor {
  public static final Class[] DEFAULT_EL_DEFS = {RuntimeEL.class, StringEL.class, JvmEL.class};

  private static final ELDefinitionExtractor EXTRACTOR = new ELDefinitionExtractor() {};

  public static ELDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public List<ElFunctionDefinition> extractFunctions(Class[] classes, Object contextMsg) {
    return extractFunctions(ImmutableList.<Class>builder().add(DEFAULT_EL_DEFS).add(classes).build(), contextMsg);
  }

  List<ElFunctionDefinition> extractFunctions(List<Class> augmentedClasses, Object contextMsg) {
    List<ElFunctionDefinition> fDefs = new ArrayList<>();
    for (Class<?> klass : augmentedClasses) {
      for (Method method : klass.getDeclaredMethods()) {
        if (method.getAnnotation(ElFunction.class) != null) {
          if (!Modifier.isPublic(method.getModifiers())) {
            throw new IllegalArgumentException(Utils.format(
                "{} Class='{}' Method='{}', method must be public to be an EL function", contextMsg,
                klass.getSimpleName(), method.getName()));
          }
        }
      }
      for (Method method : klass.getMethods()) {
        ElFunction fAnnotation = method.getAnnotation(ElFunction.class);
        if (fAnnotation != null) {
          String fName = fAnnotation.name();
          if (fName.isEmpty()) {
            throw new IllegalArgumentException(
                Utils.format("{} Class='{}' Method='{}', EL function name cannot be empty", contextMsg,
                             klass.getSimpleName(), method.getName()));
          }
          if (!fAnnotation.prefix().isEmpty()) {
            fName = fAnnotation.prefix() + ":" + fName;
          }
          if (!Modifier.isStatic(method.getModifiers())) {
            throw new IllegalArgumentException(Utils.format("{} Class='{}' Function='{}', method must be static",
                                                            contextMsg, klass.getSimpleName(), fName));
          }
          Annotation[][] pAnnotations = method.getParameterAnnotations();
          Class<?>[] pTypes = method.getParameterTypes();
          List<ElFunctionArgumentDefinition> fArgDefs = new ArrayList<>(pTypes.length);
          for (int i = 0; i < pTypes.length; i++) {
            Annotation pAnnotation = pAnnotations[i][0];
            fArgDefs.add(new ElFunctionArgumentDefinition(((ElParam) pAnnotation).value(), pTypes[i].getSimpleName()));
          }
          fDefs.add(new ElFunctionDefinition(fAnnotation.prefix(), fName, fAnnotation.description(), fArgDefs,
                                             method.getReturnType().getSimpleName()));
        }
      }
    }
    return fDefs;
  }

  public List<ElConstantDefinition> extractConstants(Class[] classes, Object contextMsg) {
    return extractConstants(ImmutableList.<Class>builder().add(DEFAULT_EL_DEFS).add(classes).build(), contextMsg);
  }

  public List<ElConstantDefinition> extractConstants(List<Class> augmentedClasses, Object contextMsg) {
    List<ElConstantDefinition> cDefs = new ArrayList<>();
    for (Class<?> klass : augmentedClasses) {
      for (Field field : klass.getDeclaredFields()) {
        if (field.getAnnotation(ElConstant.class) != null) {
          if (!Modifier.isPublic(field.getModifiers())) {
            throw new IllegalArgumentException(Utils.format(
                "{} Class='{}' Field='{}', field must public to be an EL constant", contextMsg, klass.getSimpleName(),
                field.getName()));
          }
        }
      }
      for (Field field : klass.getFields()) {
        ElConstant cAnnotation = field.getAnnotation(ElConstant.class);
        if (cAnnotation != null) {
          String cName = cAnnotation.name();
          if (cName.isEmpty()) {
            throw new IllegalArgumentException(
                Utils.format("{} Class='{}' Field='{}', EL constant name cannot be empty",
                             contextMsg, klass.getSimpleName(), field.getName()));
          }
          if (!Modifier.isStatic(field.getModifiers())) {
            throw new IllegalArgumentException(Utils.format("{} Class='{}' Constant='{}', field must static",
                                                            contextMsg, klass.getSimpleName(), cName));
          }
          cDefs.add(new ElConstantDefinition(cName, cAnnotation.description(), field.getType().getSimpleName()));
        }
      }
    }
    return cDefs;
  }

}
