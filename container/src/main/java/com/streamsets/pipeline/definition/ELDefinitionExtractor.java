/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.google.common.collect.ImmutableSet;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ELDefinitionExtractor {
  public static final Class[] DEFAULT_EL_DEFS = {RuntimeEL.class, StringEL.class, JvmEL.class};

  private static final ELDefinitionExtractor EXTRACTOR = new ELDefinitionExtractor() {};

  public static ELDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public List<ElConstantDefinition> extractConstants(Class[] classes, Object contextMsg) {
    return extractConstants(ImmutableSet.<Class>builder().add(DEFAULT_EL_DEFS).add(classes).build(), contextMsg);
  }

  public List<ElFunctionDefinition> extractFunctions(Class[] classes, Object contextMsg) {
    return extractFunctions(ImmutableSet.<Class>builder().add(DEFAULT_EL_DEFS).add(classes).build(), contextMsg);
  }

  public Map<String, ElFunctionDefinition> getElFunctionsCatalog() {
    return elFunctionsIdx;
  }

  public Map<String, ElConstantDefinition> getELConstantsCatalog() {
    return elConstantsIdx;
  }

  private AtomicInteger indexCounter;
  private final Map<Method, ElFunctionDefinition> elFunctions;
  private final Map<Field, ElConstantDefinition> elConstants;
  private final Map<String, ElFunctionDefinition> elFunctionsIdx;
  private final Map<String, ElConstantDefinition> elConstantsIdx;

  private ELDefinitionExtractor() {
    indexCounter = new AtomicInteger();
    elFunctions = new ConcurrentHashMap<>();
    elConstants = new ConcurrentHashMap<>();
    elFunctionsIdx = new ConcurrentHashMap<>();
    elConstantsIdx = new ConcurrentHashMap<>();
  }

  List<ElFunctionDefinition> extractFunctions(Set<Class> augmentedClasses, Object contextMsg) {
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
        ElFunctionDefinition fDef = elFunctions.get(method);
        if (fDef == null) {
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
            fDef = new ElFunctionDefinition(Integer.toString(indexCounter.incrementAndGet()), fAnnotation.prefix(),
                                            fName, fAnnotation.description(), fArgDefs,
                                            method.getReturnType().getSimpleName());
            elFunctionsIdx.put(fDef.getIndex(), fDef);
            elFunctions.put(method, fDef);
          }
        }
        if (fDef != null) {
          fDefs.add(fDef);
        }
      }
    }
    return fDefs;
  }

  List<ElConstantDefinition> extractConstants(Set<Class> augmentedClasses, Object contextMsg) {
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
        ElConstantDefinition cDef = elConstants.get(field);
        if (cDef == null) {
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
            cDef = new ElConstantDefinition(Integer.toString(indexCounter.incrementAndGet()), cName,
                                            cAnnotation.description(), field.getType().getSimpleName());
            elConstantsIdx.put(cDef.getIndex(), cDef);
            elConstants.put(field, cDef);
          }
        }
        if (cDef != null) {
          cDefs.add(cDef);
        }
      }
    }
    return cDefs;
  }

}
