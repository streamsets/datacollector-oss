/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ModelDefinition;
import com.streamsets.pipeline.config.ModelType;
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.el.ElFunctionDefinition;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ConfigDefinitionExtractor {

  private static final ConfigDefinitionExtractor EXTRACTOR = new ConfigDefinitionExtractor() {};

  public static ConfigDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public List<ErrorMessage> validate(Class klass, Object contextMsg) {
    List<ErrorMessage> errors = new ArrayList<>();
    Set<String> names = new HashSet<>();
    for (Field field : klass.getDeclaredFields()) {
      if (field.getAnnotation(ConfigDef.class) != null) {
        if (!Modifier.isPublic(field.getModifiers())) {
          errors.add(new ErrorMessage(DefinitionError.DEF_150, contextMsg, klass.getSimpleName(), field.getName()));
        }
        if (Modifier.isStatic(field.getModifiers())) {
          errors.add(new ErrorMessage(DefinitionError.DEF_151, contextMsg, klass.getSimpleName(), field.getName()));
        }
        if (Modifier.isFinal(field.getModifiers())) {
          errors.add(new ErrorMessage(DefinitionError.DEF_154, contextMsg, klass.getSimpleName(), field.getName()));
        }
        List<ErrorMessage> subErrors = validate(field, Utils.formatL("{} Field='{}'", contextMsg, field.getName()));
        errors.addAll(subErrors);
        if (names.contains(field.getName())) {
          errors.add(new ErrorMessage(DefinitionError.DEF_152, contextMsg, field.getName()));
        }
        names.add(field.getName());
      }
      if (errors.isEmpty()) {
        List<ConfigDefinition> defs = getConfigDefinitions(klass, contextMsg);
        errors.addAll(validateDependencies(defs, contextMsg));
      }
    }
    return errors;
  }

  private List<ConfigDefinition> getConfigDefinitions(Class klass, Object contextMsg) {
    List<ConfigDefinition> defs = new ArrayList<>();
    for (Field field : klass.getFields()) {
      if (field.getAnnotation(ConfigDef.class) != null) {
        defs.add(extract(field, Utils.formatL("{} Field='{}'", contextMsg, field.getName())));
      }
    }
    return defs;
  }

  public List<ConfigDefinition> extract(Class klass, Object contextMsg) {
    List<ErrorMessage> errors = validate(klass, contextMsg);
    if (errors.isEmpty()) {
      List<ConfigDefinition> defs = getConfigDefinitions(klass, contextMsg);
      resolveDependencies(defs, contextMsg);
      return defs;
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid ConfigDefinition: {}", errors));
    }
  }

  private List<ErrorMessage> validateDependencies(List<ConfigDefinition>  defs, Object contextMsg) {
    List<ErrorMessage> errors = new ArrayList<>();
    Map<String, ConfigDefinition> definitionsMap = new HashMap<>();
    for (ConfigDefinition def : defs) {
      definitionsMap.put(def.getName(), def);
    }
    for (ConfigDefinition def : defs) {
      if (!def.getDependsOn().isEmpty()) {
        ConfigDefinition dependsOnDef = definitionsMap.get(def.getDependsOn());

        if (dependsOnDef == null) {
          errors.add(new ErrorMessage(DefinitionError.DEF_153, contextMsg, contextMsg, def.getName(),
                                      def.getDependsOn()));
        } else {
          // evaluate dependsOn triggers
          ConfigDef annotation = def.getConfigField().getAnnotation(ConfigDef.class);
          for (String trigger : annotation.triggeredByValue()) {
            errors.addAll(ConfigValueExtractor.get().validate(dependsOnDef.getConfigField(), dependsOnDef.getType(),
                                                              trigger, contextMsg, true));
          }
        }
      }
    }
    return errors;
  }

  void resolveDependencies(List<ConfigDefinition>  defs, Object contextMsg) {
    Map<String, ConfigDefinition> definitionsMap = new HashMap<>();
    for (ConfigDefinition def : defs) {
      definitionsMap.put(def.getName(), def);
    }
    for (ConfigDefinition def : defs) {
      if (!def.getDependsOn().isEmpty()) {
        ConfigDefinition dependsOnDef = definitionsMap.get(def.getDependsOn());

        // evaluate dependsOn triggers
        ConfigDef annotation = def.getConfigField().getAnnotation(ConfigDef.class);
        List<Object> triggers = new ArrayList<>();
        for (String trigger : annotation.triggeredByValue()) {
          triggers.add(ConfigValueExtractor.get().extract(dependsOnDef.getConfigField(), dependsOnDef.getType(),
                                                          trigger, contextMsg, true));
        }
        def.setTriggeredByValues(triggers);
      }
    }

    // compute dependsOnChain
    for (ConfigDefinition def : defs) {
      ConfigDefinition tempConfigDef = def;
      Map<String, List<Object>> dependsOnMap = new HashMap<>();
      while(tempConfigDef != null && tempConfigDef.getDependsOn() != null && !tempConfigDef.getDependsOn().isEmpty()) {
        dependsOnMap.put(tempConfigDef.getDependsOn(), tempConfigDef.getTriggeredByValues());
        tempConfigDef = definitionsMap.get(tempConfigDef.getDependsOn());
      }
      if(!dependsOnMap.isEmpty()) {
        def.setDependsOnMap(dependsOnMap);
      }
    }
  }

  List<ErrorMessage> validate(Field field, Object contextMsg) {
    List<ErrorMessage> errors = new ArrayList<>();
    ConfigDef annotation = field.getAnnotation(ConfigDef.class);
    errors.addAll(ConfigValueExtractor.get().validate(field, annotation, contextMsg));
    List<ErrorMessage> modelErrors = ModelDefinitionExtractor.get().validate(field, contextMsg);
    if (modelErrors.isEmpty()) {
      ModelDefinition model = ModelDefinitionExtractor.get().extract(field, contextMsg);
      errors.addAll(validateELFunctions(annotation, model, contextMsg));
      errors.addAll(validateELConstants(annotation, model, contextMsg));
    } else {
      errors.addAll(modelErrors);
    }
    if (annotation.type() != ConfigDef.Type.NUMBER &&
        (annotation.min() != Long.MIN_VALUE || annotation.max() != Long.MAX_VALUE)) {
      errors.add(new ErrorMessage(DefinitionError.DEF_155, contextMsg, field.getName()));
    }
    return errors;
  }

  ConfigDefinition extract(Field field, Object contextMsg) {
    List<ErrorMessage> errors = validate(field, contextMsg);
    if (errors.isEmpty()) {
      ConfigDefinition def = null;
      ConfigDef annotation = field.getAnnotation(ConfigDef.class);
      if (annotation != null) {
        String name = field.getName();
        ConfigDef.Type type = annotation.type();
        String label = annotation.label();
        String description = annotation.description();
        Object defaultValue = ConfigValueExtractor.get().extract(field, annotation, contextMsg);
        boolean required = annotation.required();
        String group = annotation.group();
        String fieldName = field.getName();
        String dependsOn = annotation.dependsOn();
        List<Object> triggeredByValues = null;  // done at resolveDependencies() invocation
        ModelDefinition model = ModelDefinitionExtractor.get().extract(field, contextMsg);
        int displayPosition = annotation.displayPosition();
        List<ElFunctionDefinition> elFunctionDefinitions = getELFunctions(annotation, model, contextMsg);
        List<ElConstantDefinition> elConstantDefinitions = getELConstants(annotation, model ,contextMsg);
        List<Class> elDefs = ImmutableList.copyOf(annotation.elDefs());
        long min = annotation.min();
        long max = annotation.max();
        String mode = (annotation.mode() != null) ? getMimeString(annotation.mode()) : null;
        int lines = annotation.lines();
        ConfigDef.Evaluation evaluation = annotation.evaluation();
        Map<String, List<Object>> dependsOnMap = null; // done at resolveDependencies() invocation
        def = new ConfigDefinition(field, name, type, label, description, defaultValue, required, group, fieldName, model,
                                   dependsOn, triggeredByValues, displayPosition, elFunctionDefinitions,
                                   elConstantDefinitions, min, max, mode, lines, elDefs, evaluation, dependsOnMap);
      }
      return def;
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid ConfigDefinition: {}", errors));
    }
  }


  private String getMimeString(ConfigDef.Mode mode) {
    switch(mode) {
      case JSON:
        return "application/json";
      case PLAIN_TEXT:
        return "text/plain";
      case PYTHON:
        return "text/x-python";
      case JAVASCRIPT:
        return "text/javascript";
      case RUBY:
        return "text/x-ruby";
      case JAVA:
        return "text/x-java";
      case SCALA:
        return "text/x-scala";
      case SQL:
        return "text/x-sql";
      default:
        return null;
    }
  }

  private static final Set<ConfigDef.Type> TYPES_SUPPORTING_ELS = ImmutableSet.of(
      ConfigDef.Type.LIST, ConfigDef.Type.MAP, ConfigDef.Type.NUMBER, ConfigDef.Type.STRING, ConfigDef.Type.TEXT);

  private static final Set<ModelType> MODELS_SUPPORTING_ELS = ImmutableSet.of(ModelType.LANE_PREDICATE_MAPPING);

  private List<ErrorMessage> validateELFunctions(ConfigDef annotation,ModelDefinition model,  Object contextMsg) {
    List<ErrorMessage> errors;
    if (TYPES_SUPPORTING_ELS.contains(annotation.type()) ||
        (annotation.type() == ConfigDef.Type.MODEL && MODELS_SUPPORTING_ELS.contains(model.getModelType()))) {
      errors = ELDefinitionExtractor.get().validateFunctions(annotation.elDefs(), contextMsg);
    } else {
      errors = new ArrayList<>();
    }
    return errors;
  }

  private List<ElFunctionDefinition> getELFunctions(ConfigDef annotation,ModelDefinition model,  Object contextMsg) {
    List<ElFunctionDefinition> functions = Collections.emptyList();
    if (TYPES_SUPPORTING_ELS.contains(annotation.type()) ||
        (annotation.type() == ConfigDef.Type.MODEL && MODELS_SUPPORTING_ELS.contains(model.getModelType()))) {
      functions = ELDefinitionExtractor.get().extractFunctions(annotation.elDefs(), contextMsg);
    }
    return functions;
  }

  private List<ErrorMessage> validateELConstants(ConfigDef annotation, ModelDefinition model, Object contextMsg) {
    List<ErrorMessage> errors;
    if (TYPES_SUPPORTING_ELS.contains(annotation.type()) ||
        (annotation.type() == ConfigDef.Type.MODEL && MODELS_SUPPORTING_ELS.contains(model.getModelType()))) {
      errors = ELDefinitionExtractor.get().validateConstants(annotation.elDefs(), contextMsg);
    } else {
      errors = new ArrayList<>();
    }
    return errors;
  }

  private List<ElConstantDefinition> getELConstants(ConfigDef annotation, ModelDefinition model, Object contextMsg) {
    List<ElConstantDefinition> functions = Collections.emptyList();
    if (TYPES_SUPPORTING_ELS.contains(annotation.type()) ||
        (annotation.type() == ConfigDef.Type.MODEL && MODELS_SUPPORTING_ELS.contains(model.getModelType()))) {
      functions = ELDefinitionExtractor.get().extractConstants(annotation.elDefs(), contextMsg);
    }
    return functions;
  }

}