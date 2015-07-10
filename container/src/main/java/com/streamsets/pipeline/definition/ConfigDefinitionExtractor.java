/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ConfigDefinitionExtractor {

  private static final ConfigDefinitionExtractor EXTRACTOR = new ConfigDefinitionExtractor() {};

  public static ConfigDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public List<ErrorMessage> validate(Class klass, Object contextMsg) {
    return validate("", klass, true, false, false, contextMsg);
  }

  public List<ErrorMessage> validateComplexField(String configPrefix, Class klass, Object contextMsg) {
    return validate(configPrefix, klass, true, false, true, contextMsg);
  }

  private List<ErrorMessage> validate(String configPrefix, Class klass, boolean validateDependencies, boolean isBean,
      boolean isComplexField, Object contextMsg) {
    List<ErrorMessage> errors = new ArrayList<>();
    boolean noConfigs = true;
    for (Field field : klass.getDeclaredFields()) {
      if (field.getAnnotation(ConfigDef.class) != null && field.getAnnotation(ConfigDefBean.class) != null) {
        errors.add(new ErrorMessage(DefinitionError.DEF_152, contextMsg, field.getName()));
      } else {
        if (field.getAnnotation(ConfigDef.class) != null || field.getAnnotation(ConfigDefBean.class) != null) {
          if (!Modifier.isPublic(field.getModifiers())) {
            errors.add(new ErrorMessage(DefinitionError.DEF_150, contextMsg, klass.getSimpleName(), field.getName()));
          }
          if (Modifier.isStatic(field.getModifiers())) {
            errors.add(new ErrorMessage(DefinitionError.DEF_151, contextMsg, klass.getSimpleName(), field.getName()));
          }
          if (Modifier.isFinal(field.getModifiers())) {
            errors.add(new ErrorMessage(DefinitionError.DEF_154, contextMsg, klass.getSimpleName(), field.getName()));
          }
        }
        if (field.getAnnotation(ConfigDef.class) != null) {
          noConfigs = false;
          List<ErrorMessage> subErrors = validateConfigDef(configPrefix, field, isComplexField,
                                                           Utils.formatL("{} Field='{}'", contextMsg,
                                                                         field.getName()));
          errors.addAll(subErrors);
        } else if (field.getAnnotation(ConfigDefBean.class) != null) {
          noConfigs = false;
          List<ErrorMessage> subErrors = validateConfigDefBean(configPrefix + field.getName() + ".", field.getType(),
              isComplexField, Utils.formatL("{} BeanField='{}'", contextMsg, field.getName()));
          errors.addAll(subErrors);
        }
      }
    }
    if (isBean && noConfigs) {
      errors.add(new ErrorMessage(DefinitionError.DEF_160, contextMsg));
    }
    if (errors.isEmpty() & validateDependencies) {
      errors.addAll(validateDependencies(getConfigDefinitions(configPrefix, klass, contextMsg), contextMsg));
    }
    return errors;
  }

  private List<ConfigDefinition> getConfigDefinitions(String configPrefix, Class klass, Object contextMsg) {
    List<ConfigDefinition> defs = new ArrayList<>();
    for (Field field : klass.getFields()) {
      if (field.getAnnotation(ConfigDef.class) != null) {
        defs.add(extractConfigDef(configPrefix, field, Utils.formatL("{} Field='{}'", contextMsg, field.getName())));
      } else if (field.getAnnotation(ConfigDefBean.class) != null) {
        defs.addAll(extract(configPrefix + field.getName() + ".", field.getType(), true,
            Utils.formatL("{} BeanField='{}'", contextMsg, field.getName())));
      }
    }
    return defs;
  }

  public List<ConfigDefinition> extract(Class klass, Object contextMsg) {
    return extract("", klass, contextMsg);
  }

  public List<ConfigDefinition> extract(String configPrefix, Class klass, Object contextMsg) {
    List<ConfigDefinition> defs = extract(configPrefix, klass, false, contextMsg);
    resolveDependencies("", defs, contextMsg);
    return defs;
  }

  private List<ConfigDefinition> extract(String configPrefix, Class klass, boolean isBean, Object contextMsg) {
    List<ErrorMessage> errors = validate(configPrefix, klass, false, isBean, false, contextMsg);
    if (errors.isEmpty()) {
      return getConfigDefinitions(configPrefix, klass, contextMsg);
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

  void resolveDependencies(String configPrefix, List<ConfigDefinition>  defs, Object contextMsg) {
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

  List<ErrorMessage> validateConfigDef(String configPrefix, Field field, boolean isComplexField, Object contextMsg) {
    List<ErrorMessage> errors = new ArrayList<>();
    ConfigDef annotation = field.getAnnotation(ConfigDef.class);
    errors.addAll(ConfigValueExtractor.get().validate(field, annotation, contextMsg));
    if (annotation.type() == ConfigDef.Type.MODEL && field.getAnnotation(ComplexField.class) != null && isComplexField) {
      errors.add(new ErrorMessage(DefinitionError.DEF_161, contextMsg,  field.getName()));
    } else {
      List<ErrorMessage> modelErrors = ModelDefinitionExtractor.get().validate(configPrefix + field.getName() + ".", field, contextMsg);
      if (modelErrors.isEmpty()) {
        ModelDefinition model = ModelDefinitionExtractor.get().extract(configPrefix + field.getName() + ".", field, contextMsg);
        errors.addAll(validateELFunctions(annotation, model, contextMsg));
        errors.addAll(validateELConstants(annotation, model, contextMsg));
      } else {
        errors.addAll(modelErrors);
      }
      if (annotation.type() != ConfigDef.Type.NUMBER &&
          (annotation.min() != Long.MIN_VALUE || annotation.max() != Long.MAX_VALUE)) {
        errors.add(new ErrorMessage(DefinitionError.DEF_155, contextMsg, field.getName()));
      }
      errors.addAll(validateDependsOnName(configPrefix, annotation.dependsOn(),
                                          Utils.formatL("{} Field='{}'", contextMsg, field.getName())));
    }
    return errors;
  }

  @SuppressWarnings("unchecked")
  List<ErrorMessage> validateConfigDefBean(String configPrefix, Class klass, boolean isComplexField, Object contextMsg) {
    List<ErrorMessage> errors = new ArrayList<>();
    try {
      if (!klass.isPrimitive()) {
        klass.getConstructor();
      }
      errors.addAll(validate(configPrefix, klass, false, true, isComplexField, contextMsg));
    } catch (NoSuchMethodException ex) {
      errors.add(new ErrorMessage(DefinitionError.DEF_156, contextMsg, klass.getSimpleName()));
    }
    return errors;
  }

  ConfigDefinition extractConfigDef(String configPrefix, Field field, Object contextMsg) {
    List<ErrorMessage> errors = validateConfigDef(configPrefix, field, false, contextMsg);
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
        String dependsOn = resolveDependsOn(configPrefix, annotation.dependsOn());
        List<Object> triggeredByValues = null;  // done at resolveDependencies() invocation
        ModelDefinition model = ModelDefinitionExtractor.get().extract(configPrefix + field.getName() + ".", field,
                                                                       contextMsg);
        if (model != null) {
          defaultValue = model.getModelType().prepareDefault(defaultValue);
        }
        int displayPosition = annotation.displayPosition();
        List<ElFunctionDefinition> elFunctionDefinitions = getELFunctions(annotation, model, contextMsg);
        List<ElConstantDefinition> elConstantDefinitions = getELConstants(annotation, model ,contextMsg);
        List<Class> elDefs = new ImmutableList.Builder().add(annotation.elDefs()).add(ELDefinitionExtractor.DEFAULT_EL_DEFS).build();
        long min = annotation.min();
        long max = annotation.max();
        String mode = (annotation.mode() != null) ? getMimeString(annotation.mode()) : null;
        int lines = annotation.lines();
        ConfigDef.Evaluation evaluation = annotation.evaluation();
        Map<String, List<Object>> dependsOnMap = null; // done at resolveDependencies() invocation
        def = new ConfigDefinition(field, configPrefix + name, type, label, description, defaultValue, required, group,
                                   fieldName, model, dependsOn, triggeredByValues, displayPosition,
                                   elFunctionDefinitions, elConstantDefinitions, min, max, mode, lines, elDefs,
                                   evaluation, dependsOnMap);
      }
      return def;
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid ConfigDefinition: {}", errors));
    }
  }

  private List<ErrorMessage> validateDependsOnName(String configPrefix, String dependsOn, Object contextMsg) {
    List<ErrorMessage> errors = new ArrayList<>();
    if (!dependsOn.isEmpty()) {
      if (dependsOn.startsWith("^")) {
        if (dependsOn.substring(1).contains("^")) {
          errors.add(new ErrorMessage(DefinitionError.DEF_157, contextMsg));
        }
      } else if (dependsOn.endsWith("^")) {
        boolean gaps = false;
        for (int i = dependsOn.indexOf("^"); !gaps && i < dependsOn.length(); i++) {
          gaps = dependsOn.charAt(i) != '^';
        }
        if (gaps) {
          errors.add(new ErrorMessage(DefinitionError.DEF_158, contextMsg));
        } else {
          int relativeCount = dependsOn.length() - dependsOn.indexOf("^");
          int dotCount = configPrefix.split("\\.").length;
          if (relativeCount > dotCount) {
            errors.add(new ErrorMessage(DefinitionError.DEF_159, contextMsg, relativeCount, dotCount, configPrefix));
          }
        }
      }
    }
    return  errors;
  }

  private String resolveDependsOn(String configPrefix, String dependsOn) {
    if (!dependsOn.isEmpty()) {
      if (dependsOn.startsWith("^")) {
        //is absolute from the top
        dependsOn = dependsOn.substring(1);
      } else if (dependsOn.endsWith("^")) {
        configPrefix = configPrefix.substring(0, configPrefix.length() - 1);
        //is relative backwards based on the ^ count
        int relativeCount = dependsOn.length() - dependsOn.indexOf("^");
        while (relativeCount > 0) {
          int pos = configPrefix.lastIndexOf(".");
          configPrefix = (pos == -1) ? "" : configPrefix.substring(0, pos);
          relativeCount--;
        }
        if (!configPrefix.isEmpty()) {
          configPrefix += ".";
        }
        dependsOn = configPrefix + dependsOn.substring(0, dependsOn.indexOf("^"));
      } else {
        dependsOn = configPrefix + dependsOn;
      }
    }
    return  dependsOn;
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