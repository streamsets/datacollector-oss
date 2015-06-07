/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ModelDefinition;
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.el.ElFunctionDefinition;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
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

  public List<ConfigDefinition> extract(Class klass, Object contextMsg) {
    Set<String> names = new HashSet<>();
    List<ConfigDefinition> defs = new ArrayList<>();
    for (Field field : klass.getDeclaredFields()) {
      if (field.getAnnotation(ConfigDef.class) != null) {
        if (!Modifier.isPublic(field.getModifiers())) {
          throw new IllegalArgumentException(Utils.format(
              "{} Class='{}' Field='{}', field must public to be an configuration", contextMsg, klass.getSimpleName(),
              field.getName()));
        }
      }
    }
    for (Field field : klass.getFields()) {
      if (field.getAnnotation(ConfigDef.class) != null) {
        ConfigDefinition def = extract(field, Utils.formatL("{} Field='{}'", contextMsg, field.getName()));
        Utils.checkArgument(!names.contains(def.getName()), Utils.formatL(
            "{}, there cannot be 2 configurations with the same name '{}'", contextMsg, def.getName()));
        names.add(def.getName());
        defs.add(def);
      }
    }
    resolveDependencies(defs, contextMsg);
    return defs;
  }

  void resolveDependencies(List<ConfigDefinition>  defs, Object contextMsg) {
    Map<String, ConfigDefinition> definitionsMap = new HashMap<>();
    for (ConfigDefinition def : defs) {
      definitionsMap.put(def.getName(), def);
    }
    for (ConfigDefinition def : defs) {
      if (!def.getDependsOn().isEmpty()) {
        ConfigDefinition dependsOnDef = definitionsMap.get(def.getDependsOn());

        // verify dependsOn config exists
        Utils.checkArgument(dependsOnDef != null, Utils.formatL(
            "{}, configuration '{}' depends on an non-existing configuration '{}'", contextMsg, def.getName(),
            def.getDependsOn()));

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

  ConfigDefinition extract(Field field, Object contextMsg) {
    ConfigDefinition def = null;
    ConfigDef annotation = field.getAnnotation(ConfigDef.class);
    if (annotation != null) {
      Utils.checkArgument(!Modifier.isStatic(field.getModifiers()),
                          Utils.formatL("{}, configuration field cannot be static", contextMsg));
      Utils.checkArgument(Modifier.isPublic(field.getModifiers()),
                          Utils.formatL("{}, configuration field must be public", contextMsg));
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
      List<ElFunctionDefinition> elFunctionDefinitions =
          ELDefinitionExtractor.get().extractFunctions(annotation.elDefs(), contextMsg);
      List<ElConstantDefinition> elConstantDefinitions =
          ELDefinitionExtractor.get().extractConstants(annotation.elDefs(), contextMsg);
      long min = annotation.min();
      long max = annotation.max();
      String mode = (annotation.mode() != null) ? annotation.mode().name() : null;
      int lines = annotation.lines();
      ConfigDef.Evaluation evaluation = annotation.evaluation();
      Map<String, List<Object>> dependsOnMap = null; // done at resolveDependencies() invocation
      def = new ConfigDefinition(field, name, type, label, description, defaultValue, required, group, fieldName, model,
                                 dependsOn, triggeredByValues, displayPosition, elFunctionDefinitions,
                                 elConstantDefinitions, min, max, mode, lines, null, evaluation, dependsOnMap);
    }
    return def;
  }


}