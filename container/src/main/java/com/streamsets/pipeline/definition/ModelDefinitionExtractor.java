/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.FieldValueChooser;
import com.streamsets.pipeline.api.LanePredicateMapping;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ModelDefinition;
import com.streamsets.pipeline.config.ModelType;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ModelDefinitionExtractor {

  public abstract ModelDefinition extract(Field field, Object contextMsg);

  private static final ModelDefinitionExtractor EXTRACTOR = new Extractor();

  public static ModelDefinitionExtractor get() {
    return EXTRACTOR;
  }

  static class Extractor extends ModelDefinitionExtractor {

    private static final Map<Class<? extends Annotation>, ModelDefinitionExtractor> MODEL_EXTRACTOR =
        ImmutableMap.<Class<? extends Annotation>, ModelDefinitionExtractor>builder()
                    .put(FieldSelector.class, new FieldSelectorExtractor())
                    .put(FieldValueChooser.class, new FieldValueChooserExtractor())
                    .put(ValueChooser.class, new ValueChooserExtractor())
                    .put(LanePredicateMapping.class, new LanePredicateMappingExtractor())
                    .put(ComplexField.class, new ComplexFieldExtractor())
                    .build();
    @Override
    public ModelDefinition extract(Field field, Object contextMsg) {
      ModelDefinition def = null;
      ConfigDef configAnnotation = field.getAnnotation(ConfigDef.class);
      if (configAnnotation != null) {
        if (configAnnotation.type() == ConfigDef.Type.MODEL) {
          Set<Annotation> modelAnnotations = new HashSet<>();
          for (Class<? extends  Annotation> modelAnnotationClass : MODEL_EXTRACTOR.keySet()) {
            Annotation modelAnnotation = field.getAnnotation(modelAnnotationClass);
            if (modelAnnotation != null) {
              modelAnnotations.add(modelAnnotation);
            }
          }
          Utils.checkArgument(!modelAnnotations.isEmpty(), Utils.formatL("{}, Model annotation missing'", contextMsg));
          Utils.checkArgument(modelAnnotations.size() == 1, Utils.formatL("{}, only one Model annotation is allowed, {}",
                                                                          contextMsg, modelAnnotations));
          Annotation modelAnnotation = modelAnnotations.iterator().next();
          ModelDefinitionExtractor extractor = MODEL_EXTRACTOR.get(modelAnnotation.annotationType());
          Utils.checkState(extractor != null, Utils.formatL("{}, paser for Model '{}' not found", contextMsg,
                                                         modelAnnotation));
          def = extractor.extract(field, contextMsg);
        }
      }
      return def;
    }
  }

  static class FieldSelectorExtractor extends ModelDefinitionExtractor {
    @Override
    public ModelDefinition extract(Field field, Object contextMsg) {
      FieldSelector fieldSelector = field.getAnnotation(FieldSelector.class);
      ModelType modelType = (fieldSelector.singleValued()) ? ModelType.FIELD_SELECTOR_SINGLE_VALUED
                                                           : ModelType.FIELD_SELECTOR_MULTI_VALUED;
      return new ModelDefinition(modelType, null, null, null, null);
    }
  }

  static class FieldValueChooserExtractor extends ModelDefinitionExtractor {
    @Override
    public ModelDefinition extract(Field field, Object contextMsg) {
      FieldValueChooser fieldValueChooser = field.getAnnotation(FieldValueChooser.class);
      try {
        ChooserValues values = fieldValueChooser.value().newInstance();
        return new ModelDefinition(ModelType.FIELD_VALUE_CHOOSER, values.getClass().getName(), values.getValues(),
                                    values.getLabels(), null);
      } catch (Exception ex) {
        throw new IllegalArgumentException(Utils.format("{}, could not evaluate ChooserValue: {}",
                                                        contextMsg, ex.getMessage()), ex);
      }
    }
  }

  static class ValueChooserExtractor extends ModelDefinitionExtractor {
    @Override
    public ModelDefinition extract(Field field, Object contextMsg) {
      ValueChooser valueChooser = field.getAnnotation(ValueChooser.class);
      try {
        ChooserValues values = valueChooser.value().newInstance();
        return new ModelDefinition(ModelType.VALUE_CHOOSER, values.getClass().getName(), values.getValues(),
                                    values.getLabels(), null);
      } catch (Exception ex) {
        throw new IllegalArgumentException(Utils.format("{}, could not evaluate ChooserValue: {}",
                                                        contextMsg, ex.getMessage()), ex);
      }
    }
  }

  static class LanePredicateMappingExtractor extends ModelDefinitionExtractor {
    @Override
    public ModelDefinition extract(Field field, Object contextMsg) {
      return new ModelDefinition(ModelType.LANE_PREDICATE_MAPPING, null, null, null, null);
    }
  }

  static class ComplexFieldExtractor extends ModelDefinitionExtractor {
    @Override
    public ModelDefinition extract(Field field, Object contextMsg) {
      Utils.checkArgument(List.class.isAssignableFrom(field.getType()), Utils.formatL(
          "{}, ComplexField configuration must be a list", contextMsg));
      ComplexField complexField = field.getAnnotation(ComplexField.class);
      Class complexFieldClass = complexField.value();
      return new ModelDefinition(ModelType.COMPLEX_FIELD, null, null, null,
                                 ConfigDefinitionExtractor.get().extract(complexFieldClass, contextMsg));
    }
  }

}