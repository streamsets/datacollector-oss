/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.definition;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.ModelDefinition;
import com.streamsets.datacollector.config.ModelType;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.FieldValueChooser;
import com.streamsets.pipeline.api.LanePredicateMapping;
import com.streamsets.pipeline.api.MultiValueChooser;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ModelDefinitionExtractor {

  public abstract List<ErrorMessage> validate(String configPrefix, Field field, Object contextMsg);

  public abstract ModelDefinition extract(String configPrefix, Field field, Object contextMsg);

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
          .put(MultiValueChooser.class, new MultiValueChooserExtractor())
          .put(LanePredicateMapping.class, new LanePredicateMappingExtractor())
          .put(ComplexField.class, new ComplexFieldExtractor())
          .build();

    @Override
    public List<ErrorMessage> validate(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = new ArrayList<>();
      ConfigDef configAnnotation = field.getAnnotation(ConfigDef.class);
      if (configAnnotation != null) {
        if (configAnnotation.type() == ConfigDef.Type.MODEL) {
          List<Annotation> modelAnnotations = new ArrayList<>();
          for (Class<? extends  Annotation> modelAnnotationClass : MODEL_EXTRACTOR.keySet()) {
            Annotation modelAnnotation = field.getAnnotation(modelAnnotationClass);
            if (modelAnnotation != null) {
              modelAnnotations.add(modelAnnotation);
            }
          }
          if (modelAnnotations.isEmpty()) {
            errors.add(new ErrorMessage(DefinitionError.DEF_200, contextMsg));
          }
          if (modelAnnotations.size() > 1)  {
            errors.add(new ErrorMessage(DefinitionError.DEF_201, contextMsg, modelAnnotations));
          }
          if (modelAnnotations.size() > 0) {
            Annotation modelAnnotation = modelAnnotations.get(0);
            ModelDefinitionExtractor extractor = MODEL_EXTRACTOR.get(modelAnnotation.annotationType());
            if (extractor == null) {
              errors.add(new ErrorMessage(DefinitionError.DEF_202, contextMsg, modelAnnotation));
            } else {
              errors.addAll(extractor.validate(configPrefix, field, contextMsg));
            }
          }
        }
      }
      return errors;
    }

    @Override
    public ModelDefinition extract(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = validate(configPrefix, field, contextMsg);
      if (errors.isEmpty()) {
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
            Annotation modelAnnotation = modelAnnotations.iterator().next();
            ModelDefinitionExtractor extractor = MODEL_EXTRACTOR.get(modelAnnotation.annotationType());
            def = extractor.extract(configPrefix, field, contextMsg);
          }
        }
        return def;
      } else {
        throw new IllegalArgumentException(Utils.format("Invalid ModelDefinition: {}", errors));
      }
    }
  }

  static class FieldSelectorExtractor extends ModelDefinitionExtractor {

    @Override
    public List<ErrorMessage> validate(String configPrefix, Field field, Object contextMsg) {
      return new ArrayList<>();
    }

    @Override
    public ModelDefinition extract(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = validate(configPrefix, field, contextMsg);
      if (errors.isEmpty()) {
        FieldSelector fieldSelector = field.getAnnotation(FieldSelector.class);
        ModelType modelType = (fieldSelector.singleValued()) ? ModelType.FIELD_SELECTOR_SINGLE_VALUED
                                                             : ModelType.FIELD_SELECTOR_MULTI_VALUED;
        return new ModelDefinition(modelType, null, null, null, null, null);
      } else {
        throw new IllegalArgumentException(Utils.format("Invalid ModelDefinition: {}", errors));
      }
    }
  }

  static class FieldValueChooserExtractor extends ModelDefinitionExtractor {

    @Override
    public List<ErrorMessage> validate(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = new ArrayList<>();
      try {
        FieldValueChooser fieldValueChooser = field.getAnnotation(FieldValueChooser.class);
        ChooserValues values = fieldValueChooser.value().newInstance();
        values.getValues();
        values.getLabels();
      } catch (Exception ex) {
        errors.add(new ErrorMessage(DefinitionError.DEF_210, contextMsg, ex.toString()));
      }
      return errors;
    }

    @Override
    public ModelDefinition extract(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = validate(configPrefix, field, contextMsg);
      if (errors.isEmpty()) {
        FieldValueChooser fieldValueChooser = field.getAnnotation(FieldValueChooser.class);
        try {
          ChooserValues values = fieldValueChooser.value().newInstance();
          return new ModelDefinition(ModelType.FIELD_VALUE_CHOOSER, values.getClass().getName(), values.getValues(),
                                     values.getLabels(), null, null);
        } catch (Exception ex) {
          throw new RuntimeException(Utils.format("It should not happen: {}", ex.toString()), ex);
        }
      } else {
        throw new IllegalArgumentException(Utils.format("Invalid ModelDefinition: {}", errors));
      }
    }
  }

  static class ValueChooserExtractor extends ModelDefinitionExtractor {

    @Override
    public List<ErrorMessage> validate(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = new ArrayList<>();
      try {
        ValueChooser valueChooser = field.getAnnotation(ValueChooser.class);
        ChooserValues values = valueChooser.value().newInstance();
        values.getValues();
        values.getLabels();
      } catch (Exception ex) {
        errors.add(new ErrorMessage(DefinitionError.DEF_220, contextMsg, ex.toString()));
      }
      return errors;
    }

    @Override
    public ModelDefinition extract(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = validate(configPrefix, field, contextMsg);
      if (errors.isEmpty()) {
        ValueChooser valueChooser = field.getAnnotation(ValueChooser.class);
        try {
          ChooserValues values = valueChooser.value().newInstance();
          return new ModelDefinition(ModelType.VALUE_CHOOSER, values.getClass().getName(), values.getValues(),
                                     values.getLabels(), null, null);
        } catch (Exception ex) {
          throw new RuntimeException(Utils.format("It should not happen: {}", ex.toString()), ex);
        }
      } else {
        throw new IllegalArgumentException(Utils.format("Invalid ModelDefinition: {}", errors));
      }
    }
  }

  static class MultiValueChooserExtractor extends ModelDefinitionExtractor {

    @Override
    public List<ErrorMessage> validate(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = new ArrayList<>();
      try {
        MultiValueChooser multiValueChooser = field.getAnnotation(MultiValueChooser.class);
        ChooserValues values = multiValueChooser.value().newInstance();
        values.getValues();
        values.getLabels();
      } catch (Exception ex) {
        errors.add(new ErrorMessage(DefinitionError.DEF_220, contextMsg, ex.toString()));
      }
      return errors;
    }

    @Override
    public ModelDefinition extract(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = validate(configPrefix, field, contextMsg);
      if (errors.isEmpty()) {
        MultiValueChooser multiValueChooser = field.getAnnotation(MultiValueChooser.class);
        try {
          ChooserValues values = multiValueChooser.value().newInstance();
          return new ModelDefinition(ModelType.MULTI_VALUE_CHOOSER, values.getClass().getName(), values.getValues(),
            values.getLabels(), null, null);
        } catch (Exception ex) {
          throw new RuntimeException(Utils.format("It should not happen: {}", ex.toString()), ex);
        }
      } else {
        throw new IllegalArgumentException(Utils.format("Invalid ModelDefinition: {}", errors));
      }
    }
  }

  static class LanePredicateMappingExtractor extends ModelDefinitionExtractor {

    @Override
    public List<ErrorMessage> validate(String configPrefix, Field field, Object contextMsg) {
      return new ArrayList<>();
    }

    @Override
    public ModelDefinition extract(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = validate(configPrefix, field, contextMsg);
      if (errors.isEmpty()) {
        return new ModelDefinition(ModelType.LANE_PREDICATE_MAPPING, null, null, null, null, null);
      } else {
        throw new IllegalArgumentException(Utils.format("Invalid ModelDefinition: {}", errors));
      }
    }
  }

  static class ComplexFieldExtractor extends ModelDefinitionExtractor {

    @Override
    public List<ErrorMessage> validate(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = new ArrayList<>();
      if (!List.class.isAssignableFrom(field.getType())) {
        errors.add(new ErrorMessage(DefinitionError.DEF_230, contextMsg));
      } else {
        configPrefix += field.getName() + ".";
        Class complexFieldClass = (Class)((ParameterizedType)field.getGenericType()).getActualTypeArguments()[0];
        errors.addAll(ConfigDefinitionExtractor.get().validateComplexField("", complexFieldClass,
                                                                           Collections.<String>emptyList(),contextMsg));
      }
      return errors;
    }

    @Override
    public ModelDefinition extract(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = validate(configPrefix, field, contextMsg);
      if (errors.isEmpty()) {
        Class complexFieldClass = (Class)((ParameterizedType)field.getGenericType()).getActualTypeArguments()[0];
        return new ModelDefinition(ModelType.COMPLEX_FIELD, null, null, null, complexFieldClass,
                                   ConfigDefinitionExtractor.get().extract("", complexFieldClass,
                                                                           Collections.<String>emptyList(), contextMsg));
      } else {
        throw new IllegalArgumentException(Utils.format("Invalid ModelDefinition: {}", errors));
      }
    }
  }

}