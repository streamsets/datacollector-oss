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
package com.streamsets.datacollector.definition;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.ModelDefinition;
import com.streamsets.datacollector.config.ModelType;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.PredicateModel;
import com.streamsets.pipeline.api.MultiValueChooserModel;
import com.streamsets.pipeline.api.ValueChooserModel;
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
          .put(FieldSelectorModel.class, new FieldSelectorExtractor())
          .put(ValueChooserModel.class, new ValueChooserExtractor())
          .put(MultiValueChooserModel.class, new MultiValueChooserExtractor())
          .put(PredicateModel.class, new PredicateExtractor())
          .put(ListBeanModel.class, new ListBeanExtractor())
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
        FieldSelectorModel fieldSelectorModel = field.getAnnotation(FieldSelectorModel.class);
        ModelType modelType = (fieldSelectorModel.singleValued()) ? ModelType.FIELD_SELECTOR
                                                             : ModelType.FIELD_SELECTOR_MULTI_VALUE;
        return new ModelDefinition(
            modelType,
            null,
            null,
            null,
            null,
            null,
            null
        );
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
        ValueChooserModel valueChooserModel = field.getAnnotation(ValueChooserModel.class);
        ChooserValues values = valueChooserModel.value().newInstance();
      } catch (Exception ex) {
        errors.add(new ErrorMessage(DefinitionError.DEF_220, contextMsg, ex.toString()));
      }
      return errors;
    }

    @Override
    public ModelDefinition extract(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = validate(configPrefix, field, contextMsg);
      if (errors.isEmpty()) {
        ValueChooserModel valueChooserModel = field.getAnnotation(ValueChooserModel.class);
        try {
          ChooserValues values = valueChooserModel.value().newInstance();
          return new ModelDefinition(
              ModelType.VALUE_CHOOSER,
              values.getClass().getName(),
              values.getValues(),
              values.getLabels(),
              null,
              null,
              valueChooserModel.filteringConfig()
          );
        } catch (Exception ex) {
          throw new RuntimeException(Utils.format("Unexpected exception: {}", ex.toString()), ex);
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
        MultiValueChooserModel multiValueChooserModel = field.getAnnotation(MultiValueChooserModel.class);
        ChooserValues values = multiValueChooserModel.value().newInstance();
      } catch (Exception ex) {
        errors.add(new ErrorMessage(DefinitionError.DEF_220, contextMsg, ex.toString()));
      }
      return errors;
    }

    @Override
    public ModelDefinition extract(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = validate(configPrefix, field, contextMsg);
      if (errors.isEmpty()) {
        MultiValueChooserModel multiValueChooserModel = field.getAnnotation(MultiValueChooserModel.class);
        try {
          ChooserValues values = multiValueChooserModel.value().newInstance();
          return new ModelDefinition(
              ModelType.MULTI_VALUE_CHOOSER,
              values.getClass().getName(),
              values.getValues(),
              values.getLabels(),
              null,
              null,
              null
          );
        } catch (Exception ex) {
          throw new RuntimeException(Utils.format("Unexpected exception: {}", ex.toString()), ex);
        }
      } else {
        throw new IllegalArgumentException(Utils.format("Invalid ModelDefinition: {}", errors));
      }
    }
  }

  static class PredicateExtractor extends ModelDefinitionExtractor {

    @Override
    public List<ErrorMessage> validate(String configPrefix, Field field, Object contextMsg) {
      return new ArrayList<>();
    }

    @Override
    public ModelDefinition extract(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = validate(configPrefix, field, contextMsg);
      if (errors.isEmpty()) {
        return new ModelDefinition(
            ModelType.PREDICATE,
            null,
            null,
            null,
            null,
            null,
            null
        );
      } else {
        throw new IllegalArgumentException(Utils.format("Invalid ModelDefinition: {}", errors));
      }
    }
  }

  static class ListBeanExtractor extends ModelDefinitionExtractor {

    @Override
    public List<ErrorMessage> validate(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = new ArrayList<>();
      if (!List.class.isAssignableFrom(field.getType())) {
        errors.add(new ErrorMessage(DefinitionError.DEF_230, contextMsg));
      } else {
        Class listBeanClass = (Class)((ParameterizedType)field.getGenericType()).getActualTypeArguments()[0];
        errors.addAll(ConfigDefinitionExtractor.get().validateComplexField("", listBeanClass,
                                                                           Collections.<String>emptyList(),contextMsg));
      }
      return errors;
    }

    @Override
    public ModelDefinition extract(String configPrefix, Field field, Object contextMsg) {
      List<ErrorMessage> errors = validate(configPrefix, field, contextMsg);
      if (errors.isEmpty()) {
        Class listBeanClass = (Class)((ParameterizedType)field.getGenericType()).getActualTypeArguments()[0];
        return new ModelDefinition(
            ModelType.LIST_BEAN,
            null,
            null,
            null,
            listBeanClass,
            ConfigDefinitionExtractor.get().extract("", listBeanClass, Collections.<String>emptyList(), contextMsg),
            null
        );
      } else {
        throw new IllegalArgumentException(Utils.format("Invalid ModelDefinition: {}", errors));
      }
    }
  }

}
