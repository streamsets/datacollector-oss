/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.restapi.rbean.json.swagger;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.restapi.rbean.lang.RBean;
import com.streamsets.datacollector.restapi.rbean.lang.RBoolean;
import com.streamsets.datacollector.restapi.rbean.lang.RChar;
import com.streamsets.datacollector.restapi.rbean.lang.RDate;
import com.streamsets.datacollector.restapi.rbean.lang.RDatetime;
import com.streamsets.datacollector.restapi.rbean.lang.RDecimal;
import com.streamsets.datacollector.restapi.rbean.lang.RDouble;
import com.streamsets.datacollector.restapi.rbean.lang.REnum;
import com.streamsets.datacollector.restapi.rbean.lang.RLong;
import com.streamsets.datacollector.restapi.rbean.lang.RString;
import com.streamsets.datacollector.restapi.rbean.lang.RText;
import com.streamsets.datacollector.restapi.rbean.lang.RTime;
import io.swagger.converter.ModelConverter;
import io.swagger.converter.ModelConverterContext;
import io.swagger.jackson.ModelResolver;
import io.swagger.models.ComposedModel;
import io.swagger.models.Model;
import io.swagger.models.ModelImpl;
import io.swagger.models.properties.BooleanProperty;
import io.swagger.models.properties.DecimalProperty;
import io.swagger.models.properties.DoubleProperty;
import io.swagger.models.properties.LongProperty;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.PropertyBuilder;
import io.swagger.models.properties.RefProperty;
import io.swagger.models.properties.StringProperty;
import io.swagger.util.Json;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class RJsonModelConverter extends ModelResolver {
  private static final Logger LOG = LoggerFactory.getLogger(RJsonModelConverter.class);
  public RJsonModelConverter(ObjectMapper mapper) {
    super(mapper);
  }

  public static final String DATA_VERSION = "dataVersion";
  public static final String READ_ONLY_SIGNATURE = "readOnlySignature";
  public static final String FAILED = "failed";
  public static final String MESSAGES = "messages";
  public static final List<String> RBEAN_INTERNAL_FIELDS =
      ImmutableList.of(DATA_VERSION, READ_ONLY_SIGNATURE, FAILED, MESSAGES);

  @Override
  public Property resolveProperty(java.lang.reflect.Type type, ModelConverterContext context, Annotation[] annotations,
      Iterator<ModelConverter> chain) {
    final JavaType jType = _mapper.constructType(type);
    if (jType != null) {
      final Class<?> cls = jType.getRawClass();
      if (cls.equals(RString.class)) {
        return new StringProperty();
      } else if (cls.equals(RChar.class)) {
        return new StringProperty();
      } else if (cls.equals(RText.class)) {
        return new StringProperty();
      } else if (cls.equals(RDate.class)) {
        return new LongProperty();
      } else if (cls.equals(RDatetime.class)) {
        return new LongProperty();
      } else if (cls.equals(RTime.class)) {
        return new LongProperty();
      } else if (cls.equals(RLong.class)) {
        return new LongProperty();
      } else if (cls.equals(RBoolean.class)) {
        return new BooleanProperty();
      } else if(cls.equals(RDecimal.class)) {
        return new DecimalProperty();
      } else if(cls.equals(RDouble.class)) {
        return new DoubleProperty();
      } else if(cls.equals(REnum.class)) {
        Property property = new StringProperty();
        if (jType.containedTypeCount() > 0) {
          _addEnumProps(jType.containedType(0).getRawClass(), property);
        }
        return property;
      } else if (RBean.class.isAssignableFrom(cls)) {
        // complex type
        Model innerModel = context.resolve(jType);
        RBEAN_INTERNAL_FIELDS.forEach(innerModel.getProperties()::remove);
        if (innerModel instanceof ComposedModel) {
          innerModel = ((ComposedModel) innerModel).getChild();
        }
        if (innerModel instanceof ModelImpl) {
          ModelImpl mi = (ModelImpl) innerModel;
          return new RefProperty(StringUtils.isNotEmpty(mi.getReference()) ? mi.getReference() : mi.getName());
        }
      }
      return chain.next().resolveProperty(type, context, annotations, chain);
    } else {
      return null;
    }
  }

  @Override
  public Model resolve(java.lang.reflect.Type type, ModelConverterContext context, Iterator<ModelConverter> chain) {
    JavaType javaType = _mapper.constructType(type);
    Model model = super.resolve(type, context, chain);
    if (javaType.isTypeOrSubTypeOf(RBean.class)) {
      RBEAN_INTERNAL_FIELDS.forEach(model.getProperties()::remove);
    }
    return model;
  }
}
