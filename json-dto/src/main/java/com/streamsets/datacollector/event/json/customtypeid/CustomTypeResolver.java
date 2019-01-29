/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.datacollector.event.json.customtypeid;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;

import java.util.Collection;

public class CustomTypeResolver extends StdTypeResolverBuilder {
  public CustomTypeResolver() {
    super();
  }

  @Override
  public StdTypeResolverBuilder init(
      JsonTypeInfo.Id idType, TypeIdResolver idRes
  ) {
    return super.init(idType, idRes);
  }

  @Override
  public TypeSerializer buildTypeSerializer(
      SerializationConfig config, JavaType baseType, Collection<NamedType> subtypes
  ) {
    return super.buildTypeSerializer(config, baseType, subtypes);
  }

  @Override
  public TypeDeserializer buildTypeDeserializer(
      DeserializationConfig config, JavaType baseType, Collection<NamedType> subtypes
  ) {
    return super.buildTypeDeserializer(config, baseType, subtypes);
  }

  @Override
  public StdTypeResolverBuilder inclusion(JsonTypeInfo.As includeAs) {
    return super.inclusion(includeAs);
  }

  @Override
  public StdTypeResolverBuilder typeProperty(String typeIdPropName) {
    return super.typeProperty(typeIdPropName);
  }

  @Override
  public StdTypeResolverBuilder defaultImpl(Class<?> defaultImpl) {
    return super.defaultImpl(defaultImpl);
  }

  @Override
  public StdTypeResolverBuilder typeIdVisibility(boolean isVisible) {
    return super.typeIdVisibility(isVisible);
  }

  @Override
  public Class<?> getDefaultImpl() {
    return super.getDefaultImpl();
  }

  @Override
  public String getTypeProperty() {
    return super.getTypeProperty();
  }

  @Override
  public boolean isTypeIdVisible() {
    return super.isTypeIdVisible();
  }

  @Override
  protected TypeIdResolver idResolver(
      MapperConfig<?> config, JavaType baseType, Collection<NamedType> subtypes, boolean forSer, boolean forDeser
  ) {
    return super.idResolver(config, baseType, subtypes, forSer, forDeser);
  }
}
