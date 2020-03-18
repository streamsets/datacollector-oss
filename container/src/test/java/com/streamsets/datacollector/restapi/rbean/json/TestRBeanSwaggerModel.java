/**
 * Copyright 2020 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.restapi.rbean.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.streamsets.datacollector.restapi.rbean.lang.RBean;
import com.streamsets.datacollector.restapi.rbean.lang.RChar;
import com.streamsets.datacollector.restapi.rbean.lang.RDate;
import com.streamsets.datacollector.restapi.rbean.lang.RDatetime;
import com.streamsets.datacollector.restapi.rbean.lang.RDecimal;
import com.streamsets.datacollector.restapi.rbean.lang.RDouble;
import com.streamsets.datacollector.restapi.rbean.lang.REnum;
import com.streamsets.datacollector.restapi.rbean.lang.RLong;
import com.streamsets.datacollector.restapi.rbean.lang.RString;
import com.streamsets.datacollector.restapi.rbean.lang.RTime;
import io.swagger.converter.ModelConverters;
import io.swagger.models.Model;
import io.swagger.models.properties.ArrayProperty;
import io.swagger.models.properties.DecimalProperty;
import io.swagger.models.properties.DoubleProperty;
import io.swagger.models.properties.LongProperty;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;
import io.swagger.models.properties.StringProperty;
import io.swagger.util.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestRBeanSwaggerModel {

  enum Alphabet {
    A,
    B,
    ;
  }

  private static class RSubBean extends RBean<RSubBean> {
    public RString id;

    @Override
    public RString getId() {
      return id;
    }

    @Override
    public void setId(RString id) {
      this.id = id;
    }
  }

  private static class RAllTypes extends RBean<RAllTypes> {
    public RChar rChar;
    public RString id;
    public RDate rDate;
    public RTime rTime;
    public RLong rLong;
    public RDatetime rDatetime;
    public REnum<Alphabet> rEnum;
    public RDouble rDouble;
    public RDecimal rDecimal;
    public RSubBean rSubBean;
    public List<RSubBean> rSubBeanList;

    @Override
    public RString getId() {
      return id;
    }

    @Override
    public void setId(RString id) {
      this.id = id;
    }
  }

  @Before
  public void setUp() throws Exception {
    ObjectMapper objectMapper = Json.mapper().copy();
    RJson.configureRJsonForSwagger(objectMapper);
  }
  
  @Test
  public void testAllTypes() {
    Map<String, Model> modelMap = ModelConverters.getInstance()
        .read(TypeToken.of(RAllTypes.class).getType());

    Model rAllTypes = modelMap.get(RAllTypes.class.getSimpleName());
    Assert.assertNotNull(rAllTypes);

    Map<String, Class<? extends Property>> expectedProperties = ImmutableMap.<String, Class<? extends Property>>builder()
        .put("id", StringProperty.class)
        .put("rChar", StringProperty.class)
        .put("rDate", LongProperty.class)
        .put("rTime", LongProperty.class)
        .put("rDatetime", LongProperty.class)
        .put("rLong", LongProperty.class)
        .put("rDouble", DoubleProperty.class)
        .put("rDecimal", DecimalProperty.class)
        .put("rEnum", StringProperty.class)
        .put("rSubBean", RefProperty.class)
        .put("rSubBeanList", ArrayProperty.class)
        .build();

    Assert.assertEquals(expectedProperties.size(), rAllTypes.getProperties().size());
    expectedProperties.forEach(
        (k, v) -> Assert.assertEquals(v, rAllTypes.getProperties().get(k).getClass())
    );
    StringProperty rEnumProperty = ((StringProperty)rAllTypes.getProperties().get("rEnum"));
    Assert.assertTrue(
        new HashSet<>(rEnumProperty.getEnum())
            .containsAll(Arrays.stream(Alphabet.values()).map(Enum::name).collect(Collectors.toSet()))
    );

    RefProperty refProperty = (RefProperty) rAllTypes.getProperties().get("rSubBean");
    Assert.assertEquals(refProperty.getSimpleRef(), RSubBean.class.getSimpleName());

    ArrayProperty arrayProperty = (ArrayProperty) rAllTypes.getProperties().get("rSubBeanList");
    Assert.assertEquals(((RefProperty)arrayProperty.getItems()).getSimpleRef(), RSubBean.class.getSimpleName());
  }
}
