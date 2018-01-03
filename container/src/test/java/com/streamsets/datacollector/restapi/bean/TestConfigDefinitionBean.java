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
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ModelDefinition;
import com.streamsets.datacollector.config.ModelType;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.datacollector.restapi.bean.ConfigDefinitionJson;
import com.streamsets.pipeline.api.ConfigDef;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestConfigDefinitionBean {

  @Test(expected = NullPointerException.class)
  public void testConfigDefinitionBeanNull() {
    new ConfigDefinitionJson(null);
  }

  @Test
  public void testConfigDefinitionBean() {
    com.streamsets.datacollector.config.ModelDefinition modelDefinition = new ModelDefinition(
        ModelType.LIST_BEAN,
        "myClass",
        null ,
        null,
        null,
        null,
        null
    );

    List<Object> triggeredBy = new ArrayList<>();
    triggeredBy.add("X");
    triggeredBy.add("Y");
    triggeredBy.add("Z");
    com.streamsets.datacollector.config.ConfigDefinition configDefinition =
      new ConfigDefinition("int", ConfigDef.Type.NUMBER, "l2", "d2", "-1", true, "g", "intVar", modelDefinition, "A",
        triggeredBy, 0, Collections.<ElFunctionDefinition>emptyList(), Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE
        , "text/plain", 0, Collections.<Class> emptyList(), ConfigDef.Evaluation.IMPLICIT, null);

    ConfigDefinitionJson configDefinitionJsonBean =
      new ConfigDefinitionJson(configDefinition);

    Assert.assertEquals(configDefinition.getName(), configDefinitionJsonBean.getName());
    Assert.assertEquals(configDefinition.getDefaultValue(), configDefinitionJsonBean.getDefaultValue());
    Assert.assertEquals(configDefinition.getDependsOn(), configDefinitionJsonBean.getDependsOn());
    Assert.assertEquals(configDefinition.getDescription(), configDefinitionJsonBean.getDescription());
    Assert.assertEquals(configDefinition.getDisplayPosition(), configDefinitionJsonBean.getDisplayPosition());
    Assert.assertEquals(configDefinition.getFieldName(), configDefinitionJsonBean.getFieldName());
    Assert.assertEquals(configDefinition.getGroup(), configDefinitionJsonBean.getGroup());
    Assert.assertEquals(configDefinition.getLabel(), configDefinitionJsonBean.getLabel());
    Assert.assertEquals(configDefinition.getTriggeredByValues(), configDefinitionJsonBean.getTriggeredByValues());
    Assert.assertEquals(configDefinition.getType(), configDefinitionJsonBean.getType());
    Assert.assertEquals(configDefinition.isRequired(), configDefinitionJsonBean.isRequired());

  }

}
