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
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.ModelDefinitionJson;
import com.streamsets.pipeline.api.ConfigDef;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestModelDefinitionBean {

  @Test(expected = NullPointerException.class)
  public void testModelDefinitionBeanNull() {
    new ModelDefinitionJson(null);
  }

  @Test
  public void testModelDefinitionBean() {
    List<String> values = new ArrayList<>();
    values.add("v1");
    values.add("v2");

    List<String> labels = new ArrayList<>();
    labels.add("V ONE");
    labels.add("V TWO");

    List<Object> triggeredBy = new ArrayList<>();
    triggeredBy.add("X");
    triggeredBy.add("Y");
    triggeredBy.add("Z");

    List< ConfigDefinition > configDefinitions = new ArrayList<>();
    configDefinitions.add(new ConfigDefinition("int", ConfigDef.Type.NUMBER, ConfigDef.Upload.NO, "l2", "d2", "-1", true, "g", "intVar", null, "A",
      triggeredBy, 0, Collections.<ElFunctionDefinition>emptyList(), Collections.<ElConstantDefinition>emptyList(),
      Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(), ConfigDef.Evaluation.IMPLICIT, null, ConfigDef.DisplayMode.BASIC, ""));

    ModelDefinition modelDefinition = new ModelDefinition(
        ModelType.LIST_BEAN,
        "valuesProviderClass",
        values,
        labels,
        String.class,
        configDefinitions,
        null
    );

    ModelDefinitionJson modelDefinitionJsonBean = new ModelDefinitionJson(modelDefinition);

    Assert.assertEquals(modelDefinition.getValues(), modelDefinitionJsonBean.getValues());
    Assert.assertEquals(modelDefinition.getLabels(), modelDefinitionJsonBean.getLabels());
    Assert.assertEquals(modelDefinition.getModelType(), BeanHelper.unwrapModelType(modelDefinitionJsonBean.getModelType()));
    Assert.assertEquals(modelDefinition.getValuesProviderClass(), modelDefinitionJsonBean.getValuesProviderClass());
  }

}
