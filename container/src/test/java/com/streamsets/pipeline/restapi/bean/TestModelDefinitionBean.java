/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ModelType;
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.el.ElFunctionDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestModelDefinitionBean {

  @Test(expected = NullPointerException.class)
  public void testModelDefinitionBeanNull() {
    ModelDefinitionJson modelDefinitionJson = new ModelDefinitionJson(null);
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
    configDefinitions.add(new ConfigDefinition("int", ConfigDef.Type.NUMBER, "l2", "d2", "-1", true, "g", "intVar", null, "A",
      triggeredBy, 0, Collections.<ElFunctionDefinition>emptyList(), Collections.<ElConstantDefinition>emptyList(),
      Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0));

    com.streamsets.pipeline.config.ModelDefinition modelDefinition =
      new com.streamsets.pipeline.config.ModelDefinition(ModelType.COMPLEX_FIELD,
                                                         "valuesProviderClass", values,labels, configDefinitions);

    ModelDefinitionJson modelDefinitionJsonBean = new ModelDefinitionJson(modelDefinition);

    Assert.assertEquals(modelDefinition.getValues(), modelDefinitionJsonBean.getValues());
    Assert.assertEquals(modelDefinition.getLabels(), modelDefinitionJsonBean.getLabels());
    Assert.assertEquals(modelDefinition.getModelType(), BeanHelper.unwrapModelType(modelDefinitionJsonBean.getModelType()));
    Assert.assertEquals(modelDefinition.getValuesProviderClass(), modelDefinitionJsonBean.getValuesProviderClass());
    Assert.assertEquals(modelDefinition.getConfigDefinitions(),
      BeanHelper.unwrapConfigDefinitions(modelDefinitionJsonBean.getConfigDefinitions()));

  }

  @Test
  public void testModelDefinitionBeanConstrWithArgs() {
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
    configDefinitions.add(new ConfigDefinition("int", ConfigDef.Type.NUMBER, "l2", "d2", "-1", true, "g", "intVar",
      null, "A", triggeredBy, 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0));

    com.streamsets.pipeline.config.ModelDefinition modelDefinition =
      new com.streamsets.pipeline.config.ModelDefinition(ModelType.COMPLEX_FIELD,
                                                         "valuesProviderClass", values,labels, configDefinitions);

    ModelDefinitionJson modelDefinitionJsonBean = new ModelDefinitionJson(ModelTypeJson.COMPLEX_FIELD,
      "valuesProviderClass", values,labels, BeanHelper.wrapConfigDefinitions(configDefinitions));

    Assert.assertEquals(modelDefinition.getValues(), modelDefinitionJsonBean.getValues());
    Assert.assertEquals(modelDefinition.getLabels(), modelDefinitionJsonBean.getLabels());
    Assert.assertEquals(modelDefinition.getModelType(), BeanHelper.unwrapModelType(modelDefinitionJsonBean.getModelType()));
    Assert.assertEquals(modelDefinition.getValuesProviderClass(), modelDefinitionJsonBean.getValuesProviderClass());
    Assert.assertEquals(modelDefinition.getConfigDefinitions(),
      BeanHelper.unwrapConfigDefinitions(modelDefinitionJsonBean.getConfigDefinitions()));

    //test underlying
    Assert.assertEquals(modelDefinition.getValues(), modelDefinitionJsonBean.getModelDefinition().getValues());
    Assert.assertEquals(modelDefinition.getLabels(), modelDefinitionJsonBean.getModelDefinition().getLabels());
    Assert.assertEquals(modelDefinition.getModelType(), modelDefinitionJsonBean.getModelDefinition().getModelType());
    Assert.assertEquals(modelDefinition.getValuesProviderClass(),
      modelDefinitionJsonBean.getModelDefinition().getValuesProviderClass());
    Assert.assertEquals(modelDefinition.getConfigDefinitions(),
      modelDefinitionJsonBean.getModelDefinition().getConfigDefinitions());
  }
}
