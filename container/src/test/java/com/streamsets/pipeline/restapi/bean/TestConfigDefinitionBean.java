/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ModelDefinition;
import com.streamsets.pipeline.config.ModelType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestConfigDefinitionBean {

  @Test(expected = NullPointerException.class)
  public void testConfigDefinitionBeanNull() {
    ConfigDefinitionJson configDefinitionJsonBean =
      new ConfigDefinitionJson(null);
  }

  @Test
  public void testConfigDefinitionBean() {
    com.streamsets.pipeline.config.ModelDefinition modelDefinition = new ModelDefinition(ModelType.COMPLEX_FIELD, null,
      "myClass", null , null, null);

    List<Object> triggeredBy = new ArrayList<>();
    triggeredBy.add("X");
    triggeredBy.add("Y");
    triggeredBy.add("Z");
    com.streamsets.pipeline.config.ConfigDefinition configDefinition =
      new ConfigDefinition("int", ConfigDef.Type.INTEGER, "l2", "d2", "-1", true, "g", "intVar", modelDefinition, "A",
        triggeredBy, 0);

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
    //Shallow compare model definition. Its own unit test will take care of real comparison.
    Assert.assertEquals(configDefinition.getModel(), configDefinitionJsonBean.getModel().getModelDefinition());
    Assert.assertEquals(configDefinition.getTriggeredByValues(), configDefinitionJsonBean.getTriggeredByValues());
    Assert.assertEquals(configDefinition.getType(), configDefinitionJsonBean.getType());
    Assert.assertEquals(configDefinition.isRequired(), configDefinitionJsonBean.isRequired());

  }

  @Test
  public void testConfigDefinitionBeanConstructorWithArgs() {
    com.streamsets.pipeline.config.ModelDefinition modelDefinition = new ModelDefinition(ModelType.COMPLEX_FIELD, null,
      "myClass", null , null, null);
    List<Object> triggeredBy = new ArrayList<>();
    triggeredBy.add("X");
    triggeredBy.add("Y");
    triggeredBy.add("Z");
    com.streamsets.pipeline.config.ConfigDefinition configDefinition =
      new ConfigDefinition("int", ConfigDef.Type.INTEGER, "l2", "d2", "-1", true, "g", "intVar", modelDefinition, "A",
        triggeredBy, 0);

    ModelDefinitionJson modelDefinitionJsonBean =
      new ModelDefinitionJson(modelDefinition);
    ConfigDefinitionJson configDefinitionJsonBean =
      new ConfigDefinitionJson("int", ConfigDef.Type.INTEGER, "l2", "d2", "-1", true,
        "g", "intVar", modelDefinitionJsonBean, "A", triggeredBy, 0);

    Assert.assertEquals(configDefinition.getName(), configDefinitionJsonBean.getName());
    Assert.assertEquals(configDefinition.getDefaultValue(), configDefinitionJsonBean.getDefaultValue());
    Assert.assertEquals(configDefinition.getDependsOn(), configDefinitionJsonBean.getDependsOn());
    Assert.assertEquals(configDefinition.getDescription(), configDefinitionJsonBean.getDescription());
    Assert.assertEquals(configDefinition.getDisplayPosition(), configDefinitionJsonBean.getDisplayPosition());
    Assert.assertEquals(configDefinition.getFieldName(), configDefinitionJsonBean.getFieldName());
    Assert.assertEquals(configDefinition.getGroup(), configDefinitionJsonBean.getGroup());
    Assert.assertEquals(configDefinition.getLabel(), configDefinitionJsonBean.getLabel());
    //Shallow compare model definition. Its own unit test will take care of real comparison.
    Assert.assertEquals(configDefinition.getModel(), configDefinitionJsonBean.getModel().getModelDefinition());
    Assert.assertEquals(configDefinition.getTriggeredByValues(), configDefinitionJsonBean.getTriggeredByValues());
    Assert.assertEquals(configDefinition.getType(), configDefinitionJsonBean.getType());
    Assert.assertEquals(configDefinition.isRequired(), configDefinitionJsonBean.isRequired());

    //compare the underlying com.streamsets.pipeline.config.ConfigDefinition object
    Assert.assertEquals(configDefinition.getName(), configDefinitionJsonBean.getConfigDefinition().getName());
    Assert.assertEquals(configDefinition.getDefaultValue(),
      configDefinitionJsonBean.getConfigDefinition().getDefaultValue());
    Assert.assertEquals(configDefinition.getDependsOn(), configDefinitionJsonBean.getConfigDefinition().getDependsOn());
    Assert.assertEquals(configDefinition.getDescription(), configDefinitionJsonBean.getConfigDefinition().getDescription());
    Assert.assertEquals(configDefinition.getDisplayPosition(),
      configDefinitionJsonBean.getConfigDefinition().getDisplayPosition());
    Assert.assertEquals(configDefinition.getFieldName(), configDefinitionJsonBean.getConfigDefinition().getFieldName());
    Assert.assertEquals(configDefinition.getGroup(), configDefinitionJsonBean.getConfigDefinition().getGroup());
    Assert.assertEquals(configDefinition.getLabel(), configDefinitionJsonBean.getConfigDefinition().getLabel());
    //Shallow compare model definition. Its own unit test will take care of real comparison.
    Assert.assertEquals(configDefinition.getModel(), configDefinitionJsonBean.getConfigDefinition().getModel());
    Assert.assertEquals(configDefinition.getTriggeredByValues(),
      configDefinitionJsonBean.getConfigDefinition().getTriggeredByValues());
    Assert.assertEquals(configDefinition.getType(), configDefinitionJsonBean.getConfigDefinition().getType());
    Assert.assertEquals(configDefinition.isRequired(), configDefinitionJsonBean.getConfigDefinition().isRequired());
  }
}
