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
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.el.ElFunctionDefinition;
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
    com.streamsets.pipeline.config.ModelDefinition modelDefinition = new ModelDefinition(ModelType.COMPLEX_FIELD,
                                                                                         "myClass", null , null, null,
                                                                                         null);

    List<Object> triggeredBy = new ArrayList<>();
    triggeredBy.add("X");
    triggeredBy.add("Y");
    triggeredBy.add("Z");
    com.streamsets.pipeline.config.ConfigDefinition configDefinition =
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
