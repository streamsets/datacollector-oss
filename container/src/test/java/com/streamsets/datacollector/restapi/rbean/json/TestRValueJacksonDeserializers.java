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
package com.streamsets.datacollector.restapi.rbean.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.rbean.lang.NotificationMsg;
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
import com.streamsets.datacollector.restapi.rbean.lang.RValue;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

public class TestRValueJacksonDeserializers {

  @Test
  public void testSerializer() throws IOException {
    ObjectMapper mapper = ObjectMapperFactory.get();
    RString r = new RString("hello").addMessage(new NotificationMsg(TestRBeanSerializationDeserialization.Errors.X));
    String json = mapper.writeValueAsString(r);
    String value = mapper.readValue(json, String.class);
    Assert.assertEquals("hello",value);

    r = new RString("hello").setScrubbed(true).addMessage(new NotificationMsg(TestRBeanSerializationDeserialization.Errors.X));
    json = mapper.writeValueAsString(r);
    List list = mapper.readValue(json, List.class);
    Assert.assertEquals(RValueJacksonSerializer.SCRUBBED, list);
  }

  @Test
  public void testDeserializer() throws IOException {
    ObjectMapper mapper = ObjectMapperFactory.get();

    // the easiest way to test the deserializer is to make it deserialize.

    // not testing enum here because we need a Bean REnum<E> property
    RValue[] values = {
        new RBoolean().setValue(true),
        new RChar().setValue('c'),
        new RDate().setValue(1L),
        new RDatetime().setValue(2L),
        new RDecimal().setValue(new BigDecimal("1.1")),
        new RDouble().setValue(2.2d),
        new RLong().setValue(3L),
        new RString().setValue("string"),
        new RText().setValue("text"),
        new RTime().setValue(4L),
        new RBoolean().setValue(null),
        new RChar().setValue(null),
        new RDate().setValue(null),
        new RDatetime().setValue(null),
        new RDecimal().setValue(null),
        new RDouble().setValue(null),
        new RLong().setValue(null),
        new RString().setValue(null),
        new RText().setValue(null),
        new RTime().setValue(null)
    };
    for (RValue value : values) {
      String json = mapper.writeValueAsString(value);
      RValue got = mapper.readValue(json, value.getClass());
      Assert.assertEquals(value, got);
    }
  }

  public static class MyBaseRBean<C extends MyBaseRBean<C>> extends RBean<C> {
    private RString id = new RString();

    public enum BE {
      BASEA,
      BASEB
    }

    private REnum<MyBaseRBean.BE> be;


    @Override
    public RString getId() {
      return id;
    }

    public void setId(RString id) {
      this.id = id;
    }

    public REnum<BE> getBe() {
      return be;
    }

    public void setBe(REnum<BE> be) {
      this.be = be;
    }
  }

  public static class MyRBean extends MyBaseRBean<MyRBean> {
    public enum E {
      A,
      B
    }
    private REnum<E> e;


    public REnum<E> getE() {
      return e;
    }

    public void setE(REnum<E> e) {
      this.e = e;
    }
  }

  @Test
  public void testEnumDeser() throws Exception {
    ObjectMapper mapper = ObjectMapperFactory.get();

    MyRBean bean = new MyRBean();
    bean.setId(new RString().setValue("id"));
    bean.setBe(new REnum<MyBaseRBean.BE>().setValue(MyBaseRBean.BE.BASEA));
    bean.setE(new REnum<MyRBean.E>().setValue(MyRBean.E.A));
    String json = mapper.writeValueAsString(bean);
    MyRBean got = mapper.readValue(json, MyRBean.class);
    Assert.assertEquals(bean.getId(), got.getId());
    Assert.assertEquals(bean.getBe(), got.getBe());
    Assert.assertEquals(bean.getE(), got.getE());

    bean = new MyRBean();
    json = mapper.writeValueAsString(bean);
    got = mapper.readValue(json, MyRBean.class);
    Assert.assertEquals(new RString(), got.getId());
    Assert.assertEquals(new REnum<>(), got.getBe());
    Assert.assertEquals(new REnum<>(), got.getE());
  }

}
