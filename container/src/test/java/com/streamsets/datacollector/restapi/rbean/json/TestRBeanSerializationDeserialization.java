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
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.rbean.lang.NotificationMsg;
import com.streamsets.datacollector.restapi.rbean.lang.RBean;
import com.streamsets.datacollector.restapi.rbean.lang.RDate;
import com.streamsets.datacollector.restapi.rbean.lang.REnum;
import com.streamsets.datacollector.restapi.rbean.lang.RString;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.impl.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestRBeanSerializationDeserialization {

  public enum Foo {
    BAR
  }

  public static class RBean2 extends RBean<RBean2> {

    private RString id = new RString();
    private REnum<Foo> foo;
    private RDate date;

    @Override
    public RString getId() {
      return id;
    }

    public void setId(RString id) {
      this.id = id;
    }

    public REnum<Foo> getFoo() {
      return foo;
    }

    public RBean2 setFoo(REnum<Foo> foo) {
      this.foo = foo;
      return this;
    }

    public RDate getDate() {
      return date;
    }

    public RBean2 setDate(RDate date) {
      this.date = date;
      return this;
    }

  }

  public static class RBean1 extends RBean<RBean1> {

    private RString id = new RString();
    private REnum<Foo> foo;
    private RString str;
    private List<RString> listStr = new ArrayList<>();
    private RBean2 subBean = new RBean2();

    @Override
    public RString getId() {
      return id;
    }

    public void setId(RString id) {
      this.id = id;
    }

    public REnum<Foo> getFoo() {
      return foo;
    }

    public RBean1 setFoo(REnum<Foo> foo) {
      this.foo = foo;
      return this;
    }

    public RString getStr() {
      return str;
    }

    public RBean1 setStr(RString str) {
      this.str = str;
      return this;
    }

    public List<RString> getListStr() {
      return listStr;
    }

    public RBean1 setListStr(List<RString> listStr) {
      this.listStr = Utils.checkNotNull(listStr, "listStr");
      return this;
    }

    public RBean2 getSubBean() {
      return subBean;
    }

    public RBean1 setSubBean(RBean2 subBean) {
      this.subBean = subBean;
      return this;
    }

  }

  enum Errors implements ErrorCode {
    X, Y, W, Z;

    @Override
    public String getCode() {
      return name();
    }

    @Override
    public String getMessage() {
      return name() + name();
    }
  }

  @Test
  public void testSerializationDeserialization() throws IOException {
    ObjectMapper mapper = ObjectMapperFactory.get();

    RBean1 bean = new RBean1();
    bean.setId(new RString().setValue("id"));
    bean.setFoo(new REnum<Foo>().setValue(Foo.BAR)).setStr(new RString().setValue("x"));
    bean.setListStr(ImmutableList.of(
        new RString().setValue("grrh").addMessage(new NotificationMsg(Errors.X)),
        new RString().setValue("grrh").addMessage(new NotificationMsg(Errors.Y))
    ));
    bean.setReadOnlySignature("signature");

    RBean2 subBean = new RBean2();
    subBean.setDate(new RDate().setValue(1L))
        .setFoo(new REnum<Foo>().setValue(Foo.BAR))
        .setId(new RString().setValue("sid"));
    subBean.setReadOnlySignature("subBean.signature");
    bean.setSubBean(subBean);

    String json = mapper.writeValueAsString(bean);
    RBean1 got = mapper.readValue(json, RBean1.class);

    Assert.assertEquals(bean.getId(), got.getId());
    Assert.assertEquals(bean.getFoo(), got.getFoo());
    Assert.assertEquals(bean.getStr(), got.getStr());
    Assert.assertEquals(bean.getListStr(), got.getListStr());
    Assert.assertEquals(bean.getSubBean().getId(), got.getSubBean().getId());
    Assert.assertEquals(bean.getSubBean().getDate(), got.getSubBean().getDate());
    Assert.assertEquals(bean.getSubBean().getFoo(), got.getSubBean().getFoo());
  }

  @Test
  public void testSerializationMessages() throws IOException {
    ObjectMapper mapper = ObjectMapperFactory.get();

    RBean1 bean = new RBean1();
    bean.setId(new RString().setValue("id").addMessage(new NotificationMsg(Errors.X)).setAlt("ID"));
    bean.setListStr(ImmutableList.of(
        new RString().setValue("grrh").addMessage(new NotificationMsg(Errors.Y)),
        new RString().setValue("grrh").addMessage(new NotificationMsg(Errors.W))
    ));

    RBean2 subBean = new RBean2();
    subBean.setDate(new RDate().setValue(1L).addMessage(new NotificationMsg(Errors.Z)));

    bean.setSubBean(subBean);

    String json = mapper.writeValueAsString(bean);
    Map map = mapper.readValue(json, Map.class);
    Assert.assertNotNull(map.get("#messages#id"));
    Assert.assertNotNull(map.get("#alt#id"));
    Assert.assertNotNull(map.get("#messages#listStr[0]"));
    Assert.assertNotNull(map.get("#messages#listStr[1]"));
    Assert.assertNotNull(map.get("subBean"));
    Assert.assertNotNull(((Map)map.get("subBean")).get("#messages#date"));
  }

}
