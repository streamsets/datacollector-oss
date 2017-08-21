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
package com.streamsets.pipeline.lib.generator.xml;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Collections;

public class TestXmlDataGeneratorFactory {

  @Test
  public void testConstructor() {
    DataFactory.Settings settings = Mockito.mock(DataFactory.Settings.class);
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.SCHEMA_VALIDATION))).thenReturn(true);
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.SCHEMAS))).thenReturn(Collections.emptyList());
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.PRETTY_FORMAT))).thenReturn(false);

    XmlDataGeneratorFactory factory = new XmlDataGeneratorFactory(settings);
    Assert.assertEquals(true, factory.isSchemaValidation());
    Assert.assertEquals(Collections.emptyList(), factory.getSchemas());
    Assert.assertEquals(false, factory.isPrettyFormat());

    settings = Mockito.mock(DataFactory.Settings.class);
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.SCHEMA_VALIDATION))).thenReturn(false);
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.SCHEMAS))).thenReturn(ImmutableList.of(""));
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.PRETTY_FORMAT))).thenReturn(true);

    factory = new XmlDataGeneratorFactory(settings);
    Assert.assertEquals(false, factory.isSchemaValidation());
    Assert.assertEquals(1, factory.getSchemas().size());
    Assert.assertEquals(true, factory.isPrettyFormat());
  }

  @Test
  public void testGeneratorNoSchemaValidationr() throws IOException {
    DataFactory.Settings settings = Mockito.mock(DataFactory.Settings.class);
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.SCHEMA_VALIDATION))).thenReturn(false);
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.SCHEMAS))).thenReturn(Collections.emptyList());
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.PRETTY_FORMAT))).thenReturn(true);
    Mockito.when(settings.getCharset()).thenReturn(Charset.forName("UTF-8"));
    XmlDataGeneratorFactory factory = new XmlDataGeneratorFactory(settings);

    OutputStream os = Mockito.mock(OutputStream.class);

    DataGenerator generator = factory.getGenerator(os);
    Assert.assertNotNull(generator);
    Assert.assertTrue(generator instanceof XmlCharDataGenerator);

    XmlCharDataGenerator xmlGenerator = (XmlCharDataGenerator) generator;
    Assert.assertEquals(false, xmlGenerator.isSchemaValidation());
    Assert.assertNull(xmlGenerator.getSchema());
    Assert.assertEquals(true, xmlGenerator.isPrettyFormat());
  }

  private static final String SCHEMA = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                                       "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">\n" +
                                       "\t<xs:element name=\"root\" type=\"xs:string\"/>\n" +
                                       "</xs:schema>";

  @Test
  public void testGeneratorSchemaValidation() throws IOException {
    DataFactory.Settings settings = Mockito.mock(DataFactory.Settings.class);
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.SCHEMA_VALIDATION))).thenReturn(true);
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.SCHEMAS))).thenReturn(ImmutableList.of(SCHEMA));
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.PRETTY_FORMAT))).thenReturn(false);
    Mockito.when(settings.getCharset()).thenReturn(Charset.forName("UTF-8"));
    XmlDataGeneratorFactory factory = new XmlDataGeneratorFactory(settings);

    OutputStream os = Mockito.mock(OutputStream.class);

    DataGenerator generator = factory.getGenerator(os);
    Assert.assertNotNull(generator);
    Assert.assertTrue(generator instanceof XmlCharDataGenerator);

    XmlCharDataGenerator xmlGenerator = (XmlCharDataGenerator) generator;
    Assert.assertEquals(true, xmlGenerator.isSchemaValidation());
    Assert.assertNotNull(xmlGenerator.getSchema());
    Assert.assertEquals(false, xmlGenerator.isPrettyFormat());
  }

  @Test
  public void testAccessViaDataGeneratorFormat() throws IOException {
    DataFactory.Settings settings = Mockito.mock(DataFactory.Settings.class);
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.SCHEMA_VALIDATION))).thenReturn(true);
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.SCHEMAS))).thenReturn(ImmutableList.of(SCHEMA));
    Mockito.when(settings.getConfig(Mockito.eq(XmlDataGeneratorFactory.PRETTY_FORMAT))).thenReturn(false);
    Mockito.when(settings.getCharset()).thenReturn(Charset.forName("UTF-8"));
    DataGeneratorFactory factory = DataGeneratorFormat.XML.create(settings);
    Assert.assertNotNull(factory);
    Assert.assertTrue(factory instanceof XmlDataGeneratorFactory);
  }

}
