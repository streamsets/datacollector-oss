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
package com.streamsets.pipeline.lib.parser.xml;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.xml.StreamingXmlParser;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.testing.ApiUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestXmlCharDataParser {

  public static final String BOOK_XML_NO_NAMESPACE =
      "<bookstore>\n" +
      "  <book>\n" +
      "    <title lang=\"en\">Harry Potter</title>\n" +
      "    <price>29.99</price>\n" +
      "  </book>\n" +
      "  <book>\n" +
      "    <title lang=\"en_us\">Learning XML</title>\n" +
      "    <price>39.95</price>\n" +
      "  </book>\n" +
      "</bookstore>";

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testParse() throws Exception {
    OverrunReader reader = new OverrunReader(
        new StringReader(
            "<r><e>Hello</e><e>Bye</e></r>"), 1000, true, false);
    DataParser parser = new XmlCharDataParser(
        getContext(), "id", reader, 0, "e", false,
        null, 100, true, false);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("Hello", record.get().getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals(18, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::18", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals(29, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testXpathWithoutDelimiterElement() throws Exception {
    OverrunReader reader = new OverrunReader(
        new StringReader(BOOK_XML_NO_NAMESPACE), 1000, true, false);
    DataParser parser = new XmlCharDataParser(
        getContext(), "id", reader, 0, "", true,
        null , 1000, true, false);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);

    List<Field> books = record.get().getValueAsMap().get("book").getValueAsList();

    assertBookRecord(
        "/bookstore/book[0]",
        "Harry Potter",
        "en",
        "29.99",
        books.get(0).getValueAsMap().get("title"),
        books.get(0).getValueAsMap().get("price")
    );

    assertBookRecord(
        "/bookstore/book[1]",
        "Learning XML",
        "en_us",
        "39.95",
        books.get(1).getValueAsMap().get("title"),
        books.get(1).getValueAsMap().get("price")
    );

    parser.close();
  }

  @Test
  public void testXpathWithDelimiterElement() throws Exception {
    //ensure the output xpath remains consistent regardless of record path
    for (String delimiter : Arrays.asList("/bookstore/book", "book", "/*[1]/*")) {
      OverrunReader reader = new OverrunReader(
          new StringReader(BOOK_XML_NO_NAMESPACE), 1000, true, false);
      DataParser parser = new XmlCharDataParser(
          getContext(), "id", reader, 0, delimiter, true,
          null, 1000, true, false);
      Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
      Record record = parser.parse();
      assertBookRecord("/bookstore/book", "Harry Potter", "en", "29.99", record);
      record = parser.parse();
      assertBookRecord("/bookstore/book", "Learning XML", "en_us", "39.95", record);
      parser.close();
    }
  }

  @Test
  public void testOldStyleParserOutput() throws Exception {
    // backwards compatibility for SDC-5407

    for (String delimiter : Arrays.asList("/bookstore/book", "book", "/*[1]/*")) {
      OverrunReader reader = new OverrunReader(
          new StringReader(BOOK_XML_NO_NAMESPACE), 1000, true, false);
      DataParser parser = new XmlCharDataParser(getContext(), "id", reader,
          0, delimiter, true, null,
          1000, false, false);
      Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
      Record record = parser.parse();
      assertBookRecordOldStyle(
          "/bookstore/book",
          "Harry Potter",
          "en",
          "29.99",
          record.get("/title"),
          record.get("/price"),
          "",
          "",
          ""
      );
      record = parser.parse();
      assertBookRecordOldStyle(
          "/bookstore/book",
          "Learning XML",
          "en_us",
          "39.95",
          record.get("/title"),
          record.get("/price"),
          "",
          "",
          ""
      );
      parser.close();
    }
  }

  private static final String NAMESPACE1_URI = "http://namespace1.com";
  private static final String NAMESPACE2_URI = "http://namespace2.com";
  private static final String NAMESPACE3_URI = "http://namespace3.com";

  // this namespace had no prefix in input doc
  private static final String NAMESPACE1_OUTPUT_PREFIX = "ns1";
  // namespace prefix from input doc should be preserved
  private static final String NAMESPACE2_OUTPUT_PREFIX = "books";
  // final namespace also has no prefix in input doc
  private static final String NAMESPACE3_OUTPUT_PREFIX = "ns2";

  @Test
  public void testXpathWithDelimiterElementNamespaced() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
        "<bookstore xmlns=\""+NAMESPACE1_URI+"\" xmlns:"+NAMESPACE2_OUTPUT_PREFIX+"=\""+NAMESPACE2_URI+"\">\n"+
            "  <"+NAMESPACE2_OUTPUT_PREFIX+":book>\n" +
            "    <title xmlns=\"" + NAMESPACE3_URI + "\" lang=\"en\">Harry Potter</title>\n" +
            "    <price xmlns=\"" + NAMESPACE3_URI + "\">29.99</price>\n" +
            "  </"+NAMESPACE2_OUTPUT_PREFIX+":book>\n" +
            "  <"+NAMESPACE2_OUTPUT_PREFIX+":book>\n" +
            "    <title xmlns=\"" + NAMESPACE3_URI + "\" lang=\"en_us\">Learning XML</title>\n" +
            "    <price xmlns=\"" + NAMESPACE3_URI + "\">39.95</price>\n" +
            "  </"+NAMESPACE2_OUTPUT_PREFIX+":book>\n" +
            "</bookstore>"
    ), 1000, true, false);

    final Map<String, String> namespaces = new HashMap<>();
    namespaces.put("bs", NAMESPACE1_URI);
    namespaces.put("b", NAMESPACE2_URI);
    DataParser parser = new XmlCharDataParser(
        getContext(),
        "id",
        reader,
        0,
        "/bs:bookstore/b:book",
        true,
        namespaces,
        1000,
        true,
        false
    );
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));

    Record record = parser.parse();
    assertBookRecord(
        "/ns1:bookstore/"+NAMESPACE2_OUTPUT_PREFIX+":book",
        "Harry Potter",
        "en",
        "29.99",
        record,
        "ns2:",
        "",
        "ns2:"
    );
    assertNamespacedBookRecordHeaders(record);

    record = parser.parse();
    assertBookRecord(
        "/ns1:bookstore/"+NAMESPACE2_OUTPUT_PREFIX+":book",
        "Learning XML",
        "en_us",
        "39.95",
        record,
        "ns2:",
        "",
        "ns2:"
    );
    assertNamespacedBookRecordHeaders(record);

    parser.close();
  }

  @Test
  public void testXpathWithDelimiterElementNonNested() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
        "<root>\n" +
            "  <something attr1=\"attrVal1-1\" attr2=\"attrVal2-1\">1</something>\n" +
            "  <something attr1=\"attrVal1-2\" attr2=\"attrVal2-2\">2</something>\n" +
            "</root>"
    ), 1000, true, false);
    DataParser parser = new XmlCharDataParser(getContext(), "id", reader,
        0, "something", true, null,
        1000, false, false);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();

    Field attr1 = record.get("/"+StreamingXmlParser.ATTR_PREFIX_KEY+"attr1");
    Assert.assertEquals("attrVal1-1", attr1.getValueAsString());
    Assert.assertNotNull(attr1.getAttributes());
    Assert.assertEquals(1, attr1.getAttributes().size());
    Assert.assertEquals("/root/something/@attr1", attr1.getAttribute(StreamingXmlParser.XPATH_KEY));

    Field attr2 = record.get("/"+StreamingXmlParser.ATTR_PREFIX_KEY+"attr2");
    Assert.assertEquals("attrVal2-1", attr2.getValueAsString());
    Assert.assertNotNull(attr2.getAttributes());
    Assert.assertEquals(1, attr2.getAttributes().size());
    Assert.assertEquals("/root/something/@attr2", attr2.getAttribute(StreamingXmlParser.XPATH_KEY));

    Field value = record.get("/"+StreamingXmlParser.VALUE_KEY);
    Assert.assertEquals("1", value.getValueAsString());
    Assert.assertNotNull(value.getAttributes());
    Assert.assertEquals(1, value.getAttributes().size());
    Assert.assertEquals("/root/something", value.getAttribute(StreamingXmlParser.XPATH_KEY));

    record = parser.parse();

    attr1 = record.get("/"+StreamingXmlParser.ATTR_PREFIX_KEY+"attr1");
    Assert.assertEquals("attrVal1-2", attr1.getValueAsString());
    Assert.assertNotNull(attr1.getAttributes());
    Assert.assertEquals(1, attr1.getAttributes().size());
    Assert.assertEquals("/root/something/@attr1", attr1.getAttribute(StreamingXmlParser.XPATH_KEY));

    attr2 = record.get("/"+StreamingXmlParser.ATTR_PREFIX_KEY+"attr2");
    Assert.assertEquals("attrVal2-2", attr2.getValueAsString());
    Assert.assertNotNull(attr2.getAttributes());
    Assert.assertEquals(1, attr2.getAttributes().size());
    Assert.assertEquals("/root/something/@attr2", attr2.getAttribute(StreamingXmlParser.XPATH_KEY));

    value = record.get("/"+StreamingXmlParser.VALUE_KEY);
    Assert.assertEquals("2", value.getValueAsString());
    Assert.assertNotNull(value.getAttributes());
    Assert.assertEquals(1, value.getAttributes().size());
    Assert.assertEquals("/root/something", value.getAttribute(StreamingXmlParser.XPATH_KEY));

    parser.close();
  }

  private static void assertBookRecord(
      String bookXpath,
      String bookTitle,
      String lang,
      String price,
      Record record
  ) {
    Assert.assertNotNull(record);
    assertBookRecord(bookXpath, bookTitle, lang, price, record.get("/title"),
        record.get("/price"), "", "", "");
  }

  private static void assertBookRecord(
      String bookXpath,
      String bookTitle,
      String lang,
      String price,
      Record record,
      String titleXpathPrefix,
      String langXpathPrefix,
      String priceXpathPrefix
  ) {
    Assert.assertNotNull(record);
    assertBookRecord(
        bookXpath,
        bookTitle,
        lang,
        price,
        record.get("/" + titleXpathPrefix + "title"),
        record.get("/" + priceXpathPrefix + "price"),
        titleXpathPrefix,
        langXpathPrefix,
        priceXpathPrefix
    );
  }

  private static void assertBookRecord(
      String bookXpath,
      String bookTitle,
      String bookLang,
      String bookPrice,
      Field title,
      Field price
  ) {
    assertBookRecord(bookXpath, bookTitle, bookLang, bookPrice, title, price, "", "", "");
  }

  private static void assertBookRecord(
      String bookXpath,
      String bookTitle,
      String bookLang,
      String bookPrice,
      Field title,
      Field price,
      String titleXpathPrefix,
      String langXpathPrefix,
      String priceXpathPrefix
  ) {
    Assert.assertNotNull(title);
    Assert.assertNotNull(price);

    Map<String, Field> titleMap = ApiUtils.firstItemAsMap(title);

    Field titleValueField = titleMap.get(StreamingXmlParser.VALUE_KEY);
    Assert.assertEquals(bookTitle, titleValueField.getValueAsString());
    Assert.assertNotNull(titleValueField.getAttributes());
    Assert.assertEquals(1, titleValueField.getAttributes().size());
    String titleXpath = titleValueField.getAttribute(StreamingXmlParser.XPATH_KEY);
    Assert.assertEquals(bookXpath + "/" + titleXpathPrefix + "title", titleXpath);

    Field titleField = title.getValueAsList().get(0);

    String langField = titleField.getAttribute(StreamingXmlParser.XMLATTR_ATTRIBUTE_PREFIX+"lang");
    Assert.assertEquals(bookLang, langField);

    Map<String, Field> priceMap = ApiUtils.firstItemAsMap(price);
    Field priceField = priceMap.get(StreamingXmlParser.VALUE_KEY);
    Assert.assertEquals(bookPrice, priceField.getValueAsString());
    String priceXpath = priceField.getAttribute(StreamingXmlParser.XPATH_KEY);
    Assert.assertEquals(bookXpath + "/" + priceXpathPrefix + "price", priceXpath);

  }

  private static void assertBookRecordOldStyle(
      String bookXpath,
      String bookTitle,
      String bookLang,
      String bookPrice,
      Field title,
      Field price,
      String titleXpathPrefix,
      String langXpathPrefix,
      String priceXpathPrefix
  ) {
    Assert.assertNotNull(title);
    Assert.assertNotNull(price);

    Map<String, Field> titleMap = ApiUtils.firstItemAsMap(title);

    Field titleField = titleMap.get(StreamingXmlParser.VALUE_KEY);
    Assert.assertEquals(bookTitle, titleField.getValueAsString());
    Assert.assertNotNull(titleField.getAttributes());
    Assert.assertEquals(1, titleField.getAttributes().size());
    String titleXpath = titleField.getAttribute(StreamingXmlParser.XPATH_KEY);
    Assert.assertEquals(bookXpath + "/" + titleXpathPrefix + "title", titleXpath);

    Field langField = titleMap.get(StreamingXmlParser.ATTR_PREFIX_KEY+"lang");
    Assert.assertEquals(bookLang, langField.getValueAsString());
    Assert.assertNotNull(langField.getAttributes());
    Assert.assertEquals(1, langField.getAttributes().size());
    String langXpath = langField.getAttribute(StreamingXmlParser.XPATH_KEY);
    Assert.assertEquals(bookXpath + "/"+ titleXpathPrefix + "title/@" + langXpathPrefix + "lang", langXpath);

    Map<String, Field> priceMap = ApiUtils.firstItemAsMap(price);
    Field priceField = priceMap.get(StreamingXmlParser.VALUE_KEY);
    Assert.assertEquals(bookPrice, priceField.getValueAsString());
    String priceXpath = priceField.getAttribute(StreamingXmlParser.XPATH_KEY);
    Assert.assertEquals(bookXpath + "/" + priceXpathPrefix + "price", priceXpath);

  }

  private static void assertNamespacedBookRecordHeaders(Record record) {
    Record.Header header = record.getHeader();
    Assert.assertEquals(
        NAMESPACE1_URI,
        header.getAttribute(XmlCharDataParser.RECORD_ATTRIBUTE_NAMESPACE_PREFIX+NAMESPACE1_OUTPUT_PREFIX)
    );
    Assert.assertEquals(
        NAMESPACE2_URI,
        header.getAttribute(XmlCharDataParser.RECORD_ATTRIBUTE_NAMESPACE_PREFIX+NAMESPACE2_OUTPUT_PREFIX)
    );
    Assert.assertEquals(
        NAMESPACE3_URI,
        header.getAttribute(XmlCharDataParser.RECORD_ATTRIBUTE_NAMESPACE_PREFIX+NAMESPACE3_OUTPUT_PREFIX)
    );
  }

  @Test
  public void testParseWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("<r><e>Hello</e><e>Bye</e></r>"), 1000,
        true, false);
    DataParser parser = new XmlCharDataParser(getContext(), "id", reader, 18, "e",
        false, null, 100, true, false);
    Assert.assertEquals(18, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::18", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals(29, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("<r><e>Hello</e><e>Bye</e></r>"),
        1000, true, false);
    DataParser parser = new XmlCharDataParser(getContext(), "id", reader, 0, "e",
        false, null, 100, true, false);
    parser.close();
    parser.parse();
  }

  @Test
  public void testInvalidXml() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("<r><open-tag>asdf</r>"),
        1000, true, false);
    DataParser parser = new XmlCharDataParser(getContext(), "id", reader, 0, "e",
        false, null, 100, true, false);

    try {
      parser.parse();
      Assert.fail("Parsing should have failed.");
    } catch (DataParserException e) {
      String error = e.toString();
      Assert.assertTrue("Error: " + error, error.contains("XML_PARSER_03"));
      Assert.assertTrue("Error: " + error, error.contains("The element type \"open-tag\" must be terminated by the matching end-tag"));
    } finally {
      parser.close();
    }
  }

}
