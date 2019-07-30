/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.mapr.v5_2.loader;


import com.mapr.db.MapRDB;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.destination.mapr.loader.MapRJsonDocumentLoader;
import com.streamsets.testing.SingleForkNoReuseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.ojai.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;


@Category(SingleForkNoReuseTest.class)
public class TestMapRJson5_2DocumentLoader {

  @Test
  public void testRecordToMaprDocumentGenerator()
      throws IOException {

    MapRJsonDocumentLoader loader = new MapRJson5_2DocumentLoader();
    MapRJsonDocumentLoader.RecordToMapRJsonDocumentGenerator parser = new MapRJsonDocumentLoader.RecordToMapRJsonDocumentGenerator(loader);

    Record rec = RecordCreator.create("s", "s:1");
    Map<String, Field> r = new LinkedHashMap<>();
    r.put("f00", Field.create((float)3.14));
    r.put("f01", Field.create((double)6.28));
    r.put("f02", Field.create(true));
    r.put("f03", Field.create('a'));
    r.put("f04", Field.create((byte) 0x41));
    r.put("f05", Field.create(22));
    r.put("f06", Field.create(27L));
    r.put("f07", Field.create((short)32));
    r.put("f08", Field.create("helloWorld"));

    Date date = new Date(1564663787423L);
    r.put("f09", Field.createDate(date));
    r.put("f10", Field.createDatetime(date));


    Map<String, Field> r2 = new LinkedHashMap<>();
    r2.put("f110", Field.create((float)3.145));
    r2.put("f111", Field.create("helloWorld2"));
    r2.put("f112", Field.create((byte) 0x44));
    ArrayList<Field> list = new ArrayList<Field>();
    list.add(Field.create("Test"));
    list.add(Field.create(123.3));
    list.add(Field.create(11111L));
    list.add(Field.create((String)null));
    list.add(Field.create("byte array example".getBytes()));
    r2.put("f113", Field.create(list));

    r.put("f11",Field.create(r2));
    r.put("f12",Field.create("byte array example".getBytes()));

    rec.set(Field.create(r));

    Document d = MapRDB.newDocument();

    parser.writeRecordToDocument(d,rec);
    String expectedResult = "{\"f00\":3.140000104904175,\"f01\":6.28,\"f02\":true,\"f03\":\"a\",\"f04\":\"QQ==\"," +
        "\"f05\":22,\"f06\":27,\"f07\":32,\"f08\":\"helloWorld\",\"f09\":1564663787423,\"f10\":1564663787423," +
        "\"f11\":{\"f110\":3.1449999809265137,\"f111\":\"helloWorld2\",\"f112\":\"RA==\",\"f113\":[\"Test\"," +
        "123.3,11111,null,[98,121,116,101,32,97,114,114,97,121,32,101,120,97,109,112,108,101]]}," +
        "\"f12\":\"Ynl0ZSBhcnJheSBleGFtcGxl\"}";
    String obtainedResult = d.toString();
    Assert.assertEquals(obtainedResult,expectedResult);

  }
}
