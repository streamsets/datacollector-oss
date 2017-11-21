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
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.StringBuilderPoolFactory;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

@RunWith(Parameterized.class)
public class TestLEEFParser {

  private String logLine;
  private int logLineLength;
  private double leefVersion;
  private String expectedAct;

  public TestLEEFParser(String testName, int logLineIndex, int logLineLength, double leefVersion, String expectedAct) {
    this.logLine = LOG_LINES[logLineIndex];
    this.logLineLength = logLineLength;
    this.leefVersion = leefVersion;
    this.expectedAct = expectedAct;
  }

  private static final String LOG_LINE = "LEEF:1.0|Trend Micro|Deep Discovery Inspector|3.8.1175|SECURITY_RISK_DETECTION|" +
      "devTimeFormat=MMM dd yyyy HH:mm:ss z\t ptype=IDS\tdvc=10.201.156.143\tdeviceMacAddress=00:0C: 29:A6:53:0C" +
      "\tdvchost=ddi38-143\tdeviceGUID=6B593E17AFB 7-40FBBB28-A4CE-0462-A536\tdevTime=Mar 09 2015 11:58:24 G MT+08:00" +
      "\tsev=6\tprotoGroup=HTTP\tproto=HTTP\tvL ANId=4095\tdeviceDirection=1\tdhost=www.freewebs.com" +
      "\tdst=216.52.115.2\tdstPort=80\tdstMAC=00:1b:21:35:8b :98\tshost=172.16.1.197\tsrc=172.16.1.197" +
      "\tsrcPort= 12121\tsrcMAC=fe:ed:be:ef:5a:c6\tmalType=MALWARE\ts AttackPhase=Point of Entry\tfname=setting.doc\tfileTyp e=0" +
      "\tfsize=0\truleId=20\tmsg=Malware URL requested - Type 1\tdeviceRiskConfidenceLevel=2" +
      "\turl=http://www.freewebs.com/setting3/setting.doc\tpComp=CAV\triskType =1\tsrcGroup=Default\tsrcZone=1\tdstZone=0" +
      "\tdete ctionType=1\tact=not blocked\tthreatType=1\tinteres tedIp=172.16.1.197\tpeerIp=216.52.115.2\thostName=www. freewebs.com" +
      "\tcnt=1\taggregatedCnt=1\tcccaDestinati onFormat=URL\tcccaDetectionSource=GLOBAL_INTELLIGENCE<009 >cccaRiskLevel=2" +
      "\tcccaDestination=http://www.freewebs.com /setting3/setting.doc\tcccaDetection=1\tevtCat=Callbac k evtSubCat=Bot" +
      "\tpAttackPhase=Command and Control Communi cation";

  private static final String LEEF_2_LOG_LINE = "LEEF:2.0|Trend Micro|Deep Security Agent|<DSA version>|4000030|cat=Anti-Malware\tname=HEU_AEGIS_CRYPT" +
      "\tdesc=HEU_AEGIS_CRYPT\tsev=6\tcn1=241\tcn1Label=Host\tID dvc=10.0.0.1\tTrendMicroDsTags=FS\tTrendMicroDsTenant=Primary\tTrendMicroDsTenantId=0\t" +
      "filePath=C:\\\\Windows\\\\System32\\\\virus.exe\tact=Terminate\tmsg=Realtime\tTrendMicroDsMalwareTarget=Multiple\t" +
      "TrendMicroDsMalwareTargetType=File System";

  private static final String LEEF_2_CARAT_LINE = "LEEF:2.0|Trend Micro|Deep Security Agent|<DSA version>|4000030|^|cat=Anti-Malware^name=HEU_AEGIS_CRYPT" +
      "^desc=HEU_AEGIS_CRYPT^sev=6^cn1=241^cn1Label=Host^ID dvc=10.0.0.1^TrendMicroDsTags=FS^TrendMicroDsTenant=Primary^TrendMicroDsTenantId=0^" +
      "filePath=C:\\\\Windows\\\\System32\\\\virus.exe^act=Terminate^msg=Realtime^TrendMicroDsMalwareTarget=Multiple^" +
      "TrendMicroDsMalwareTargetType=File System";

  private static final String LEEF_2_HEX_DELIM_LINE = "LEEF:2.0|Trend Micro|Deep Security Agent|<DSA version>|4000030|x08|cat=Anti-Malware\bname=HEU_AEGIS_CRYPT" +
      "\bdesc=HEU_AEGIS_CRYPT\bsev=6\bcn1=241\bcn1Label=Host\bID dvc=10.0.0.1\bTrendMicroDsTags=FS\bTrendMicroDsTenant=Primary\bTrendMicroDsTenantId=0\b" +
      "filePath=C:\\\\Windows\\\\System32\\\\virus.exe\bact=Terminate\bmsg=Realtime\bTrendMicroDsMalwareTarget=Multiple\b" +
      "TrendMicroDsMalwareTargetType=File System";

  private static final String[] LOG_LINES = {
      LOG_LINE,
      LEEF_2_LOG_LINE,
      LEEF_2_CARAT_LINE,
      LEEF_2_HEX_DELIM_LINE
  };

  @Parameterized.Parameters(name = "str({0})")
  public static Collection data() throws Exception {
    return Arrays.asList(new Object[][] {
            { "LEEF 1.0", 0, LOG_LINE.length(), 1.0, "not blocked" },
            { "LEEF 2.0", 1, LEEF_2_LOG_LINE.length(), 2.0, "Terminate" },
            { "LEEF 2.0 with carat delimiter", 2, LEEF_2_CARAT_LINE.length(), 2.0, "Terminate" },
            { "LEEF 2.0 with hex delimiter", 3, LEEF_2_HEX_DELIM_LINE.length(), 2.0, "Terminate" }
        }
    );
  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  @Test
  public void testParse() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(logLine), 10000, true, false);
    DataParser parser = new LEEFParser(getContext(), "id", reader, 0, 10000, true,
        getStringBuilderPool(), getStringBuilderPool());
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals(logLine, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(logLineLength, Long.parseLong(parser.getOffset()));

    Assert.assertTrue(record.has("/leefVersion"));
    Assert.assertEquals(leefVersion, record.get("/leefVersion").getValueAsInteger(), 0);

    Assert.assertTrue(record.has("/extensions/act"));
    Assert.assertEquals(expectedAct, record.get("/extensions/act").getValueAsString());


    parser.close();
  }

  @Test(expected = DataParserException.class)
  public void testParseNonLogLine() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] This is a log line that does not confirm to common log format"),
      1000, true, false);
    DataParser parser = new LEEFParser(getContext(), "id", reader, 0, 1000, true,
        getStringBuilderPool(), getStringBuilderPool());
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    try {
      parser.parse();
    } finally {
      parser.close();
    }
  }

  private GenericObjectPool<StringBuilder> getStringBuilderPool() {
    GenericObjectPoolConfig stringBuilderPoolConfig = new GenericObjectPoolConfig();
    stringBuilderPoolConfig.setMaxTotal(1);
    stringBuilderPoolConfig.setMinIdle(1);
    stringBuilderPoolConfig.setMaxIdle(1);
    stringBuilderPoolConfig.setBlockWhenExhausted(false);
    return new GenericObjectPool<>(new StringBuilderPoolFactory(1024), stringBuilderPoolConfig);
  }
}
