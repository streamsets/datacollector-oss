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
package com.streamsets.pipeline.lib.sampling;

import com.codahale.metrics.Timer;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldVisitor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.Sampler;
import com.streamsets.pipeline.lib.util.SdcRecordConstants;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("deprecation")
public class TestRecordSampler {

  private static List<Record> records = new ArrayList<>();

  @BeforeClass
  public static void beforeClass() {
    for (int i = 0; i < 500; i++) {
      records.add(new Record() {

        private Header header = new Header() {

          private Map<String ,String> map = new HashMap<>();

          @Override
          public String getStageCreator() {
            return null;
          }

          @Override
          public String getSourceId() {
            return null;
          }

          @Override
          public String getTrackingId() {
            return null;
          }

          @Override
          public String getPreviousTrackingId() {
            return null;
          }

          @Override
          public String getStagesPath() {
            return null;
          }

          @Override
          public byte[] getRaw() {
            return new byte[0];
          }

          @Override
          public String getRawMimeType() {
            return null;
          }

          @Override
          public Set<String> getAttributeNames() {
            return map.keySet();
          }

          @Override
          public String getAttribute(String name) {
            return map.get(name);
          }

          @Override
          public void setAttribute(String name, String value) {
            map.put(name, value);
          }

          @Override
          public void deleteAttribute(String name) {

          }

          @Override
          public String getErrorDataCollectorId() {
            return null;
          }

          @Override
          public String getErrorPipelineName() {
            return null;
          }

          @Override
          public String getErrorCode() {
            return null;
          }

          @Override
          public String getErrorMessage() {
            return null;
          }

          @Override
          public String getErrorStage() {
            return null;
          }

          @Override
          public String getErrorStageLabel() {
            return null;
          }

          @Override
          public long getErrorTimestamp() {
            return 0;
          }

          @Override
          public String getErrorStackTrace() {
            return null;
          }

          @Override
          public String getErrorJobId() {
            return null;
          }

          @Override
          public String getErrorJobName() {
            return null;
          }

          /** To be removed */
          public Map<String, Object> getAllAttributes() {
            return null;
          }

          /** To be removed */
          public Map<String, Object> overrideUserAndSystemAttributes(Map<String, Object> newAttrs) {
            return null;
          }

          /** To be removed */
          public Map<String, Object> getUserAttributes() {return null;}

          /** To be removed */
          public Map<String, Object> setUserAttributes(Map<String, Object> newAttributes) {return null;}

        };

        @Override
        public Header getHeader() {
          return header;
        }

        @Override
        public Field get() {
          return null;
        }

        @Override
        public Field set(Field field) {
          return null;
        }

        @Override
        public Field get(String fieldPath) {
          return null;
        }

        @Override
        public Field delete(String fieldPath) {
          return null;
        }

        @Override
        public boolean has(String fieldPath) {
          return false;
        }

        @Override
        public Set<String> getFieldPaths() {
          return null;
        }

        @Override
        public Set<String> getEscapedFieldPaths() {
          return null;
        }

        @Override
        public List<String> getEscapedFieldPathsOrdered() {
          return null;
        }

        @Override
        public Field set(String fieldPath, Field newField) {
          return null;
        }

        @Override
        public void forEachField(FieldVisitor visitor) throws StageException {
        }

      });
    }
  }

  @Test
  public void testSampledRecordHeaderDest() {

    Sampler sampler = new RecordSampler(null, false, 5, 5);
    long startTime = System.currentTimeMillis();
    Record record = records.get(0);
    boolean sampled = sampler.sample(record);
    long endTime = System.currentTimeMillis();

    Assert.assertEquals(true, sampled);
    Assert.assertEquals( SdcRecordConstants.TRUE, record.getHeader().getAttribute(SdcRecordConstants.SDC_SAMPLED_RECORD));

    long timeStamp = Long.parseLong(record.getHeader().getAttribute(SdcRecordConstants.SDC_SAMPLED_TIME));
    Assert.assertTrue(timeStamp >= startTime && timeStamp <= endTime);

  }

  @Test
  public void testSampledRecordHeaderOrig() {

    Stage.Context mock = Mockito.mock(Stage.Context.class);
    Sampler sampler = new RecordSampler(mock, false, 20, 20);
    Record record = records.get(0);
    // sample in destination
    boolean sampled = sampler.sample(record);
    Assert.assertEquals(true, sampled);

    Mockito.verify(mock, Mockito.times(0)).getTimer(Mockito.anyString());
    Mockito.verify(mock, Mockito.times(0)).createTimer(Mockito.anyString());

    Timer mockTimer = Mockito.mock(Timer.class);
    Mockito.when(mock.createTimer(Mockito.anyString())).thenReturn(mockTimer);
    Mockito.doNothing().when(mockTimer).update(Mockito.anyLong(), Mockito.any(TimeUnit.class));

    // sample in origin
    sampler = new RecordSampler(mock, true, 20, 20);
    sampler.sample(record);

    Mockito.verify(mock, Mockito.times(1)).getTimer(Mockito.anyString());
    Mockito.verify(mock, Mockito.times(1)).createTimer(Mockito.anyString());

  }
}
