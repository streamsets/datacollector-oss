/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.record;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;

import java.util.Iterator;
import java.util.Locale;

public class RecordImpl implements Record {
  private final Record previous;
  private final byte[] raw;
  private final String rawMime;
  private final SimpleMap<String, String> headerData;
  private final Header header;
  private final SimpleMap<String, Field> fieldData;

  public class HeaderImpl implements Header {
    private final ReservedPrefixSimpleMap<String> headerUserFacing;

    public HeaderImpl(SimpleMap<String, String> headers) {
      headerUserFacing = new ReservedPrefixSimpleMap<String>("_.", headers);
    }

    @Override
    public byte[] getRaw() {
      return raw.clone();
    }

    @Override
    public String getRawMime() {
      return rawMime;
    }

    @Override
    public Iterator<String> getAttributeNames() {
      return headerUserFacing.getKeys().iterator();
    }

    @Override
    public String getAttribute(String name) {
      return headerUserFacing.get(name);
    }

    @Override
    public void setAttribute(String name, String value) {
      headerUserFacing.put(name, value);
    }

    @Override
    public void removeAttribute(String name) {
      headerUserFacing.remove(name);
    }

  }

  public RecordImpl(byte[] raw, String rawMime, String module, String source) {
    Preconditions.checkNotNull(module, "module cannot be null");
    Preconditions.checkNotNull(source, "source cannot be null");
    previous = null;
    this.raw = raw.clone();
    this.rawMime = rawMime;
    headerData = new VersionedSimpleMap<String, String>();
    headerData.put(MODULE, module);
    headerData.put(SOURCE, source);
    headerData.put(PROCESSING_PATH, module);
    fieldData = new VersionedSimpleMap<String, Field>();
    header = new HeaderImpl(headerData);
  }

  public RecordImpl(RecordImpl record, String module) {
    Preconditions.checkNotNull(record, "record cannot be null");
    previous = record;
    raw = record.raw;
    rawMime = record.rawMime;
    headerData = new VersionedSimpleMap<String, String>(record.headerData);
    headerData.put(MODULE, module);
    headerData.put(PROCESSING_PATH, headerData.get(PROCESSING_PATH) + ":" + module);
    fieldData = new VersionedSimpleMap<String, Field>(record.fieldData);
    header = new HeaderImpl(headerData);
  }

  @Override
  public Record getPrevious() {
    return previous;
  }

  @Override
  public Header getHeader() {
    return header;
  }

  @Override
  public Iterator<String> getFieldNames() {
    return fieldData.getKeys().iterator();
  }

  @Override
  public Field getField(String name) {
    return fieldData.get(name);
  }

  @Override
  public void setField(String name, Field field) {
    fieldData.put(name, field);
  }

  @Override
  public void deleteField(String name) {
    fieldData.remove(name);
  }

  @Override
  public String toString(Locale locale) {
    return null;
  }
}
