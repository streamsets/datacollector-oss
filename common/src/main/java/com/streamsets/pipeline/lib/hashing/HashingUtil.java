/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.hashing;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.FieldRegexUtil;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * This is a refactored code for hasing using Guavas Library which is currently used by FieldHasher
 * and DedupProcessor.
 */
public final class HashingUtil {

  public static HashFunction getHasher(String hashAlgorithm) {
    switch(hashAlgorithm) {
      case "murmur3_128":
        return Hashing.murmur3_128();
      case "MD5":
        return Hashing.md5();
      case "SHA-1":
        return Hashing.sha1();
      case "SHA-256":
        return Hashing.sha256();
      default:
        throw new IllegalArgumentException(Utils.format("Unsupported Hashing Algorithm: {}", hashAlgorithm));
    }
  }

  public static RecordFunnel getRecordFunnel(Collection<String> fieldsToHash) {
    if(fieldsToHash == null || fieldsToHash.isEmpty()) {
      return new RecordFunnel();
    }
    return new RecordFunnel(fieldsToHash, false);
  }

  public static RecordFunnel getRecordFunnel(
      Collection<String> fieldsToHash,
      boolean includeRecordHeader) {
    if(fieldsToHash == null || fieldsToHash.isEmpty()) {
      return new RecordFunnel();
    }
    return new RecordFunnel(fieldsToHash, includeRecordHeader);
  }

  public static class RecordFunnel implements Funnel<Record> {
    private Collection<String> fieldsToHash = null;
    private boolean includeRecordHeader = false;

    public RecordFunnel() {
    }

    public RecordFunnel(Collection<String> fieldsToHash, boolean includeRecordHeader) {
      this.fieldsToHash = fieldsToHash;
      this.includeRecordHeader = includeRecordHeader;
    }

    protected List<String> getFieldsToHash(Record record) {
      Set<String> fieldPaths = record.getEscapedFieldPaths();
      List<String> fields = new ArrayList<>();
      if (fieldsToHash != null) {
        for(String field : fieldsToHash) {
          List<String> matchingFieldPaths = FieldRegexUtil.getMatchingFieldPaths(field, fieldPaths);
          Collections.sort(matchingFieldPaths);
          fields.addAll(matchingFieldPaths);
        }
      } else {
        fields = new ArrayList<>(record.getEscapedFieldPaths());
        Collections.sort(fields);
      }
      return fields;
    }

    @Override
    public void funnel(Record record, PrimitiveSink sink) {
      for (String path : getFieldsToHash(record)) {
        Field field = record.get(path);
        if (field.getValue() != null) {
          switch (field.getType()) {
            case BOOLEAN:
              sink.putBoolean(field.getValueAsBoolean());
              break;
            case CHAR:
              sink.putChar(field.getValueAsChar());
              break;
            case BYTE:
              sink.putByte(field.getValueAsByte());
              break;
            case SHORT:
              sink.putShort(field.getValueAsShort());
              break;
            case INTEGER:
              sink.putInt(field.getValueAsInteger());
              break;
            case LONG:
              sink.putLong(field.getValueAsLong());
              break;
            case FLOAT:
              sink.putFloat(field.getValueAsFloat());
              break;
            case DOUBLE:
              sink.putDouble(field.getValueAsDouble());
              break;
            case DATE:
              sink.putLong(field.getValueAsDate().getTime());
              break;
            case DATETIME:
              sink.putLong(field.getValueAsDatetime().getTime());
              break;
            case DECIMAL:
              sink.putString(field.getValueAsString(), Charset.defaultCharset());
              break;
            case STRING:
              sink.putString(field.getValueAsString(), Charset.defaultCharset());
              break;
            case BYTE_ARRAY:
              sink.putBytes(field.getValueAsByteArray());
              break;
            case MAP:
            case LIST:
          }
        } else {
          sink.putBoolean(true);
        }
        sink.putByte((byte)0);
      }

      if (this.includeRecordHeader) {
        for (String attrName : record.getHeader().getAttributeNames()) {
          String headerAttr = record.getHeader().getAttribute(attrName);
          if (headerAttr != null) {
            sink.putString(headerAttr, Charset.defaultCharset());
          } else {
            sink.putBoolean(true);
          }
          sink.putByte((byte)0);
        }
      }
    }
  }
}
