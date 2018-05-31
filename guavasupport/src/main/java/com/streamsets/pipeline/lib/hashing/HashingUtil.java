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
 * This is a refactored code for hashing using Guavas Library which is currently used by FieldHasher
 * and DedupProcessor.
 */
public final class HashingUtil {

  public enum HashType {
    MURMUR3_128,
    MURMUR3_32,
    SIPHASH24,
    MD5,
    SHA1,
    SHA256,
    SHA512,
    ADLER32,
    CRC32,
    CRC32C,
    ;
  }

  public static HashFunction getHasher(HashType hashType) {
    switch(hashType) {
      case MURMUR3_128:
        return Hashing.murmur3_128();
      case MURMUR3_32:
        return Hashing.murmur3_32();
      case SIPHASH24:
        return Hashing.sipHash24();
      case MD5:
        return Hashing.md5();
      case SHA1:
        return Hashing.sha1();
      case SHA256:
        return Hashing.sha256();
      case SHA512:
        return Hashing.sha512();
      case ADLER32:
        return Hashing.adler32();
      case CRC32:
        return Hashing.crc32();
      case CRC32C:
        return Hashing.crc32c();
      default:
        throw new IllegalArgumentException(Utils.format("Unsupported Hashing Algorithm: {}", hashType.name()));
    }
  }

  public static RecordFunnel getRecordFunnel(
      Collection<String> fieldsToHash,
      boolean includeRecordHeader,
      boolean useSeparators,
      char separator) {
    if(fieldsToHash == null || fieldsToHash.isEmpty()) {
      return new RecordFunnel();
    }
    return new RecordFunnel(fieldsToHash, includeRecordHeader, useSeparators, separator);
  }

  public static class RecordFunnel implements Funnel<Record> {
    private Collection<String> fieldsToHash = null;
    private boolean includeRecordHeader = false;
    private boolean useSeparators = false;
    private Character separator = '\u0000';

    private RecordFunnel() {
    }

    public RecordFunnel(Collection<String> fieldsToHash, boolean includeRecordHeader, boolean useSeparators, Character separator) {
      this.fieldsToHash = fieldsToHash;
      this.includeRecordHeader = includeRecordHeader;
      this.useSeparators = useSeparators;
      this.separator = separator;
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
        if (field == null) {
          throw new IllegalArgumentException(
              Utils.format("Field Path {}  does not exist in the record", path)
          );
        }
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
            case TIME:
              sink.putLong(field.getValueAsTime().getTime());
              break;
            case DATETIME:
              sink.putLong(field.getValueAsDatetime().getTime());
              break;

            case DECIMAL:
            case STRING:
              sink.putString(field.getValueAsString(), Charset.defaultCharset());
              break;

            case BYTE_ARRAY:
              sink.putBytes(field.getValueAsByteArray());
              break;
            case FILE_REF:
              throw new IllegalStateException(
                  Utils.format(
                      "Hashing not supported for field: {} of type {}",
                      path,
                      field.getType()
                  )
              );
            default:
              break;
          }
        } else {
          sink.putBoolean(true);
        }
        if(useSeparators) {
          sink.putString(java.nio.CharBuffer.wrap(new char[] {separator}), Charset.forName("UTF-8"));
        }
      }

      if (this.includeRecordHeader) {
        for (String attrName : record.getHeader().getAttributeNames()) {
          String headerAttr = record.getHeader().getAttribute(attrName);
          if (headerAttr != null) {
            sink.putString(headerAttr, Charset.defaultCharset());
          } else {
            sink.putBoolean(true);
          }

          if(useSeparators) {
            sink.putString(java.nio.CharBuffer.wrap(new char[] {separator}), Charset.forName("UTF-8"));
          }
        }
      }
    }
  }
}
