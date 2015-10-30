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
package com.streamsets.pipeline.lib.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.protobuf.Errors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class ProtobufTypeUtil {

  /*

    Protobuf Concepts:
    --------------------

    FileDescriptorSet - a protobuf describing one put more proto files.
    Got by parsing the .desc file. Contains one or more file descriptor proto

    FileDescriptorProto - a protobuf describing a Single proto file

    FileDescriptor - a java class containing the descriptor information for a proto file.
    Built using FileDescriptorProto and dependent FileDescriptors.

    Descriptor - descirbes a message type found within a proto file. A FileDescriptor has one or more Descriptors.

    FieldDescriptor - describes fields and extensions contained by a (message) descriptor

  */


  private static final String FORWARD_SLASH = "/";
  static final String PROTOBUF_UNKNOWN_FIELDS_PREFIX = "protobuf.unknown.fields.";

  public static void getAllFileDescriptors(
    DescriptorProtos.FileDescriptorSet set,
    Map<String, Set<Descriptors.FileDescriptor>> dependenciesMap,
    Map<String, Descriptors.FileDescriptor> fileDescriptorMap
  ) throws Descriptors.DescriptorValidationException, IOException {

    List<DescriptorProtos.FileDescriptorProto> fileList = set.getFileList();
    for(DescriptorProtos.FileDescriptorProto fdp : fileList) {
      if(!fileDescriptorMap.containsKey(fdp.getName())) {
        Set<Descriptors.FileDescriptor> dependencies = dependenciesMap.get(fdp.getName());
        if (dependencies == null) {
          dependencies = new LinkedHashSet<>();
          dependenciesMap.put(fdp.getName(), dependencies);
          dependencies.addAll(getDependencies(dependenciesMap, fileDescriptorMap, fdp, set));
        }
        Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(
          fdp,
          dependencies.toArray(new Descriptors.FileDescriptor[0])
        );
        fileDescriptorMap.put(fdp.getName(), fileDescriptor);
      }
    }

  }

  public static Map<String, Set<Descriptors.FieldDescriptor>> getAllExtensions(
    Map<String, Descriptors.FileDescriptor> fileDescriptorMap
  ) {
    Map<String, Set<Descriptors.FieldDescriptor>> allExtensions = new HashMap<>();
    for(Descriptors.FileDescriptor f : fileDescriptorMap.values()) {
      for(Descriptors.FieldDescriptor fieldDescriptor : f.getExtensions()) {
        String containingType = fieldDescriptor.getContainingType().getName();
        Set<Descriptors.FieldDescriptor> fieldDescriptors = allExtensions.get(containingType);
        if(fieldDescriptors == null) {
          fieldDescriptors = new LinkedHashSet<>();
          allExtensions.put(containingType, fieldDescriptors);
        }
        fieldDescriptors.add(fieldDescriptor);
      }
      for(Descriptors.Descriptor d : f.getMessageTypes()) {
        addExtensions(allExtensions, d);
      }

    }
    return allExtensions;
  }

  public static Descriptors.Descriptor getDescriptor(
    DescriptorProtos.FileDescriptorSet set,
    Map<String, Descriptors.FileDescriptor> fileDescriptorMap,
    String descriptorFile,
    String messageType
  ) throws Descriptors.DescriptorValidationException, IOException {

    // find the FileDescriptorProto which contains the message type
    // IF cannot find, then bail out
    DescriptorProtos.FileDescriptorProto file = null;
    for (DescriptorProtos.FileDescriptorProto fileDescriptorProto : set.getFileList()) {
      for (DescriptorProtos.DescriptorProto descriptorProto :
        getAllMessageTypesInDescriptorProto(fileDescriptorProto)) {
        if (messageType.equals(descriptorProto.getName())) {
          file = fileDescriptorProto;
          break;
        }
      }
      if (file != null) {
        break;
      }
    }
    if (file == null) {
      // could not find the message type from all the proto files contained in the descriptor file
      throw new IOException(
          Utils.format(Errors.PROTOBUF_PARSER_00.getMessage(), messageType, descriptorFile),
          null
      );
    }
    // finally get the FileDescriptor for the message type
    Descriptors.FileDescriptor fileDescriptor = fileDescriptorMap.get(file.getName());
    // create builder using the FileDescriptor
    return fileDescriptor.findMessageTypeByName(messageType);

  }

  public static Field protobufToSdcField(
      Record record,
      String fieldPath,
      Descriptors.Descriptor descriptor,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Object message
  ) throws IOException {
    Map<String, Field> fieldMap = new HashMap<>();

    // get all the expected fields from the proto file
    Map<String, Descriptors.FieldDescriptor> fields = new LinkedHashMap<>();
    for(Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
      fields.put(fieldDescriptor.getName(), fieldDescriptor);
    }

    // get all fields in the read message
    Map<Descriptors.FieldDescriptor, Object> values = ((DynamicMessage)message).getAllFields();

    // for every field present in the proto definition create an sdc field.
    for(Descriptors.FieldDescriptor fieldDescriptor : fields.values()) {
      Object value = values.get(fieldDescriptor);
      fieldMap.put(
          fieldDescriptor.getName(),
          createField(record, fieldPath, fieldDescriptor, messageTypeToExtensionMap, value)
      );
    }

    // handle applicable extensions for this message type
    if(messageTypeToExtensionMap.containsKey(descriptor.getName())) {
      for(Descriptors.FieldDescriptor fieldDescriptor : messageTypeToExtensionMap.get(descriptor.getName())) {
        if(values.containsKey(fieldDescriptor)) {
          Object value = values.get(fieldDescriptor);
          fieldMap.put(
              fieldDescriptor.getName(),
              createField(record, fieldPath, fieldDescriptor, messageTypeToExtensionMap, value)
          );
        }
      }
    }

    // handle unknown fields
    // unknown fields can go into the record header
    UnknownFieldSet unknownFields = ((DynamicMessage) message).getUnknownFields();
    if(!unknownFields.asMap().isEmpty()) {
      ByteArrayOutputStream bOut = new ByteArrayOutputStream();
      unknownFields.writeDelimitedTo(bOut);
      bOut.flush();
      bOut.close();
      String path = fieldPath;
      if(path.isEmpty()) {
        path = FORWARD_SLASH;
      }
      byte[] bytes = org.apache.commons.codec.binary.Base64.encodeBase64(bOut.toByteArray());
      record.getHeader().setAttribute(PROTOBUF_UNKNOWN_FIELDS_PREFIX + path, new String(bytes));
    }

    return Field.create(fieldMap);
  }

  private static Field createField(
      Record record,
      String fieldPath,
      Descriptors.FieldDescriptor fieldDescriptor,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Object value
  ) throws IOException {
    Field f;
    if(value == null) {
      // If the message does not contain required fields then builder.build() throws UninitializedMessageException
      f = Field.create(getFieldType(fieldDescriptor.getJavaType()), value);
    } else if(fieldDescriptor.isRepeated()) {
      List<?> list = (List<?>) value;
      List<Field> listField = new ArrayList<>();
      for (int i = 0; i < list.size(); i++) {
        if(fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
          listField.add(
              protobufToSdcField(
                  record,
                  fieldPath + "[" + i + "]",
                  fieldDescriptor.getMessageType(),
                  messageTypeToExtensionMap,
                  list.get(i)
              )
          );
        } else {
          listField.add(
              createSdcField(
                  record,
                  fieldPath + "[" + i + "]",
                  fieldDescriptor,
                  messageTypeToExtensionMap,
                  list.get(i)
              )
          );
        }
      }
      f = Field.create(listField);
    } else {
      f = createSdcField(record, fieldPath, fieldDescriptor, messageTypeToExtensionMap, value);
    }
    return f;
  }

  private static Field createSdcField(
      Record record,
      String fieldPath,
      Descriptors.FieldDescriptor fieldDescriptor,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Object value
  ) throws IOException {
    Field f;
    switch (fieldDescriptor.getJavaType()) {
      case BOOLEAN:
        f = Field.create(Field.Type.BOOLEAN, value);
        break;
      case BYTE_STRING:
        f = Field.create(Field.Type.BYTE_ARRAY, ((ByteString)value).toByteArray());
        break;
      case DOUBLE:
        f = Field.create(Field.Type.DOUBLE, value);
        break;
      case ENUM:
        f = Field.create(Field.Type.STRING, ((Descriptors.EnumValueDescriptor)value).getName());
        break;
      case FLOAT:
        f = Field.create(Field.Type.FLOAT, value);
        break;
      case INT:
        f = Field.create(Field.Type.INTEGER, value);
        break;
      case LONG:
        f = Field.create(Field.Type.LONG, value);
        break;
      case STRING:
        f = Field.create(Field.Type.STRING, value);
        break;
      case MESSAGE:
        f = protobufToSdcField(
            record,
            fieldPath + FORWARD_SLASH + fieldDescriptor.getName(),
            fieldDescriptor.getMessageType(),
            messageTypeToExtensionMap,
            value
        );
        break;
      default:
        throw new RuntimeException(
          Utils.format("Unknown Field Descriptor type '{}'", fieldDescriptor.getJavaType().name())
        );
    }
    return f;
  }

  private static Field.Type getFieldType(Descriptors.FieldDescriptor.JavaType javaType) {
    Field.Type type;
    switch (javaType) {
      case BOOLEAN:
        type = Field.Type.BOOLEAN;
        break;
      case BYTE_STRING:
        type = Field.Type.BYTE_ARRAY;
        break;
      case DOUBLE:
        type = Field.Type.DOUBLE;
        break;
      case ENUM:
        type = Field.Type.STRING;
        break;
      case FLOAT:
        type = Field.Type.FLOAT;
        break;
      case INT:
        type = Field.Type.INTEGER;
        break;
      case LONG:
        type = Field.Type.LONG;
        break;
      case STRING:
        type = Field.Type.STRING;
        break;
      case MESSAGE:
        type = Field.Type.MAP;
        break;
      default:
        throw new RuntimeException(
          Utils.format("Unknown Field Descriptor type '{}'", javaType)
        );
    }
    return type;
  }

  private static List<DescriptorProtos.DescriptorProto> getAllMessageTypesInDescriptorProto(
    DescriptorProtos.FileDescriptorProto fileDescriptorProto
  ) {
    Queue<DescriptorProtos.DescriptorProto> queue = new LinkedList<>();
    queue.addAll(fileDescriptorProto.getMessageTypeList());
    List<DescriptorProtos.DescriptorProto> result = new ArrayList<>();
    while(!queue.isEmpty()) {
      DescriptorProtos.DescriptorProto descriptorProto = queue.poll();
      queue.addAll(descriptorProto.getNestedTypeList());
      result.add(descriptorProto);
    }
    return result;
  }

  private static Set<Descriptors.FileDescriptor> getDependencies(
    Map<String, Set<Descriptors.FileDescriptor>> dependenciesMap,
    Map<String, Descriptors.FileDescriptor> fileDescriptorMap,
    DescriptorProtos.FileDescriptorProto file,
    DescriptorProtos.FileDescriptorSet set
  ) throws Descriptors.DescriptorValidationException, IOException {
    Set<Descriptors.FileDescriptor> result = new LinkedHashSet<>();
    Iterator<String> iter  = file.getDependencyList().iterator();
    while(iter.hasNext()) {
      String name = iter.next();
      DescriptorProtos.FileDescriptorProto fileDescriptorProto = null;
      for (DescriptorProtos.FileDescriptorProto fdp : set.getFileList()) {
        if (name.equals(fdp.getName())) {
          fileDescriptorProto = fdp;
          break;
        }
      }
      if (fileDescriptorProto == null) {
        // could not find the message type from all the proto files contained in the descriptor file
        throw new IOException(Utils.format(Errors.PROTOBUF_PARSER_01.getMessage(), file.getName()), null);
      }
      Descriptors.FileDescriptor fileDescriptor = null;
      if(fileDescriptorMap.containsKey(fileDescriptorProto.getName())) {
        fileDescriptor = fileDescriptorMap.get(fileDescriptorProto.getName());
      } else {

        Set<Descriptors.FileDescriptor> deps = new LinkedHashSet<>();
        if (dependenciesMap.containsKey(name)) {
          deps.addAll(dependenciesMap.get(name));
        } else {

          deps.addAll(getDependencies(dependenciesMap, fileDescriptorMap, fileDescriptorProto, set));
        }
        fileDescriptor = Descriptors.FileDescriptor.buildFrom(
          fileDescriptorProto,
          deps.toArray(new Descriptors.FileDescriptor[0])
        );
      }
      result.add(fileDescriptor);
    }
    return result;
  }

  private static void addExtensions(
      Map<String, Set<Descriptors.FieldDescriptor>> e,
      Descriptors.Descriptor d
  ) {
    for(Descriptors.FieldDescriptor fieldDescriptor : d.getExtensions()) {
      String containingType = fieldDescriptor.getContainingType().getName();
      Set<Descriptors.FieldDescriptor> fieldDescriptors = e.get(containingType);
      if(fieldDescriptors == null) {
        fieldDescriptors = new LinkedHashSet<>();
        e.put(containingType, fieldDescriptors);
      }
      fieldDescriptors.add(fieldDescriptor);
    }
    for(Descriptors.Descriptor nestedType : d.getNestedTypes()) {
      addExtensions(e, nestedType);
    }
  }
}
