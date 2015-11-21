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
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.parser.protobuf.Errors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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

  public static Descriptors.Descriptor getDescriptor(
      Stage.Context context,
      String protoDescriptorFile,
      String messageType,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Map<String, Object> defaultValueMap
  ) throws IOException, Descriptors.DescriptorValidationException {

    FileInputStream fin = new FileInputStream(new File(context.getResourcesDirectory(), protoDescriptorFile));
    DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.parseFrom(fin);

    // Iterate over all the file descriptor set computed above and cache dependencies and all encountered
    // file descriptors

    // this map holds all the dependencies that a given file descriptor has.
    // This cached map will be looked up while building FileDescriptor instances
    Map<String, Set<Descriptors.FileDescriptor>> fileDescriptorDependentsMap = new HashMap<>();
    // All encountered FileDescriptor instances cached based on their name.
    Map<String, Descriptors.FileDescriptor> fileDescriptorMap = new HashMap<>();
    ProtobufTypeUtil.getAllFileDescriptors(set, fileDescriptorDependentsMap, fileDescriptorMap);

    // Get the descriptor for the expected message type
    Descriptors.Descriptor descriptor = ProtobufTypeUtil.getDescriptor(set, fileDescriptorMap, protoDescriptorFile, messageType);

    // Compute and cache all extensions defined for each message type
    ProtobufTypeUtil.populateDefaultsAndExtensions(fileDescriptorMap, messageTypeToExtensionMap, defaultValueMap);
    return descriptor;
  }

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

  public static void populateDefaultsAndExtensions(
    Map<String, Descriptors.FileDescriptor> fileDescriptorMap,
    Map<String, Set<Descriptors.FieldDescriptor>> typeToExtensionMap,
    Map<String, Object> defaultValueMap
  ) {
    for(Descriptors.FileDescriptor f : fileDescriptorMap.values()) {
      // go over every file descriptor and look for extensions and default values of those extensions
      for(Descriptors.FieldDescriptor fieldDescriptor : f.getExtensions()) {
        String containingType = fieldDescriptor.getContainingType().getFullName();
        Set<Descriptors.FieldDescriptor> fieldDescriptors = typeToExtensionMap.get(containingType);
        if(fieldDescriptors == null) {
          fieldDescriptors = new LinkedHashSet<>();
          typeToExtensionMap.put(containingType, fieldDescriptors);
        }
        fieldDescriptors.add(fieldDescriptor);
        if(fieldDescriptor.hasDefaultValue()) {
          defaultValueMap.put(containingType + "." + fieldDescriptor.getName(), fieldDescriptor.getDefaultValue());
        }
      }
      // go over messages within file descriptor and look for all fields and extensions and their defaults
      for(Descriptors.Descriptor d : f.getMessageTypes()) {
        addDefaultsAndExtensions(typeToExtensionMap, defaultValueMap, d);
      }
    }
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
    if(messageTypeToExtensionMap.containsKey(descriptor.getFullName())) {
      for(Descriptors.FieldDescriptor fieldDescriptor : messageTypeToExtensionMap.get(descriptor.getFullName())) {
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
      record.getHeader().setAttribute(PROTOBUF_UNKNOWN_FIELDS_PREFIX + path, new String(bytes, StandardCharsets.UTF_8));
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
      f = Field.create(getFieldType(fieldDescriptor.getJavaType()), null);
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
      Descriptors.FileDescriptor fileDescriptor;
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

  private static void addDefaultsAndExtensions(
    Map<String, Set<Descriptors.FieldDescriptor>> e,
    Map<String, Object> defaultValueMap,
    Descriptors.Descriptor d
  ) {
    for(Descriptors.FieldDescriptor fieldDescriptor : d.getExtensions()) {
      String containingType = fieldDescriptor.getContainingType().getFullName();
      Set<Descriptors.FieldDescriptor> fieldDescriptors = e.get(containingType);
      if(fieldDescriptors == null) {
        fieldDescriptors = new LinkedHashSet<>();
        e.put(containingType, fieldDescriptors);
      }
      fieldDescriptors.add(fieldDescriptor);
      if(fieldDescriptor.hasDefaultValue()) {
        defaultValueMap.put(
          fieldDescriptor.getContainingType().getFullName() + "." + fieldDescriptor.getName(),
          fieldDescriptor.getDefaultValue()
        );
      }
    }
    for(Descriptors.FieldDescriptor fieldDescriptor : d.getFields()) {
      if(fieldDescriptor.hasDefaultValue()) {
        defaultValueMap.put(d.getFullName() + "." + fieldDescriptor.getName(), fieldDescriptor.getDefaultValue());
      }
    }
    for(Descriptors.Descriptor nestedType : d.getNestedTypes()) {
      addDefaultsAndExtensions(e, defaultValueMap, nestedType);
    }
  }

  public static DynamicMessage sdcFieldToProtobufMsg(
      Record record,
      Descriptors.Descriptor desc,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Map<String, Object> defaultValueMap
  ) throws DataGeneratorException, IOException {
    return sdcFieldToProtobufMsg(record, record.get(), "", desc, messageTypeToExtensionMap, defaultValueMap);
  }

  private static DynamicMessage sdcFieldToProtobufMsg(
      Record record,
      Field field,
      String fieldPath,
      Descriptors.Descriptor desc,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Map<String, Object> defaultValueMap
  ) throws DataGeneratorException, IOException {

    if(field == null) {
      return null;
    }

    // compute all fields to look for including extensions
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(desc);
    List<Descriptors.FieldDescriptor> fields = new ArrayList<>();
    fields.addAll(desc.getFields());
    if(messageTypeToExtensionMap.containsKey(desc.getFullName())) {
      fields.addAll(messageTypeToExtensionMap.get(desc.getFullName()));
    }

    // root field is always a Map in a record representing protobuf data
    Map<String, Field> valueAsMap = field.getValueAsMap();

    for (Descriptors.FieldDescriptor f : fields) {
      field = valueAsMap.get(f.getName());
      // Repeated field
      if(f.isRepeated()) {
        handleRepeatedField(
            record,
            field,
            fieldPath,
            messageTypeToExtensionMap,
            defaultValueMap,
            f,
            builder
        );
      } else {
        // non repeated field
        handleNonRepeatedField(
            record,
            valueAsMap,
            fieldPath,
            messageTypeToExtensionMap,
            defaultValueMap,
            desc,
            f,
            builder
        );
      }
    }

    // if record has unknown fields for this field path, handle it
    handleUnknownFields(record, fieldPath, builder);

    return builder.build();
  }

  private static void handleUnknownFields(
      Record record,
      String fieldPath,
      DynamicMessage.Builder builder
  ) throws IOException {
    if(fieldPath.isEmpty()) {
      fieldPath = FORWARD_SLASH;
    }
    String attribute = record.getHeader().getAttribute(ProtobufTypeUtil.PROTOBUF_UNKNOWN_FIELDS_PREFIX + fieldPath);
    if(attribute != null) {
      UnknownFieldSet.Builder unknownFieldBuilder = UnknownFieldSet.newBuilder();
      unknownFieldBuilder.mergeDelimitedFrom(
          new ByteArrayInputStream(
              org.apache.commons.codec.binary.Base64.decodeBase64(attribute.getBytes(StandardCharsets.UTF_8))
          )
      );
      UnknownFieldSet unknownFieldSet = unknownFieldBuilder.build();
      builder.setUnknownFields(unknownFieldSet);
    }
  }

  private static void handleNonRepeatedField(
      Record record,
      Map<String, Field> valueAsMap,
      String fieldPath,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Map<String, Object> defaultValueMap,
      Descriptors.Descriptor desc,
      Descriptors.FieldDescriptor f,
      DynamicMessage.Builder builder) throws IOException, DataGeneratorException {
    Object val;
    if(valueAsMap.containsKey(f.getName())) {
      val = getValue(
        f,
        valueAsMap.get(f.getName()),
        record,
        fieldPath + FORWARD_SLASH + f.getName(),
        messageTypeToExtensionMap,
        defaultValueMap
      );
    }  else {
      // record does not contain field, look up default value
      String key = desc.getFullName() + "." + f.getName();
      if(!defaultValueMap.containsKey(key) && !f.isOptional()) {
        throw new DataGeneratorException(
          com.streamsets.pipeline.lib.generator.protobuf.Errors.PROTOBUF_GENERATOR_00,
          record.getHeader().getSourceId(),
          key
        );
      }
      val = defaultValueMap.get(key);
    }
    if(val != null) {
      builder.setField(f, val);
    }
  }

  private static void handleRepeatedField(
      Record record,
      Field field,
      String fieldPath,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Map<String, Object> defaultValueMap,
      Descriptors.FieldDescriptor f,
      DynamicMessage.Builder builder
  ) throws IOException, DataGeneratorException {
    List<Field> valueAsList = field.getValueAsList();
    List<Object> toReturn = new ArrayList<>(valueAsList.size());
    for(int i = 0; i < valueAsList.size(); i++) {
      if(f.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
        // repeated field of type message
        toReturn.add(
          sdcFieldToProtobufMsg(
            record,
            valueAsList.get(i),
            fieldPath + FORWARD_SLASH + f.getName() + "[" + i + "]",
            f.getMessageType(),
            messageTypeToExtensionMap,
            defaultValueMap
          )
        );
      } else {
        // repeated field of primitive types
        toReturn.add(
          getValue(
            f,
            valueAsList.get(i),
            record,
            fieldPath + FORWARD_SLASH + f.getName(),
            messageTypeToExtensionMap,
            defaultValueMap
          )
        );
      }
    }
    builder.setField(f, toReturn);
  }

  private static Object getValue(
      Descriptors.FieldDescriptor f,
      Field field,
      Record record,
      String protoFieldPath,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Map<String, Object> defaultValueMap
  ) throws DataGeneratorException, IOException {
    Object value = null;
    if(field.getValue() != null) {
      switch (f.getJavaType()) {
        case BOOLEAN:
          value = field.getValueAsBoolean();
          break;
        case BYTE_STRING:
          value = ByteString.copyFrom(field.getValueAsByteArray());
          break;
        case DOUBLE:
          value = field.getValueAsDouble();
          break;
        case ENUM:
          value = f.getEnumType().findValueByName(field.getValueAsString());
          break;
        case FLOAT:
          value = field.getValueAsFloat();
          break;
        case INT:
          value = field.getValueAsInteger();
          break;
        case LONG:
          value = field.getValueAsLong();
          break;
        case STRING:
          value = field.getValueAsString();
          break;
        case MESSAGE:
          Descriptors.Descriptor messageType = f.getMessageType();
          value = sdcFieldToProtobufMsg(record, field, protoFieldPath, messageType, messageTypeToExtensionMap, defaultValueMap);
          break;
        default:
          throw new RuntimeException(
            Utils.format("Unknown Field Descriptor type '{}'", f.getJavaType().name())
          );
      }
    }
    return value;
  }
}
