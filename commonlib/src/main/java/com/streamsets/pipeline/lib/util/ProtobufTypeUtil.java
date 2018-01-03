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
package com.streamsets.pipeline.lib.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.protobuf.Errors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class ProtobufTypeUtil {
  private static final String FORWARD_SLASH = "/";
  private static final String KEY = "key";
  private static final String VALUE = "value";

  static final String PROTOBUF_UNKNOWN_FIELDS_PREFIX = "protobuf.unknown.fields.";

  private ProtobufTypeUtil() {}

  /*

    Protobuf Concepts:
    --------------------

    FileDescriptorSet - a protobuf describing one put more proto files.
    Got by parsing the .desc file. Contains one or more file descriptor proto

    FileDescriptorProto - a protobuf describing a Single proto file

    FileDescriptor - a java class containing the descriptor information for a proto file.
    Built using FileDescriptorProto and dependent FileDescriptors.

    Descriptor - describes a message type found within a proto file. A FileDescriptor has one or more Descriptors.

    FieldDescriptor - describes fields and extensions contained by a (message) descriptor

  */

  /**
   * Returns a protobuf descriptor instance from the provided descriptor file.
   *
   * @param context                   Stage context used for finding the SDC resources directory
   * @param protoDescriptorFile       Path to descriptor file relative to SDC_RESOURCES
   * @param messageType               The name of the message to decode
   * @param messageTypeToExtensionMap Map of protobuf extensions required for decoding
   * @param defaultValueMap           Map of default values to use for the message
   * @return protobuf descriptor instance
   * @throws StageException
   */
  public static Descriptors.Descriptor getDescriptor(
      ProtoConfigurableEntity.Context context,
      String protoDescriptorFile,
      String messageType,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Map<String, Object> defaultValueMap
  ) throws StageException {
    File descriptorFileHandle = new File(context.getResourcesDirectory(), protoDescriptorFile);
    try(
      FileInputStream fin = new FileInputStream(descriptorFileHandle);
      ) {
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
    } catch (FileNotFoundException e) {
      throw new StageException(Errors.PROTOBUF_06, descriptorFileHandle.getAbsolutePath(), e);
    } catch (IOException e) {
      throw new StageException(Errors.PROTOBUF_08, e.toString(), e);
    }
  }

  /**
   * Loads a Protobuf file descriptor set into an ubermap of file descriptors.
   *
   * @param set               FileDescriptorSet
   * @param dependenciesMap   FileDescriptor dependency map
   * @param fileDescriptorMap The populated map of FileDescriptors
   * @throws StageException
   */
  public static void getAllFileDescriptors(
      DescriptorProtos.FileDescriptorSet set,
      Map<String, Set<Descriptors.FileDescriptor>> dependenciesMap,
      Map<String, Descriptors.FileDescriptor> fileDescriptorMap
  ) throws StageException {
    List<DescriptorProtos.FileDescriptorProto> fileList = set.getFileList();
    try {
      for (DescriptorProtos.FileDescriptorProto fdp : fileList) {
        if (!fileDescriptorMap.containsKey(fdp.getName())) {
          Set<Descriptors.FileDescriptor> dependencies = dependenciesMap.get(fdp.getName());
          if (dependencies == null) {
            dependencies = new LinkedHashSet<>();
            dependenciesMap.put(fdp.getName(), dependencies);
            dependencies.addAll(getDependencies(dependenciesMap, fileDescriptorMap, fdp, set));
          }
          Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(
              fdp,
              dependencies.toArray(new Descriptors.FileDescriptor[dependencies.size()])
          );
          fileDescriptorMap.put(fdp.getName(), fileDescriptor);
        }
      }
    } catch (Descriptors.DescriptorValidationException e) {
      throw new StageException(Errors.PROTOBUF_07, e.getDescription(), e);
    }
  }

  /**
   * Populates a map of protobuf extensions and map with the default values for
   * each message field from a map of file descriptors.
   *
   * @param fileDescriptorMap Map of file descriptors
   * @param typeToExtensionMap Map of extensions to populate
   * @param defaultValueMap Map of default values to populate
   */
  public static void populateDefaultsAndExtensions(
      Map<String, Descriptors.FileDescriptor> fileDescriptorMap,
      Map<String, Set<Descriptors.FieldDescriptor>> typeToExtensionMap,
      Map<String, Object> defaultValueMap
  ) {
    for (Descriptors.FileDescriptor f : fileDescriptorMap.values()) {
      // go over every file descriptor and look for extensions and default values of those extensions
      for (Descriptors.FieldDescriptor fieldDescriptor : f.getExtensions()) {
        String containingType = fieldDescriptor.getContainingType().getFullName();
        Set<Descriptors.FieldDescriptor> fieldDescriptors = typeToExtensionMap.get(containingType);
        if (fieldDescriptors == null) {
          fieldDescriptors = new LinkedHashSet<>();
          typeToExtensionMap.put(containingType, fieldDescriptors);
        }
        fieldDescriptors.add(fieldDescriptor);
        if (fieldDescriptor.hasDefaultValue()) {
          defaultValueMap.put(containingType + "." + fieldDescriptor.getName(), fieldDescriptor.getDefaultValue());
        }
      }
      // go over messages within file descriptor and look for all fields and extensions and their defaults
      for (Descriptors.Descriptor d : f.getMessageTypes()) {
        addDefaultsAndExtensions(typeToExtensionMap, defaultValueMap, d);
      }
    }
  }

  /**
   * Generates a protobuf descriptor instance from a FileDescriptor set.
   *
   * @param set               set of file descriptors
   * @param fileDescriptorMap map of message types to file descriptors
   * @param descriptorFile    descriptor file for message to be decoded
   * @param qualifiedMessageType       the name of the message to be decoded
   * @return protobuf descriptor instance
   * @throws StageException
   */
  public static Descriptors.Descriptor getDescriptor(
      DescriptorProtos.FileDescriptorSet set,
      Map<String, Descriptors.FileDescriptor> fileDescriptorMap,
      String descriptorFile,
      String qualifiedMessageType
  ) throws StageException {

    // find the FileDescriptorProto which contains the message type
    // IF cannot find, then bail out
    String packageName = null;
    String messageType = qualifiedMessageType;
    int lastIndex = qualifiedMessageType.lastIndexOf('.');
    if (lastIndex != -1) {
      packageName = qualifiedMessageType.substring(0, lastIndex);
      messageType = qualifiedMessageType.substring(lastIndex + 1);
    }
    DescriptorProtos.FileDescriptorProto file = getFileDescProtoForMsgType(packageName, messageType, set);
    if (file == null) {
      // could not find the message type from all the proto files contained in the descriptor file
      throw new StageException(Errors.PROTOBUF_00, qualifiedMessageType, descriptorFile);
    }
    // finally get the FileDescriptor for the message type
    Descriptors.FileDescriptor fileDescriptor = fileDescriptorMap.get(file.getName());
    // create builder using the FileDescriptor
    // this can only find the top level message types
    return fileDescriptor.findMessageTypeByName(messageType);

  }

  private static DescriptorProtos.FileDescriptorProto getFileDescProtoForMsgType(
      String packageName,
      String messageType,
      DescriptorProtos.FileDescriptorSet set
  ) {
    DescriptorProtos.FileDescriptorProto file = null;
    for (DescriptorProtos.FileDescriptorProto fileDescriptorProto : set.getFileList()) {
      if (!packageMatch(fileDescriptorProto, packageName)) {
        continue;
      }
      file = containsMessageType(fileDescriptorProto, messageType);
      if (file != null) {
        break;
      }
    }
    return file;
  }

  private static DescriptorProtos.FileDescriptorProto containsMessageType(
    DescriptorProtos.FileDescriptorProto fileDescriptorProto, String messageType
  ) {
    DescriptorProtos.FileDescriptorProto file = null;
    for (DescriptorProtos.DescriptorProto descriptorProto :
      getAllMessageTypesInDescriptorProto(fileDescriptorProto)) {
      if (messageType.equals(descriptorProto.getName())) {
        file = fileDescriptorProto;
        break;
      }
    }
    return file;
  }

  private static boolean packageMatch(DescriptorProtos.FileDescriptorProto fileDescriptorProto, String packageName) {
    // Its a match iff
    // 1. package name specified as part of message type matches the package name of FileDescriptorProto
    // 2. No package specified as part of message type and no package name specified in FileDescriptorProto
    boolean packageMatch = false;
    if (packageName != null && packageName.equals(fileDescriptorProto.getPackage())) {
      packageMatch = true;
    } else if (packageName == null && !fileDescriptorProto.hasPackage()) {
      packageMatch = true;
    }
    return packageMatch;
  }

  /**
   * Converts a protobuf message to an SDC Record Field.
   *
   * @param record                    SDC Record to add field to
   * @param fieldPath                 location in record where to insert field.
   * @param descriptor                protobuf descriptor instance
   * @param messageTypeToExtensionMap protobuf extensions map
   * @param message                   message to decode and insert into the specified field path
   * @return new Field instance representing the decoded message
   * @throws DataParserException
   */
  public static Field protobufToSdcField(
      Record record,
      String fieldPath,
      Descriptors.Descriptor descriptor,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Object message
  ) throws DataParserException {
    Map<String, Field> sdcRecordMapFieldValue = new HashMap<>();

    // get all the expected fields from the proto file
    Map<String, Descriptors.FieldDescriptor> protobufFields = new LinkedHashMap<>();
    for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
      protobufFields.put(fieldDescriptor.getName(), fieldDescriptor);
    }

    // get all fields in the read message
    Map<Descriptors.FieldDescriptor, Object> values = ((DynamicMessage) message).getAllFields();

    // for every field present in the proto definition create an sdc field.
    for (Descriptors.FieldDescriptor fieldDescriptor : protobufFields.values()) {
      Object value = values.get(fieldDescriptor);
      sdcRecordMapFieldValue.put(
          fieldDescriptor.getName(),
          createField(record, fieldPath, fieldDescriptor, messageTypeToExtensionMap, value)
      );
    }

    // handle applicable extensions for this message type
    if (messageTypeToExtensionMap.containsKey(descriptor.getFullName())) {
      for (Descriptors.FieldDescriptor fieldDescriptor : messageTypeToExtensionMap.get(descriptor.getFullName())) {
        if (values.containsKey(fieldDescriptor)) {
          Object value = values.get(fieldDescriptor);
          sdcRecordMapFieldValue.put(
              fieldDescriptor.getName(),
              createField(record, fieldPath, fieldDescriptor, messageTypeToExtensionMap, value)
          );
        }
      }
    }

    // handle unknown fields
    // unknown fields can go into the record header
    UnknownFieldSet unknownFields = ((DynamicMessage) message).getUnknownFields();
    if (!unknownFields.asMap().isEmpty()) {
      ByteArrayOutputStream bOut = new ByteArrayOutputStream();
      try {
        unknownFields.writeDelimitedTo(bOut);
        bOut.flush();
        bOut.close();
      } catch (IOException e) {
        throw new DataParserException(Errors.PROTOBUF_10, e.toString(), e);
      }
      String path = fieldPath.isEmpty() ? FORWARD_SLASH : fieldPath;
      byte[] bytes = org.apache.commons.codec.binary.Base64.encodeBase64(bOut.toByteArray());
      record.getHeader().setAttribute(PROTOBUF_UNKNOWN_FIELDS_PREFIX + path, new String(bytes, StandardCharsets.UTF_8));
    }

    return Field.create(sdcRecordMapFieldValue);
  }

  /**
   * Creates an SDC Record Field from the provided protobuf message and descriptor.
   *
   * @param record                    record to be augmented
   * @param fieldPath                 field path in the record to insert new Field
   * @param fieldDescriptor           descriptor for this message
   * @param messageTypeToExtensionMap protobuf type extensions
   * @param message                   protobuf message to decode
   * @return reference to the Field added to the record.
   * @throws DataParserException
   */
  @SuppressWarnings("unchecked")
  private static Field createField(
      Record record,
      String fieldPath,
      Descriptors.FieldDescriptor fieldDescriptor,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Object message
  ) throws DataParserException {
    Field newField;
    if (message == null) {
      // If the message does not contain required fields then builder.build() throws UninitializedMessageException
      Object defaultValue = null;
      // get default values only for optional fields and non-message types
      if (fieldDescriptor.isOptional() && fieldDescriptor.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
        defaultValue = fieldDescriptor.getDefaultValue();
      }
      newField = Field.create(getFieldType(fieldDescriptor.getJavaType()), defaultValue);
    } else if (fieldDescriptor.isMapField()) {
      // Map entry (protobuf 3 map)
      Map<String, Field> sdcMapFieldValues = new HashMap<>();
      Collection<DynamicMessage> mapEntries = (Collection<DynamicMessage>) message;
      // MapEntry
      for (DynamicMessage dynamicMessage : mapEntries) {
        // MapEntry has 2 fields, key and value
        Map<Descriptors.FieldDescriptor, Object> kv = dynamicMessage.getAllFields();
        String key = null;
        Object value = null;
        Descriptors.FieldDescriptor valueDescriptor = null;
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : kv.entrySet()) {
          switch (entry.getKey().getName()) {
            case KEY:
              key = entry.getValue().toString();
              break;
            case VALUE:
              value = entry.getValue();
              valueDescriptor = entry.getKey();
              break;
            default:
              throw new DataParserException(Errors.PROTOBUF_09, entry.getKey().getName());
          }
        }
        if (key != null && valueDescriptor != null) {
          sdcMapFieldValues.put(
              key, createSdcField(record, fieldPath, valueDescriptor, messageTypeToExtensionMap, value)
          );
        }
      }

      newField = Field.create(sdcMapFieldValues);
    } else if (fieldDescriptor.isRepeated()) {
      // List entry (repeated)
      List<?> list = (List<?>) message;
      List<Field> listField = new ArrayList<>();
      for (int i = 0; i < list.size(); i++) {
        if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
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
      newField = Field.create(listField);
    } else {
      // normal entry
      newField = createSdcField(record, fieldPath, fieldDescriptor, messageTypeToExtensionMap, message);
    }
    return newField;
  }

  private static Field createSdcField(
      Record record,
      String fieldPath,
      Descriptors.FieldDescriptor fieldDescriptor,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Object value
  ) throws DataParserException {
    Field f;
    switch (fieldDescriptor.getJavaType()) {
      case BOOLEAN:
        f = Field.create(Field.Type.BOOLEAN, value);
        break;
      case BYTE_STRING:
        f = Field.create(Field.Type.BYTE_ARRAY, ((ByteString) value).toByteArray());
        break;
      case DOUBLE:
        f = Field.create(Field.Type.DOUBLE, value);
        break;
      case ENUM:
        f = Field.create(Field.Type.STRING, ((Descriptors.EnumValueDescriptor) value).getName());
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
        throw new DataParserException(Errors.PROTOBUF_03, fieldDescriptor.getJavaType().name());
    }
    return f;
  }

  private static Field.Type getFieldType(Descriptors.FieldDescriptor.JavaType javaType) throws DataParserException {
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
        throw new DataParserException(Errors.PROTOBUF_03, javaType);
    }
    return type;
  }

  private static List<DescriptorProtos.DescriptorProto> getAllMessageTypesInDescriptorProto(
      DescriptorProtos.FileDescriptorProto fileDescriptorProto
  ) {
    Queue<DescriptorProtos.DescriptorProto> queue = new LinkedList<>();
    queue.addAll(fileDescriptorProto.getMessageTypeList());
    List<DescriptorProtos.DescriptorProto> result = new ArrayList<>();
    while (!queue.isEmpty()) {
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
  ) throws StageException {
    Set<Descriptors.FileDescriptor> result = new LinkedHashSet<>();
    for (String name : file.getDependencyList()) {
      DescriptorProtos.FileDescriptorProto fileDescriptorProto = null;
      for (DescriptorProtos.FileDescriptorProto fdp : set.getFileList()) {
        if (name.equals(fdp.getName())) {
          fileDescriptorProto = fdp;
          break;
        }
      }
      if (fileDescriptorProto == null) {
        // could not find the message type from all the proto files contained in the descriptor file
        throw new StageException(Errors.PROTOBUF_01, file.getName());
      }
      Descriptors.FileDescriptor fileDescriptor;
      if (fileDescriptorMap.containsKey(fileDescriptorProto.getName())) {
        fileDescriptor = fileDescriptorMap.get(fileDescriptorProto.getName());
      } else {
        Set<Descriptors.FileDescriptor> deps = new LinkedHashSet<>();
        if (dependenciesMap.containsKey(name)) {
          deps.addAll(dependenciesMap.get(name));
        } else {
          deps.addAll(getDependencies(dependenciesMap, fileDescriptorMap, fileDescriptorProto, set));
        }
        try {
          fileDescriptor = Descriptors.FileDescriptor.buildFrom(
              fileDescriptorProto,
              deps.toArray(new Descriptors.FileDescriptor[deps.size()])
          );
        } catch (Descriptors.DescriptorValidationException e) {
          throw new StageException(Errors.PROTOBUF_07, e.getDescription(), e);
        }
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
    for (Descriptors.FieldDescriptor fieldDescriptor : d.getExtensions()) {
      String containingType = fieldDescriptor.getContainingType().getFullName();
      Set<Descriptors.FieldDescriptor> fieldDescriptors = e.get(containingType);
      if (fieldDescriptors == null) {
        fieldDescriptors = new LinkedHashSet<>();
        e.put(containingType, fieldDescriptors);
      }
      fieldDescriptors.add(fieldDescriptor);
      if (fieldDescriptor.hasDefaultValue()) {
        defaultValueMap.put(
            fieldDescriptor.getContainingType().getFullName() + "." + fieldDescriptor.getName(),
            fieldDescriptor.getDefaultValue()
        );
      }
    }
    for (Descriptors.FieldDescriptor fieldDescriptor : d.getFields()) {
      if (fieldDescriptor.hasDefaultValue()) {
        defaultValueMap.put(d.getFullName() + "." + fieldDescriptor.getName(), fieldDescriptor.getDefaultValue());
      }
    }
    for (Descriptors.Descriptor nestedType : d.getNestedTypes()) {
      addDefaultsAndExtensions(e, defaultValueMap, nestedType);
    }
  }

  /**
   * Serializes a record to a protobuf message using the specified descriptor.
   *
   * @param record                    Record to serialize
   * @param desc                      Protobuf descriptor
   * @param messageTypeToExtensionMap Protobuf extension map
   * @param defaultValueMap           Protobuf default field values
   * @return serialized message
   * @throws DataGeneratorException
   */
  public static DynamicMessage sdcFieldToProtobufMsg(
      Record record,
      Descriptors.Descriptor desc,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Map<String, Object> defaultValueMap
  ) throws DataGeneratorException {
    return sdcFieldToProtobufMsg(record, record.get(), "", desc, messageTypeToExtensionMap, defaultValueMap);
  }

  /**
   * Serializes a field path in a record to a protobuf message using the specified descriptor.
   *
   * @param record                    Record with the field to serialize
   * @param field                     The field to serialize
   * @param fieldPath                 The field path of the specified field
   * @param desc                      Protobuf descriptor
   * @param messageTypeToExtensionMap Protobuf extension map
   * @param defaultValueMap           Protobuf default field values
   * @return serialized message
   * @throws DataGeneratorException
   */
  private static DynamicMessage sdcFieldToProtobufMsg(
      Record record,
      Field field,
      String fieldPath,
      Descriptors.Descriptor desc,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Map<String, Object> defaultValueMap
  ) throws DataGeneratorException {
    if (field == null) {
      return null;
    }

    // compute all fields to look for including extensions
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(desc);
    List<Descriptors.FieldDescriptor> fields = new ArrayList<>();
    fields.addAll(desc.getFields());
    if (messageTypeToExtensionMap.containsKey(desc.getFullName())) {
      fields.addAll(messageTypeToExtensionMap.get(desc.getFullName()));
    }

    // root field is always a Map in a record representing protobuf data
    Map<String, Field> valueAsMap = field.getValueAsMap();

    for (Descriptors.FieldDescriptor f : fields) {
      Field mapField = valueAsMap.get(f.getName());
      // Repeated field
      if (f.isMapField()) {
        handleMapField(record, mapField, fieldPath, messageTypeToExtensionMap, defaultValueMap, f, builder);
      } else if (f.isRepeated()) {
        if (mapField != null) {
          handleRepeatedField(
              record,
              mapField,
              fieldPath,
              messageTypeToExtensionMap,
              defaultValueMap,
              f,
              builder
          );
        }
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
    try {
      handleUnknownFields(record, fieldPath, builder);
    } catch (IOException e) {
      throw new DataGeneratorException(Errors.PROTOBUF_05, e.toString(), e);
    }

    return builder.build();
  }

  private static void handleUnknownFields(
      Record record,
      String fieldPath,
      DynamicMessage.Builder builder
  ) throws IOException {
    String path = fieldPath.isEmpty() ? FORWARD_SLASH : fieldPath;
    String attribute = record.getHeader().getAttribute(ProtobufTypeUtil.PROTOBUF_UNKNOWN_FIELDS_PREFIX + path);
    if (attribute != null) {
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
      DynamicMessage.Builder builder
  ) throws DataGeneratorException {
    Object val;
    String keyName = f.getName();
    if (valueAsMap.containsKey(keyName)) {
      val = getValue(
          f,
          valueAsMap.get(keyName),
          record,
          fieldPath + FORWARD_SLASH + f.getName(),
          messageTypeToExtensionMap,
          defaultValueMap
      );
    } else {
      // record does not contain field, look up default value
      String key = desc.getFullName() + "." + f.getName();
      if (!defaultValueMap.containsKey(key) && !f.isOptional()) {
        throw new DataGeneratorException(
            Errors.PROTOBUF_04,
            record.getHeader().getSourceId(),
            key
        );
      }
      val = defaultValueMap.get(key);
    }
    if (val != null) {
      builder.setField(f, val);
    }
  }

  private static void handleMapField(
      Record record,
      Field field,
      String fieldPath,
      Map<String, Set<Descriptors.FieldDescriptor>> messageTypeToExtensionMap,
      Map<String, Object> defaultValueMap,
      Descriptors.FieldDescriptor fieldDescriptor,
      DynamicMessage.Builder builder
  ) throws DataGeneratorException {
    Descriptors.Descriptor mapEntryDescriptor = fieldDescriptor.getMessageType();
    // MapEntry contains key and value fields
    Map<String, Field> sdcMapField = field.getValueAsMap();
    for (Map.Entry<String, Field> entry : sdcMapField.entrySet()) {
      builder.addRepeatedField(fieldDescriptor, DynamicMessage.newBuilder(mapEntryDescriptor)
          .setField(mapEntryDescriptor.findFieldByName(KEY), entry.getKey())
          .setField(
              mapEntryDescriptor.findFieldByName(VALUE),
              getValue(
                  mapEntryDescriptor.findFieldByName(VALUE),
                  entry.getValue(),
                  record,
                  fieldPath + FORWARD_SLASH + entry.getKey(),
                  messageTypeToExtensionMap,
                  defaultValueMap
              )
          )
          .build()
      );
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
  ) throws DataGeneratorException {
    List<Object> toReturn = new ArrayList<>();
    List<Field> valueAsList = field.getValueAsList();
    if(valueAsList != null) {
      // According to proto 2 and 3 language guide repeated fields can have 0 elements.
      // Also null is treated as empty in case of json mappings so I guess we can ignore if it is null.
      for (int i = 0; i < valueAsList.size(); i++) {
        if (f.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
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
  ) throws DataGeneratorException {
    Object value = null;
    try {
      if (field.getValue() != null) {
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
            value = sdcFieldToProtobufMsg(
                record, field, protoFieldPath, messageType, messageTypeToExtensionMap, defaultValueMap
            );
            break;
          default:
            throw new DataGeneratorException(Errors.PROTOBUF_03, f.getJavaType().name());
        }
      }
    } catch (IllegalArgumentException e) {
      throw new DataGeneratorException(Errors.PROTOBUF_11, field.getValue(), f.getJavaType().name(), e);
    }
    return value;
  }
}
