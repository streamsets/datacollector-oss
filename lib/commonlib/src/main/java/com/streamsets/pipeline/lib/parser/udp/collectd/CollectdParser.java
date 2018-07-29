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
package com.streamsets.pipeline.lib.parser.udp.collectd;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.parser.udp.AbstractParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CollectdParser extends AbstractParser {
  private static final Logger LOG = LoggerFactory.getLogger(CollectdParser.class);

  /**
   * Part Types
   **/
  private static final int HOST = 0x0000;
  private static final int TIME = 0x0001;
  private static final int TIME_HIRES = 0x0008;
  private static final int PLUGIN = 0x0002;
  private static final int PLUGIN_INSTANCE = 0x0003;
  private static final int TYPE = 0x0004;
  private static final int TYPE_INSTANCE = 0x0005;
  private static final int VALUES = 0x0006;
  private static final int INTERVAL = 0x0007;
  private static final int INTERVAL_HIRES = 0x0009;
  private static final int MESSAGE = 0x0100;
  private static final int SEVERITY = 0x0101;
  // TODO: Implement below
  private static final int SIGNATURE = 0x0200;
  private static final int ENCRYPTION = 0x0210;

  /**
   * Part Types
   **/
  private static final ImmutableMap<Integer, String> PART_TYPES = new ImmutableMap.Builder<Integer, String>()
      .put(0x0000, "host")
      .put(0x0001, "time")
      .put(0x0008, "time_hires")
      .put(0x0002, "plugin")
      .put(0x0003, "plugin_instance")
      .put(0x0004, "type")
      .put(0x0005, "type_instance")
      .put(0x0006, "values")
      .put(0x0007, "interval")
      .put(0x0009, "interval_hires")
      .put(0x0100, "message")
      .put(0x0101, "severity")
      .put(0x0200, "signature")
      .put(0x0210, "encryption")
      .build();

  /**
   * Value Types
   **/
  private static final byte COUNTER = 0; // big-endian unsigned integer
  private static final byte GAUGE = 1; // little-endian double
  private static final byte DERIVE = 2; // big-endian signed integer
  private static final byte ABSOLUTE = 3; // big-endian unsigned integer

  /**
   * Value Types
   **/
  private static final ImmutableMap<Byte, String> VALUE_TYPES = new ImmutableMap.Builder<Byte, String>()
      .put((byte) 0, "counter") // big-endian unsigned integer
      .put((byte) 1, "gauge") // little-endian double
      .put((byte) 2, "derive") // big-endian signed integer
      .put((byte) 3, "absolute") // big-endian unsigned integer
      .build();

  private static final Set<String> PLUGIN_TYPE_FIELDS =
      ImmutableSet.of(
          PART_TYPES.get(HOST),
          PART_TYPES.get(TIME),
          PART_TYPES.get(TIME_HIRES),
          PART_TYPES.get(TYPE_INSTANCE),
          PART_TYPES.get(SEVERITY),
          PART_TYPES.get(INTERVAL),
          PART_TYPES.get(INTERVAL_HIRES)
      );

  private static final Set<String> COLLECTD_TYPE_FIELDS =
      ImmutableSet.of(
          PART_TYPES.get(HOST),
          PART_TYPES.get(TIME),
          PART_TYPES.get(TIME_HIRES),
          PART_TYPES.get(PLUGIN),
          PART_TYPES.get(PLUGIN_INSTANCE),
          PART_TYPES.get(TYPE_INSTANCE),
          PART_TYPES.get(SEVERITY),
          PART_TYPES.get(INTERVAL),
          PART_TYPES.get(INTERVAL_HIRES)
      );

  enum SecurityLevel {
    NONE,
    SIGN,
    ENCRYPT
  }

  private final boolean convertTime;
  private final boolean excludeInterval;
  private final Charset charset;
  private SecurityLevel securityLevel = SecurityLevel.NONE;

  private long recordId = 0;

  private List<Record> records;
  private Map<String, Field> fields;
  private Map<String, List<String>> typesDb;
  private Map<String, String> authKeys;

  public CollectdParser(
      ProtoConfigurableEntity.Context context,
      boolean convertTime,
      String typesDbPath,
      boolean excludeInterval,
      String authFilePath,
      Charset charset
  ) {
    super(context);
    this.convertTime = convertTime;
    this.excludeInterval = excludeInterval;
    this.charset = charset;
    loadAuthFile(authFilePath);
    loadTypesDb(typesDbPath);
  }

  private void loadAuthFile(String authFileLocation) {
    authKeys = new HashMap<>();
    if (authFileLocation == null || authFileLocation.isEmpty()) {
      securityLevel = SecurityLevel.NONE;
      return;
    }

    try {
      Path authFilePath = FileSystems.getDefault().getPath(authFileLocation); // NOSONAR
      List<String> lines = Files.readAllLines(authFilePath, charset);

      for (String line : lines) {
        String[] auth = line.split(":");
        authKeys.put(auth[0].trim(), auth[1].trim());
      }
    } catch (IOException e) {
      LOG.error("Failed to read auth file.", e);
    }
    securityLevel = SecurityLevel.SIGN;
  }

  private void loadTypesDb(String typesDbLocation) {
    typesDb = new HashMap<>();
    try {
      List<String> lines;
      if (typesDbLocation == null || typesDbLocation.isEmpty()) {
        lines = Resources.readLines(Resources.getResource("types.db"), Charset.defaultCharset());
      } else {
        Path typesDbFile = FileSystems.getDefault().getPath(typesDbLocation); // NOSONAR
        lines = Files.readAllLines(typesDbFile, charset);
      }

      for (String line : lines) {
        String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
          continue;
        }
        String[] parts = trimmed.split("\\s+", 2);
        String dataSetName = parts[0];
        String[] typeSpecs = parts[1].split(",");

        List<String> typeParams = new ArrayList<>();
        for (String typeSpec : typeSpecs) {
          typeParams.add(typeSpec.trim().split(":")[0]);
        }
        typesDb.put(dataSetName, typeParams);
      }
    } catch (IOException e) {
      LOG.error("Failed to parse type db.", e);
    }


  }

  @Override
  public List<Record> parse(ByteBuf buf, InetSocketAddress recipient, InetSocketAddress sender)
      throws OnRecordErrorException {
    int offset = 0;
    records = new LinkedList<>();
    fields = new HashMap<>();

    while (offset < buf.readableBytes()) {
      offset = parsePart(offset, buf, fields);
    }

    return records;
  }

  /**
   * Parses a collectd packet "part".
   *
   * @param startOffset beginning offset for this part
   * @param buf         buffered packet
   * @param fields      field map for the output record
   * @return offset after consuming part
   */
  private int parsePart(int startOffset, ByteBuf buf, Map<String, Field> fields) throws OnRecordErrorException {
    int offset = startOffset;
    int type = buf.getUnsignedShort(offset); // 0-1
    offset += 2;
    final int length = buf.getUnsignedShort(offset); // 2-3
    offset += 2;

    switch (type) {
      case HOST:
      case PLUGIN:
      case PLUGIN_INSTANCE:
      case TYPE:
      case TYPE_INSTANCE:
      case MESSAGE:
        pruneFields(type);
        fields.put(PART_TYPES.get(type), Field.create(parseString(offset, length, buf)));
        offset += length - 4;
        break;
      case TIME_HIRES:
      case INTERVAL_HIRES:
        if (type != INTERVAL_HIRES || !excludeInterval) {
          long value = parseNumeric(offset, buf);
          if (convertTime) {
            value *= (Math.pow(2, -30) * 1000);
            type = type == TIME_HIRES ? TIME : INTERVAL;
          }
          fields.put(PART_TYPES.get(type), Field.create(value));
        }
        offset += 8;
        break;
      case TIME:
      case INTERVAL:
      case SEVERITY:
        if (type != INTERVAL || !excludeInterval) {
          fields.put(PART_TYPES.get(type), Field.create(parseNumeric(offset, buf)));
        }
        offset += 8;
        break;
      case VALUES:
        offset = parseValues(offset, buf);
        startNewRecord();
        break;
      case SIGNATURE:
        if (!verifySignature(offset, length, buf)) {
          throw new OnRecordErrorException(Errors.COLLECTD_02);
        }
        offset += length - 4;
        break;
      case ENCRYPTION:
        String user = parseUser(offset, buf);
        offset += (2 + user.length());
        byte[] iv = parseIv(offset, buf);
        offset += 16;
        decrypt(offset, length, buf, user, iv);
        // Skip the checksum and continue processing.
        offset += 20;
        break;
      default:
        // Don't recognize this part type, so skip it
        LOG.warn("Unrecognized part type: {}", type);
        offset += length - 4;
        break;
    }

    return offset;
  }

  private void pruneFields(int type) {
    List<String> toRemove = new ArrayList<>();
    switch (type) {
      case PLUGIN:
        for (String fieldName : fields.keySet()) {
          if (!PLUGIN_TYPE_FIELDS.contains(fieldName)) {
            toRemove.add(fieldName);
          }
        }
        break;
      case TYPE:
        for (String fieldName : fields.keySet()) {
          if (!COLLECTD_TYPE_FIELDS.contains(fieldName)) {
            toRemove.add(fieldName);
          }
        }
        break;
      default:
        // NO-OP
    }
    for (String fieldName : toRemove) {
      fields.remove(fieldName);
    }
  }

  private void startNewRecord() {
    Record record = context.createRecord(fields.get(PART_TYPES.get(HOST)).getValueAsString() + "::" + recordId++);

    record.set(Field.create(fields));
    records.add(record);

    fields = new HashMap<>(fields);
  }

  private long parseNumeric(int offset, ByteBuf buf) {
    // 8 bytes
    return buf.getLong(offset);
  }

  private String parseString(int offset, int length, ByteBuf buf) {
    // N-bytes
    byte[] bytes = new byte[length - 5];
    buf.getBytes(offset, bytes, 0, length - 5);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * Parses the value part of the packet where metrics are located
   *
   * @param startOffset beginning offset for this part
   * @param buf         buffered packet
   * @return offset after consuming part
   */
  private int parseValues(int startOffset, ByteBuf buf) throws OnRecordErrorException {
    int offset = startOffset;
    // N Values
    // For each Value:
    // 1 byte data type code
    int numValues = buf.getUnsignedShort(offset); // 4-5
    offset += 2;

    List<Byte> types = new ArrayList<>(numValues);

    while (numValues-- > 0) {
      types.add(buf.getByte(offset));
      offset += 1;
    }

    for (int i = 0; i < types.size(); i++) {
      Byte type = types.get(i);
      String label = getValueLabel(i, type);
      switch (type) {
        case COUNTER:
          fields.put(label, Field.create(buf.getUnsignedInt(offset)));
          offset += 8;
          break;
        case GAUGE:
          fields.put(
              label,
              Field.create(buf.order(ByteOrder.LITTLE_ENDIAN).getDouble(offset))
          );
          offset += 8;
          break;
        case DERIVE:
          fields.put(label, Field.create(buf.getLong(offset)));
          offset += 8;
          break;
        case ABSOLUTE:
          fields.put(label, Field.create(buf.getUnsignedInt(offset)));
          offset += 8;
          break;
        default:
          // error
          throw new OnRecordErrorException(Errors.COLLECTD_01, type);
      }
    }
    return offset;
  }

  private String getValueLabel(int index, Byte type) {
    String label = VALUE_TYPES.get(type);
    if (typesDb.containsKey(fields.get(PART_TYPES.get(TYPE)).getValueAsString())) {
      label = typesDb.get(fields.get(PART_TYPES.get(TYPE)).getValueAsString()).get(index);
    }
    return label;
  }

  private String parseUser(int offset, ByteBuf buf) {
    int userLength = buf.getUnsignedShort(offset);
    byte[] userBytes = new byte[userLength];
    buf.getBytes(offset + 2, userBytes, 0, userLength);
    return new String(userBytes, StandardCharsets.UTF_8);
  }

  private byte[] parseIv(int offset, ByteBuf buf) {
    byte[] iv = new byte[16];
    buf.getBytes(offset, iv, 0, 16);
    return iv;
  }

  private boolean verifySignature(int offset, int length, ByteBuf buf) throws OnRecordErrorException {
    boolean isVerified = false;
    if (securityLevel == SecurityLevel.NONE) {
      return true;
    }

    if (length < 33) {
      LOG.warn("No username");
    }

    if (length < 32) {
      LOG.warn("invalid signature");
    }

    byte[] signature = new byte[32];
    buf.getBytes(offset, signature, 0, 32);

    int userLength = length - offset - 32;
    byte[] userBytes = new byte[userLength];
    buf.getBytes(offset + 32, userBytes, 0, userLength);
    String username = new String(userBytes, StandardCharsets.UTF_8);

    if (!authKeys.containsKey(username)) {
      throw new OnRecordErrorException(Errors.COLLECTD_03, "Auth File doesn't contain requested user: " + username);
    }
    String key = authKeys.get(username);

    try {
      Mac sha256HMAC = Mac.getInstance("HmacSHA256");
      SecretKeySpec secretKey = new SecretKeySpec(key.getBytes(charset), "HmacSHA256");
      sha256HMAC.init(secretKey);

      int userPayloadLength = buf.capacity() - length + username.length();
      byte[] userPayload = new byte[userPayloadLength];
      buf.getBytes(offset + 32, userPayload, 0, userPayloadLength);
      isVerified = Arrays.equals(sha256HMAC.doFinal(userPayload), signature);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new OnRecordErrorException(Errors.COLLECTD_02, e.toString());
    }
    return isVerified;
  }

  private void decrypt(int offset, int length, ByteBuf buf, String user, byte[] iv) throws OnRecordErrorException {
    int contentLength = length - offset;
    if (contentLength < 26) {
      throw new OnRecordErrorException(Errors.COLLECTD_03, "Content Length was: " + contentLength);
    }

    if (!authKeys.containsKey(user)) {
      throw new OnRecordErrorException(Errors.COLLECTD_03, "Auth File doesn't contain requested user: " + user);
    }
    String key = authKeys.get(user);

    try {
      MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
      sha256.update(key.getBytes(charset));

      Cipher cipher = Cipher.getInstance("AES/OFB/NoPadding");

      SecretKeySpec keySpec = new SecretKeySpec(sha256.digest(), "AES");
      IvParameterSpec ivSpec = new IvParameterSpec(iv);
      cipher.init(
          Cipher.DECRYPT_MODE,
          keySpec,
          ivSpec
      );

      byte[] encrypted = new byte[contentLength];
      buf.getBytes(offset, encrypted, 0, contentLength);
      byte[] decrypted = cipher.doFinal(encrypted);


      if (!verifySha1Sum(decrypted)) {
        throw new OnRecordErrorException(Errors.COLLECTD_03, "SHA-1 Checksum Failed");
      }

      buf.setBytes(offset, decrypted);
    } catch (GeneralSecurityException e) {
      throw new OnRecordErrorException(Errors.COLLECTD_03, e.toString());
    }
  }

  private boolean verifySha1Sum(byte[] decryptedPayload) throws OnRecordErrorException {
    boolean checksumValid = false;

    try {
      MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
      sha1.update(decryptedPayload, 20, decryptedPayload.length - 20);

      byte[] includedSha1 = Arrays.copyOfRange(decryptedPayload, 0, 20);
      byte[] digest = sha1.digest();

      if (Arrays.equals(digest, includedSha1)) {
        checksumValid = true;
      }
    } catch (NoSuchAlgorithmException e) {
      throw new OnRecordErrorException(Errors.COLLECTD_03, "Could not load SHA-1 digest algorithm.");
    }

    return checksumValid;
  }
}
