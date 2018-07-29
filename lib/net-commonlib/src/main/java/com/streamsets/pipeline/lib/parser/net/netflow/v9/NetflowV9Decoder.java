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

package com.streamsets.pipeline.lib.parser.net.netflow.v9;

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.net.netflow.Errors;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowCommonDecoder;
import com.streamsets.pipeline.lib.parser.net.netflow.OutputValuesMode;
import com.streamsets.pipeline.lib.parser.net.netflow.VersionSpecificNetflowDecoder;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class NetflowV9Decoder implements VersionSpecificNetflowDecoder<NetflowV9Message> {
  private static final Logger LOG = LoggerFactory.getLogger(NetflowV9Decoder.class);
  private static final int V9_HEADER_SIZE = 20;
  private final NetflowCommonDecoder parentDecoder;
  private final OutputValuesMode outputValuesMode;

  // BEGIN state vars
  private boolean readHeader = false;
  private Integer count = null;
  private Long sysUptimeMs = null;
  private Long unixSeconds = null;
  private Long packetSequenceNum = null;
  private byte[] sourceIdBytes = null;
  private long sourceId = 0;

  private long timestamp = 0;

  private short engineType = 0;
  private short engineId = 0;
  private int sampling = 0;
  private int samplingInterval = 0;
  private int samplingMode = 0;
  private UUID packetId = null;
  private String readerId = null;
  private int readIndex = 0;

  private Integer currentFlowsetId = null;

  // vars for reading a template flowset

  // field templates (completely parsed)
  private List<NetflowV9FieldTemplate> currentTemplateFields = null;
  // number of fields in this template
  private Integer currentTemplateFieldCount = null;
  // current index of field being parsed from template
  private int currentTemplateFieldInd = -1;

  // vars for reading specific field in a template flowset

  // length (in bytes) of current template flowset
  private Integer currentTemplateLength = null;
  // number of bytes left to read for complete parsing of current template flowset
  private int currentTemplateBytesToRead = -1;
  // ID of current template flowset
  private Integer currentTemplateId = null;

  // current field type within single field template
  private Integer currentFieldType = null;
  // current field length within single field template
  private Integer currentFieldLength = null;

  // vars for reading a data flowset

  private Integer currentDataFlowLength = null;
  private Integer currentDataFlowBytesToRead = null;
  private int currentDataFlowFieldInd = -1;
  private List<NetflowV9Field> currentDataFlowFields = null;

  // vars for reading an options template flowset
  private List<NetflowV9FieldTemplate> currentOptionsTemplateFields = null;

  private Integer optionsTemplateScopeLength = null;
  private Integer optionsTemplateFieldsLength = null;

  private byte[] currentRawBytes = null;
  private Integer currentRawBytesIndex = null;


  private final List<NetflowV9Message> result = new LinkedList<>();
  // END state vars

  private final Cache<FlowSetTemplateCacheKey, FlowSetTemplate> flowSetTemplateCache;

  public NetflowV9Decoder(
      NetflowCommonDecoder parentDecoder,
      OutputValuesMode outputValuesMode,
      int maxTemplateCacheSize,
      int templateCacheTimeoutMs
  ) {
    this(parentDecoder, outputValuesMode, () -> buildTemplateCache(maxTemplateCacheSize, templateCacheTimeoutMs));
  }

  public NetflowV9Decoder(
      NetflowCommonDecoder parentDecoder,
      OutputValuesMode outputValuesMode,
      NetflowV9TemplateCacheProvider templateCacheProvider

  ) {
    this.parentDecoder = parentDecoder;
    this.outputValuesMode = outputValuesMode;
    flowSetTemplateCache = templateCacheProvider.getFlowSetTemplateCache();
  }

  public static Cache<FlowSetTemplateCacheKey, FlowSetTemplate> buildTemplateCache(
      int maxTemplateCacheSize,
      int templateCacheTimeoutMs
  ) {
    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
    if (maxTemplateCacheSize > 0) {
      cacheBuilder = cacheBuilder.maximumSize(maxTemplateCacheSize);
    }
    if (templateCacheTimeoutMs > 0) {
      cacheBuilder = cacheBuilder.expireAfterAccess(templateCacheTimeoutMs, TimeUnit.MILLISECONDS);
    }
    if (LOG.isTraceEnabled()) {
      cacheBuilder = cacheBuilder.removalListener((notification) -> LOG.trace(
          "Removing flow set template entry {} for cause: {} ",
          notification.getKey(),
          notification.getCause()
      ));
    }
    return cacheBuilder.build();
  }

  @Override
  public List<NetflowV9Message> parse(
      int netflowVersion,
      int packetLength,
      boolean packetLengthCheck,
      ByteBuf buf,
      InetSocketAddress sender,
      InetSocketAddress recipient
  ) throws OnRecordErrorException {
    if (!readHeader) {
      if (count == null) {
        // 2-3
        count = readUnsignedShortAndCheckpoint(buf);
      }
      if (count <= 0) {
        throw new OnRecordErrorException(Errors.NETFLOW_01, Utils.format("Count is invalid: {}", count));
      }
      // we cannot perform the packet length validation in Netflow v9, since the set records (template and data) can
      // be variable sizes, so we don't know up front if there are enough bytes to be certain we can read count # of
      // sets

      if (sysUptimeMs == null) {
        // 4-7
        sysUptimeMs = readUnsignedIntAndCheckpoint(buf);
      }
      if (unixSeconds == null) {
        // 8-11
        unixSeconds = readUnsignedIntAndCheckpoint(buf);
      }
      if (packetSequenceNum == null) {
        // 12-15
        packetSequenceNum = readUnsignedIntAndCheckpoint(buf);
      }
      if (sourceIdBytes == null) {
        // 16-19
        sourceIdBytes = readBytesAndCheckpoint(buf, 4);
        sourceId = Ints.fromByteArray(sourceIdBytes);
      }

      readHeader = true;
      parentDecoder.doCheckpoint();
    }

    while (readIndex < count) {
      //ByteBuf flowBuf = buf.slice(V5_HEADER_SIZE + (readIndex * V5_FLOW_SIZE), V5_FLOW_SIZE);

      if (currentFlowsetId == null) {
        currentFlowsetId = readUnsignedShortAndCheckpoint(buf);
      }

      if (currentFlowsetId < 255) {
        if (currentTemplateLength == null) {
          currentTemplateLength = readUnsignedShortAndCheckpoint(buf);
          currentTemplateBytesToRead = currentTemplateLength - 4;
        }

        while (currentTemplateBytesToRead > 0) {
          if (currentTemplateId == null) {
            currentTemplateId = readUnsignedShortAndCheckpoint(buf);
            currentTemplateBytesToRead -= 2;
          }
          // template or options
          switch (currentFlowsetId) {
            case 0:
              // flowset template
              if (currentTemplateFieldCount == null) {
                currentTemplateFieldCount = readUnsignedShortAndCheckpoint(buf);
                currentTemplateBytesToRead -= 2;
                currentTemplateFields = new LinkedList<>();
                currentTemplateFieldInd = 0;
              }
              while (currentTemplateFieldInd < currentTemplateFieldCount) {
                if (currentFieldType == null) {
                  currentFieldType = readUnsignedShortAndCheckpoint(buf);
                  currentTemplateBytesToRead -= 2;
                }
                if (currentFieldLength == null) {
                  currentFieldLength = readUnsignedShortAndCheckpoint(buf);
                  currentTemplateBytesToRead -= 2;
                }
                final NetflowV9FieldTemplate fieldTemplate = new NetflowV9FieldTemplate(
                    currentFieldType,
                    currentFieldLength
                );
                currentTemplateFields.add(fieldTemplate);
                currentTemplateFieldInd++;
                currentFieldType = null;
                currentFieldLength = null;
              }

              final FlowSetTemplate template = new FlowSetTemplate(
                  FlowKind.FLOWSET,
                  currentTemplateId,
                  currentTemplateFields
              );
              final FlowSetTemplateCacheKey flowsetTemplateCacheKey = new FlowSetTemplateCacheKey(
                  FlowKind.FLOWSET,
                  sourceIdBytes,
                  sender,
                  currentTemplateId
              );
              flowSetTemplateCache.put(flowsetTemplateCacheKey, template);
              if (LOG.isTraceEnabled()) {
                LOG.trace(
                    "Cached new flowset template {} with {} fields",
                    flowsetTemplateCacheKey.toString(),
                    currentTemplateFieldCount
                );
              }

              readIndex++;
              currentTemplateFieldCount = null;
              currentTemplateFields = null;
              currentTemplateFieldInd = -1;
              currentTemplateId = null;
              break;
            case 1:
              // options template
              if (optionsTemplateScopeLength == null) {
                optionsTemplateScopeLength = readUnsignedShortAndCheckpoint(buf);
                currentOptionsTemplateFields = new LinkedList<>();
                currentTemplateBytesToRead -= 2;
              }
              if (optionsTemplateFieldsLength == null) {
                optionsTemplateFieldsLength = readUnsignedShortAndCheckpoint(buf);
                currentTemplateBytesToRead -= 2;
              }
              while (currentTemplateBytesToRead > 0) {
                boolean finishedScopeFields = optionsTemplateScopeLength <= 0;

                if (currentFieldType == null) {
                  currentFieldType = readUnsignedShortAndCheckpoint(buf);
                  currentTemplateBytesToRead -= 2;
                }
                if (currentFieldLength == null) {
                  currentFieldLength = readUnsignedShortAndCheckpoint(buf);
                  currentTemplateBytesToRead -= 2;
                }

                NetflowV9FieldTemplate optionsFieldTemplate;
                if (finishedScopeFields) {
                  optionsFieldTemplate = new NetflowV9FieldTemplate(
                      currentFieldType,
                      currentFieldLength
                  );
                  optionsTemplateFieldsLength -= 4;
                } else {
                  optionsFieldTemplate = NetflowV9FieldTemplate.getScopeFieldTemplate(
                      currentFieldType,
                      currentFieldLength
                  );
                  optionsTemplateScopeLength -= 4;
                }
                currentOptionsTemplateFields.add(optionsFieldTemplate);
                currentFieldType = null;
                currentFieldLength = null;
              }

              final FlowSetTemplate optionsTemplate = new FlowSetTemplate(
                  FlowKind.OPTIONS,
                  currentTemplateId,
                  currentTemplateFields
              );
              flowSetTemplateCache.put(new FlowSetTemplateCacheKey(
                  FlowKind.OPTIONS,
                  sourceIdBytes,
                  sender,
                  currentTemplateId
              ), optionsTemplate);

              currentOptionsTemplateFields = null;
              optionsTemplateScopeLength = null;
              optionsTemplateFieldsLength = null;
              break;
            default:
              throw new OnRecordErrorException(Errors.NETFLOW_10, currentFlowsetId);
          }
        }

        // done with current template flowset
        currentTemplateId = null;
        currentTemplateLength = null;
        currentTemplateBytesToRead = -1;

      } else {
        // data flowset
        // in this case, the currentFlowsetId is the templateId
        final int templateId = currentFlowsetId;

        final FlowSetTemplate template = flowSetTemplateCache.getIfPresent(new FlowSetTemplateCacheKey(
            FlowKind.FLOWSET,
            sourceIdBytes,
            sender,
            templateId
        ));

        if (template == null) {
          // TODO: handle this case (capture/save the bytes until the template is received, as per section 9 here: https://www.ietf.org/rfc/rfc3954.txt
          throw new OnRecordErrorException(Errors.NETFLOW_11, currentFlowsetId);
        }

        if (currentDataFlowLength == null) {
          currentDataFlowLength = readUnsignedShortAndCheckpoint(buf);
          currentDataFlowBytesToRead = currentDataFlowLength - 4;
        }
        while (currentDataFlowBytesToRead > 0) {
          if (currentDataFlowFieldInd < 0 && currentDataFlowBytesToRead < template.getTotalFieldsLength()) {
            // we aren't in the middle of parsing a field (currentDataFlowFieldInd < 0)
            // AND there isn't enough data left for a complete record (2nd clause)
            // so this must be padding; just skip it
            readBytesAndCheckpoint(buf, currentDataFlowBytesToRead);
            break;
          }
          if (currentDataFlowFields == null) {
            currentDataFlowFields = new LinkedList<>();
            currentDataFlowFieldInd = 0;
          }

          final List<NetflowV9FieldTemplate> fieldTemplates = template.getFieldTemplates();
          final int numDataFlowFields = fieldTemplates.size();
          while (currentDataFlowFieldInd < numDataFlowFields) {
            NetflowV9FieldTemplate fieldTemplate = fieldTemplates.get(currentDataFlowFieldInd);
            NetflowV9Field field = decodeField(buf, fieldTemplate, outputValuesMode);
            currentDataFlowFields.add(field);
            currentDataFlowBytesToRead -= fieldTemplate.getLength();
            // done reading a single field
            currentDataFlowFieldInd++;
          }
          // done reading a flow record

          NetflowV9Message msg = new NetflowV9Message();
          msg.setSender(sender);
          msg.setRecipient(recipient);
          msg.setFlowKind(FlowKind.FLOWSET);
          msg.setOutputValuesMode(outputValuesMode);

          // header fields
          msg.setFlowRecordCount(count);
          msg.setSystemUptimeMs(sysUptimeMs);
          msg.setUnixSeconds(unixSeconds);
          msg.setSequenceNumber(packetSequenceNum);
          msg.setSourceId(sourceId);
          msg.setSourceIdBytes(sourceIdBytes);

          // data fields
          msg.setFields(currentDataFlowFields);
          msg.setFlowTemplateId(templateId);

          result.add(msg);
          readIndex++;
          currentDataFlowFields = null;
          currentDataFlowFieldInd = -1;

        }
        // done reading all flow records
        currentDataFlowLength = null;
      }
      // done with this flowset, whichver type it was
      parentDecoder.doCheckpoint();
      currentFlowsetId = null;
    }

    // if we reached this point without any further Signal errors, we have finished consuming
    //checkpoint();
    LinkedList<NetflowV9Message> returnResults = new LinkedList<>(result);
    resetState();
    return returnResults;
  }

  private int readUnsignedShortAndCheckpoint(ByteBuf buf) {
    final int val = buf.readUnsignedShort();
    parentDecoder.doCheckpoint();
    return val;
  }

  private long readUnsignedIntAndCheckpoint(ByteBuf buf) {
    final long val = buf.readUnsignedInt();
    parentDecoder.doCheckpoint();
    return val;
  }

  private byte[] readBytesAndCheckpoint(ByteBuf buf, int size) {
    if (currentRawBytesIndex == null) {
      currentRawBytesIndex = 0;
      currentRawBytes = new byte[size];
    }
    while (currentRawBytesIndex < size) {
      buf.readBytes(currentRawBytes, currentRawBytesIndex, 1);
      parentDecoder.doCheckpoint();
      currentRawBytesIndex++;
    }
    currentRawBytesIndex = null;
    return currentRawBytes;
  }

  public NetflowV9Field decodeField(
      ByteBuf byteBuf,
      NetflowV9FieldTemplate fieldTemplate,
      OutputValuesMode outputValuesMode) throws OnRecordErrorException {

    NetflowV9FieldType type = fieldTemplate.getType();
    int length = fieldTemplate.getLength();

    byte[] rawBytes = readBytesAndCheckpoint(byteBuf, length);

    Field interpretedValueField = null;

    if (outputValuesMode != OutputValuesMode.RAW_ONLY) {
      if (type == null) {
        // just use raw bytes if unable to recognize a known type
        interpretedValueField = getRawBytes(rawBytes);
      } else {
        switch (type) {
          case IN_BYTES:
          case IN_PKTS:
          case FLOWS:
            interpretedValueField = getArbitraryLengthPositiveIntegralFromBytes(rawBytes);
            break;
          case PROTOCOL:
          case SRC_TOS:
          case TCP_FLAGS:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case L4_SRC_PORT:
          case L4_DST_PORT:
            interpretedValueField = getUnsignedShortField(rawBytes);
            break;
          case SRC_MASK:
          case DST_MASK:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case INPUT_SNMP:
          case OUTPUT_SNMP:
            interpretedValueField = getArbitraryLengthPositiveIntegralFromBytes(rawBytes);
            break;
          case IPV4_SRC_ADDR:
          case IPV4_DST_ADDR:
          case IPV4_NEXT_HOP:
          case BGP_IPV4_NEXT_HOP:
            interpretedValueField = Field.create(NetflowCommonDecoder.getIpV4Address(rawBytes));
            break;
          case SRC_AS:
          case DST_AS:
            interpretedValueField = getArbitraryLengthPositiveIntegralFromBytes(rawBytes);
            break;
          case MUL_DST_PKTS:
          case MUL_DST_BYTES:
            interpretedValueField = getArbitraryLengthPositiveIntegralFromBytes(rawBytes);
            break;
          case LAST_SWITCHED:
          case FIRST_SWITCHED:
            interpretedValueField = getUnsignedIntField(rawBytes);
            break;
          case OUT_BYTES:
          case OUT_PKTS:
            interpretedValueField = getArbitraryLengthPositiveIntegralFromBytes(rawBytes);
            break;
          case MIN_PKT_LNGTH:
          case MAX_PKT_LNGTH:
            interpretedValueField = getUnsignedShortField(rawBytes);
            break;
          case IPV6_SRC_ADDR:
          case IPV6_DST_ADDR:
          case IPV6_NEXT_HOP:
          case BGP_IPV6_NEXT_HOP:
            interpretedValueField = getIPV6AddressAsString(rawBytes);
            break;
          case IPV6_SRC_MASK:
          case IPV6_DST_MASK:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case IPV6_FLOW_LABEL:
            interpretedValueField = getRawBytes(rawBytes);
            break;
          case ICMP_TYPE:
            interpretedValueField = getUnsignedShortField(rawBytes);
            break;
          case MUL_IGMP_TYPE:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case SAMPLING_INTERVAL:
            interpretedValueField = getUnsignedIntField(rawBytes);
            break;
          case SAMPLING_ALGORITHM:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case FLOW_ACTIVE_TIMEOUT:
          case FLOW_INACTIVE_TIMEOUT:
            interpretedValueField = getUnsignedShortField(rawBytes);
            break;
          case ENGINE_TYPE:
          case ENGINE_ID:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case TOTAL_BYTES_EXP:
          case TOTAL_PKTS_EXP:
          case TOTAL_FLOWS_EXP:
            interpretedValueField = getArbitraryLengthPositiveIntegralFromBytes(rawBytes);
            break;
          case IPV4_SRC_PREFIX:
          case IPV4_DST_PREFIX:
            interpretedValueField = getUnsignedIntField(rawBytes);
            break;
          case MPLS_TOP_LABEL_TYPE:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case MPLS_TOP_LABEL_IP_ADDR:
            interpretedValueField = getUnsignedIntField(rawBytes);
            break;
          case FLOW_SAMPLER_ID:
          case FLOW_SAMPLER_MODE:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case FLOW_SAMPLER_RANDOM_INTERVAL:
            interpretedValueField = getUnsignedIntField(rawBytes);
            break;
          case MIN_TTL:
          case MAX_TTL:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case IPV4_IDENT:
            interpretedValueField = getUnsignedShortField(rawBytes);
            break;
          case DST_TOS:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case IN_SRC_MAC:
          case OUT_DST_MAC:
          case IN_DST_MAC:
          case OUT_SRC_MAC:
            interpretedValueField = getMacAddress(rawBytes);
            break;
          case SRC_VLAN:
          case DST_VLAN:
            interpretedValueField = getUnsignedShortField(rawBytes);
            break;
          case IP_PROTOCOL_VERSION:
          case DIRECTION:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case IPV6_OPTION_HEADERS:
            interpretedValueField = getUnsignedIntField(rawBytes);
            break;
          case MPLS_LABEL_1:
          case MPLS_LABEL_2:
          case MPLS_LABEL_3:
          case MPLS_LABEL_4:
          case MPLS_LABEL_5:
          case MPLS_LABEL_6:
          case MPLS_LABEL_7:
          case MPLS_LABEL_8:
          case MPLS_LABEL_9:
          case MPLS_LABEL_10:
            interpretedValueField = getRawBytes(rawBytes);
            break;
          case IF_NAME:
          case IF_DESC:
          case SAMPLER_NAME:
            interpretedValueField = getString(rawBytes);
            break;
          case IN_PERMANENT_BYTES:
          case IN_PERMANENT_PKTS:
            interpretedValueField = getArbitraryLengthPositiveIntegralFromBytes(rawBytes);
            break;
          case FRAGMENT_OFFSET:
            interpretedValueField = getUnsignedShortField(rawBytes);
            break;
          case FORWARDING_STATUS:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case MPLS_PAL_RD:
            interpretedValueField = getRawBytes(rawBytes);
            break;
          case MPLS_PREFIX_LEN:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case SRC_TRAFFIC_INDEX:
          case DST_TRAFFIC_INDEX:
            interpretedValueField = getUnsignedIntField(rawBytes);
            break;
          case APPLICATION_DESCRIPTION:
            interpretedValueField = getString(rawBytes);
            break;
          case APPLICATION_TAG:
            interpretedValueField = getRawBytes(rawBytes);
            break;
          case APPLICATION_NAME:
            interpretedValueField = getString(rawBytes);
            break;
          case POSTIP_DIFF_SERV_CODE_POINTS:
            interpretedValueField = getUnsignedByteField(fieldTemplate.getTypeId(), rawBytes);
            break;
          case REPLICATION_FACTOR:
            interpretedValueField = getUnsignedIntField(rawBytes);
            break;
          case LAYER2_PACKET_SECTION_OFFSET:
          case LAYER2_PACKET_SECTION_SIZE:
          case LAYER2_PACKET_SECTION_DATA:
            interpretedValueField = getRawBytes(rawBytes);
            break;
          default:
            LOG.error("Type {} missing from switch in NetflowV9Decoder decodeField method", type.name());
            interpretedValueField = getRawBytes(rawBytes);
            break;
        }
      }
    }

    return new NetflowV9Field(fieldTemplate, rawBytes, interpretedValueField);
  }

  public static Field getArbitraryLengthPositiveIntegralFromBytes(byte[] bytes) {
    final BigInteger bigInt = new BigInteger(1, bytes);
    return Field.create(new BigDecimal(bigInt));
  }

  public static Field getUnsignedByteField(int typeId, byte[] bytes) throws OnRecordErrorException {
    if (bytes.length != 1) {
      throw new OnRecordErrorException(Errors.NETFLOW_12, typeId, bytes.length);
    }

    return Field.create(getUnsignedByte(bytes[0]));
  }

  /**
   * Adapted from https://stackoverflow.com/a/4266881/375670
   * @param b the single Java byte (signed)
   * @return the unsigned equivaent, as an int
   */
  public static int getUnsignedByte(byte b) {
    return b & 0xFF;
  }

  public static Field getIPV6AddressAsString(byte[] bytes) throws OnRecordErrorException {
    try {
      InetAddress addr = Inet6Address.getByAddress(bytes);
      return Field.create(addr.getHostAddress());
    } catch (UnknownHostException e) {
      throw new OnRecordErrorException(Errors.NETFLOW_13, e.getClass().getSimpleName(), e.getMessage(), e);
    }
  }

  public static Field getUnsignedIntField(byte[] bytes) {
    Utils.checkState(bytes.length == 4, "4 bytes required to parse an unsigned int");
    return Field.create(getUnsignedInt(bytes));
  }

  /**
   * Adapted from https://stackoverflow.com/a/7932774/375670
   * @param bytes the raw bytes to interpret as an unsigned short
   * @return an INTEGER field with the unsigned short value
   */
  public static Field getUnsignedShortField(byte[] bytes) {
    Utils.checkState(bytes.length == 2, "2 bytes required to parse an unsigned short");
    final short shortVal = Shorts.fromByteArray(bytes);
    int intVal = shortVal >= 0 ? shortVal : 0x10000 + shortVal;
    return Field.create(intVal);
  }

  /**
   * Adapted from https://stackoverflow.com/a/1576404/375670
   *
   * @param bytes
   * @return
   */
  private static long getUnsignedInt(byte[] bytes) {
    final int intVal = Ints.fromByteArray(bytes);
    return intVal & 0xFFFFFFFFL;
  }

  public static Field getMacAddress(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      sb.append(String.format("%02X", bytes[i]));
      if (i < bytes.length - 1) {
        sb.append(":");
      }
    }
    return Field.create(sb.toString());
  }

  public static Field getString(byte[] bytes) {
    // the character for these fields is not discussed in any known documentation; we use UTF-8 (which handles
    // single byte ASCII encoding transparently) until proven inadequate
    return Field.create(new String(bytes, Charsets.UTF_8));
  }

  public static Field getRawBytes(byte[] bytes) {
    return Field.create(bytes);
  }

  @Override
  public void resetState() {
    readHeader = false;
    count = null;
    sysUptimeMs = null;
    unixSeconds = null;
    timestamp = 0;
    packetSequenceNum = null;
    sourceIdBytes = null;
    engineType = 0;
    engineId = 0;
    sampling = 0;
    samplingInterval = 0;
    samplingMode = 0;
    packetId = null;
    readerId = null;
    readIndex = 0;
    sourceId = 0;

    currentFlowsetId = null;
    currentTemplateFields = null;
    currentTemplateFieldCount = null;
    currentTemplateFieldInd = -1;
    currentTemplateLength = null;
    currentTemplateBytesToRead = -1;
    currentTemplateId = null;
    currentFieldType = null;
    currentFieldLength = null;
    currentDataFlowLength = null;
    currentDataFlowBytesToRead = null;
    currentDataFlowFieldInd = -1;
    currentDataFlowFields = null;

    currentOptionsTemplateFields = null;
    optionsTemplateScopeLength = null;
    optionsTemplateFieldsLength = null;

    currentRawBytes = null;
    currentRawBytesIndex = null;

    result.clear();
  }
}
