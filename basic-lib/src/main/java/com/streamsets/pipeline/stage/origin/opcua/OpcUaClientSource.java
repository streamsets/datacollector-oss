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
package com.streamsets.pipeline.stage.origin.opcua;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.util.EscapeUtil;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.apache.directory.api.util.Strings;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfigBuilder;
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.client.UaTcpStackClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.channel.ChannelConfig;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UByte;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.ULong;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseDirection;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseResultMask;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseResult;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static org.eclipse.milo.opcua.stack.core.util.ConversionUtil.toList;

public class OpcUaClientSource implements PushSource {

  private static final Logger LOG = LoggerFactory.getLogger(OpcUaClientSource.class);
  private static final String SSL_CONFIG_PREFIX = "conf.tlsConfig.";
  private static final String SOURCE_TIME_FIELD_NAME = "sourceTime";
  private static final String SOURCE_PICO_SECONDS_FIELD_NAME = "sourcePicoSeconds";
  private static final String SERVER_TIME_FIELD_NAME = "serverTime";
  private static final String SERVER_PICO_SECONDS_FIELD_NAME = "serverPicoSeconds";
  private static final String STATUS_CODE_FIELD_NAME = "statusCode";
  private static final String STATUS_CODE_STR_FIELD_NAME = "statusCodeStr";
  private static final String VALUE_FIELD_NAME = "value";
  private final OpcUaClientSourceConfigBean conf;
  private Context context;
  private AtomicLong counter = new AtomicLong();
  private BlockingQueue<Exception> errorQueue;
  private List<Exception> errorList;
  private final AtomicLong clientHandles = new AtomicLong(1L);
  private OpcUaClient opcUaClient;
  private List<NodeId> nodeIds;
  private NodeId rootNodeId;
  private volatile boolean destroyed = false;
  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private Stopwatch stopwatch = null;

  OpcUaClientSource(OpcUaClientSourceConfigBean conf) {
    this.conf = conf;
  }

  @Override
  public int getNumberOfThreads() {
    return 1;
  }

  @Override
  public List<ConfigIssue> init(Info info, Context context) {
    List<ConfigIssue> issues = new ArrayList<>();
    this.context = context;
    errorQueue = new ArrayBlockingQueue<>(100);
    errorList = new ArrayList<>(100);

    SecurityPolicy securityPolicy = conf.securityPolicy.getSecurityPolicy();
    if (!securityPolicy.equals(SecurityPolicy.None) && !conf.tlsConfig.isEnabled()) {
      issues.add(
          context.createConfigIssue(
              Groups.SECURITY.name(),
              SSL_CONFIG_PREFIX + "tlsEnabled",
              Errors.OPC_UA_06,
              conf.securityPolicy.getLabel()
          )
      );
      return issues;
    }

    if (conf.tlsConfig.isEnabled()) {
      conf.tlsConfig.init(
          context,
          Groups.SECURITY.name(),
          SSL_CONFIG_PREFIX,
          issues
      );
    }

    try {
      opcUaClient = createClient();
      opcUaClient.connect().get();
    } catch (Exception ex) {
      issues.add(
          context.createConfigIssue(
              Groups.OPC_UA.name(),
              "conf.resourceUrl",
              Errors.OPC_UA_02,
              ex.getLocalizedMessage()
          )
      );
    }

    if (conf.readMode != OpcUaReadMode.BROWSE_NODES) {
      initializeNodeIds(issues);
    }

    if (conf.nodeIdConfigs.isEmpty() && conf.readMode != OpcUaReadMode.BROWSE_NODES) {
      issues.add(
          context.createConfigIssue(
              Groups.NODE_IDS.name(),
              "conf.nodeIdConfigs",
              Errors.OPC_UA_03
          )
      );
    }

    return issues;
  }

  private OpcUaClient createClient() throws Exception {
    SecurityPolicy securityPolicy = conf.securityPolicy.getSecurityPolicy();

    EndpointDescription[] endpoints = UaTcpStackClient.getEndpoints(conf.resourceUrl).get();

    EndpointDescription endpoint = Arrays.stream(endpoints)
        .filter(e -> e.getSecurityPolicyUri().equals(securityPolicy.getSecurityPolicyUri()))
        .findFirst().orElseThrow(() -> new StageException(Errors.OPC_UA_01));

    ChannelConfig channelConfig = new ChannelConfig(
        conf.channelConf.maxChunkSize,
        conf.channelConf.maxChunkCount,
        conf.channelConf.maxMessageSize,
        conf.channelConf.maxArrayLength,
        conf.channelConf.maxStringLength
    );
    OpcUaClientConfigBuilder clientConfigBuilder = OpcUaClientConfig.builder()
        .setApplicationName(LocalizedText.english(conf.applicationName))
        .setApplicationUri(conf.applicationUri)
        .setChannelConfig(channelConfig);

    if (!securityPolicy.equals(SecurityPolicy.None)) {
      KeyStore keyStore = conf.tlsConfig.getKeyStore();
      if (keyStore != null) {
        Key clientPrivateKey = keyStore.getKey(conf.clientKeyAlias, conf.tlsConfig.keyStorePassword.get().toCharArray());
        if (clientPrivateKey instanceof PrivateKey) {
          X509Certificate clientCertificate = (X509Certificate) keyStore.getCertificate(conf.clientKeyAlias);
          PublicKey clientPublicKey = clientCertificate.getPublicKey();
          KeyPair clientKeyPair = new KeyPair(clientPublicKey, (PrivateKey) clientPrivateKey);
          clientConfigBuilder.setCertificate(clientCertificate)
              .setKeyPair(clientKeyPair);
        }
      }
    }

    OpcUaClientConfig config = clientConfigBuilder.setEndpoint(endpoint)
        .setIdentityProvider(new AnonymousProvider())
        .setRequestTimeout(uint(conf.requestTimeoutMillis))
        .setSessionTimeout(uint(conf.sessionTimeoutMillis))
        .build();

    return new OpcUaClient(config);
  }

  @Override
  public void produce(Map<String, String> map, int i) throws StageException {
    try {
      switch (conf.readMode) {
        case POLLING:
          pollForData();
          break;
        case SUBSCRIBE:
          subscribeForData();
          break;
        case BROWSE_NODES:
          browseNodes();
          dispatchReceiverErrors(0);
          return;
      }
      while (!context.isStopped()) {
        dispatchReceiverErrors(100);
      }

    } catch(Exception me) {
      throw new StageException(Errors.OPC_UA_02, me, me);
    }
  }

  private void initializeNodeIds(List<ConfigIssue> issues) {
    try {
      nodeIds = new ArrayList<>();

      if (this.conf.nodeIdFetchMode.equals(NodeIdFetchMode.BROWSE)) {
        rootNodeId = Identifiers.RootFolder;
        switch (conf.rootIdentifierType) {
          case NUMERIC:
            rootNodeId = new NodeId(conf.rootNamespaceIndex, Integer.parseInt(conf.rootIdentifier));
            break;
          case STRING:
            rootNodeId = new NodeId(conf.rootNamespaceIndex, conf.rootIdentifier);
            break;
          case UUID:
            rootNodeId = new NodeId(conf.rootNamespaceIndex, UUID.fromString(conf.rootIdentifier));
            break;
          case OPAQUE:
            rootNodeId = new NodeId(conf.rootNamespaceIndex, new ByteString(conf.rootIdentifier.getBytes()));
            break;
        }
        conf.nodeIdConfigs = new ArrayList<>();
        browseNode(rootNodeId, nodeIds, issues, new HashMap<>());
        if (nodeIds.isEmpty()) {
          issues.add(
              context.createConfigIssue(
                  Groups.NODE_IDS.name(),
                  "conf.rootIdentifier",
                  Errors.OPC_UA_07
              )
          );
        }

      } else {
        if (this.conf.nodeIdFetchMode.equals(NodeIdFetchMode.FILE)) {
          try {
            conf.nodeIdConfigs = Arrays.asList(
                OBJECT_MAPPER.readValue(this.conf.nodeIdConfigsFilePath,NodeIdConfig[].class)
            );
          } catch (Exception ex) {
            issues.add(context.createConfigIssue(
                Groups.NODE_IDS.name(),
                "conf.nodeIdConfigsFilePath",
                Errors.OPC_UA_04,
                ex.getLocalizedMessage()
            ));
            return;
          }
        }

        for (NodeIdConfig nodeIdConfig: conf.nodeIdConfigs) {
          if (this.conf.nodeIdFetchMode.equals(NodeIdFetchMode.FILE)) {
            if (Strings.isEmpty(nodeIdConfig.field) || Strings.isEmpty(nodeIdConfig.identifier)) {
              issues.add(context.createConfigIssue(
                  Groups.NODE_IDS.name(),
                  "conf.nodeIdConfigsFilePath",
                  Errors.OPC_UA_11,
                  nodeIdConfig
              ));
              break;
            }
          }
          switch (nodeIdConfig.identifierType) {
            case NUMERIC:
              nodeIds.add(new NodeId(nodeIdConfig.namespaceIndex, Integer.parseInt(nodeIdConfig.identifier)));
              break;
            case STRING:
              nodeIds.add(new NodeId(nodeIdConfig.namespaceIndex, nodeIdConfig.identifier));
              break;
            case UUID:
              nodeIds.add(new NodeId(nodeIdConfig.namespaceIndex, UUID.fromString(nodeIdConfig.identifier)));
              break;
            case OPAQUE:
              nodeIds.add(new NodeId(nodeIdConfig.namespaceIndex, new ByteString(nodeIdConfig.identifier.getBytes())));
              break;
          }
        }
      }
    } catch (Exception ex) {
      issues.add(
          context.createConfigIssue(
              Groups.NODE_IDS.name(),
              "conf.nodeIdConfigs",
              Errors.OPC_UA_04,
              ex.getLocalizedMessage()
          )
      );
    }
  }

  private void refreshNodeIds() {
    if (this.conf.nodeIdFetchMode.equals(NodeIdFetchMode.BROWSE)) {
      if (stopwatch == null) {
        stopwatch = Stopwatch.createStarted();
      } else if (stopwatch.elapsed(TimeUnit.SECONDS) > conf.refreshNodeIdsInterval) {
        try {
          List<NodeId> newNodeIds = new ArrayList<>();
          conf.nodeIdConfigs = new ArrayList<>();
          browseNode(rootNodeId, newNodeIds, new ArrayList<>(), new HashMap<>());
          if (!newNodeIds.isEmpty()) {
            nodeIds = newNodeIds;
          }
        } catch (Exception ex) {
          errorQueue.offer(new StageException(Errors.OPC_UA_08, ex.getMessage(), ex));
          reConnect();
        } finally {
          stopwatch.reset()
              .start();
        }
      }
    }
  }

  private void reConnect() {
    try {
      opcUaClient.disconnect().get(conf.requestTimeoutMillis, TimeUnit.MILLISECONDS);
      opcUaClient = createClient();
      opcUaClient.connect().get(conf.requestTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (Exception ex) {
      LOG.error(Errors.OPC_UA_02.getMessage(), ex.getMessage(), ex);
      errorQueue.offer(new StageException(Errors.OPC_UA_02, ex.getMessage(), ex));
    }
  }

  private void pollForData() {
    if (destroyed) {
      return;
    }
    refreshNodeIds();
    opcUaClient.readValues(0.0, TimestampsToReturn.Both, nodeIds)
        .thenAccept(values -> {
          try {
            process(values, System.currentTimeMillis() + "." + counter.getAndIncrement(), null);
          } catch (Exception ex) {
            LOG.error(Errors.OPC_UA_09.getMessage(), ex.getMessage(), ex);
            errorQueue.offer(new StageException(Errors.OPC_UA_09, ex.getMessage(), ex));
          }
          if (ThreadUtil.sleep(conf.pollingInterval)) {
            pollForData();
          }
        })
        .exceptionally(ex -> {
          LOG.warn(Errors.OPC_UA_12.getMessage(), ex.getMessage());
          errorQueue.offer(new StageException(Errors.OPC_UA_12, ex.getMessage(), ex));
          reConnect();
          if (ThreadUtil.sleep(conf.pollingInterval)) {
            pollForData();
          }
          return null;
        });
  }

  private void subscribeForData() {
    try {
      UaSubscription subscription = opcUaClient.getSubscriptionManager()
          .createSubscription(1000.0).get();

      List<MonitoredItemCreateRequest> monitoredItemCreateRequests = new ArrayList<>();
      nodeIds.forEach(nodeId -> {
        ReadValueId readValueId = new ReadValueId(
            nodeId,
            AttributeId.Value.uid(),
            null,
            QualifiedName.NULL_VALUE
        );

        UInteger clientHandle = uint(clientHandles.getAndIncrement());

        MonitoringParameters parameters = new MonitoringParameters(
            clientHandle,
            1000.0,     // sampling interval
            null,       // filter, null means use default
            uint(10),   // queue size
            true        // discard oldest
        );

        MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(
            readValueId, MonitoringMode.Reporting, parameters);

        monitoredItemCreateRequests.add(request);
      });

      BiConsumer<UaMonitoredItem, Integer> onItemCreated =
          (item, id) -> item.setValueConsumer(this::onSubscriptionValue);

      List<UaMonitoredItem> items = subscription.createMonitoredItems(
          TimestampsToReturn.Both,
          monitoredItemCreateRequests,
          onItemCreated
      ).get();

      for (UaMonitoredItem item : items) {
        if (item.getStatusCode().isGood()) {
          LOG.debug("item created for nodeId={}", item.getReadValueId().getNodeId());
        } else {
          LOG.error(Errors.OPC_UA_05.getMessage(), item.getReadValueId().getNodeId(), item.getStatusCode());
          errorQueue.offer(new StageException(Errors.OPC_UA_05, item.getReadValueId().getNodeId(), item.getStatusCode()));
        }
      }

    } catch (InterruptedException | ExecutionException ex) {
      errorQueue.offer(ex);
    }
  }

  private void onSubscriptionValue(UaMonitoredItem item, DataValue value) {
    LOG.debug("subscription value received: item={}, value={}", item.getReadValueId().getNodeId(), value.getValue());
    try {
      String fieldName = EscapeUtil.singleQuoteEscape(item.getReadValueId().getNodeId().getIdentifier().toString());
      process(ImmutableList.of(value), item.getReadValueId().getNodeId() + "." + counter.getAndIncrement(), fieldName);
    } catch (Exception ex) {
      LOG.error(Errors.OPC_UA_09.getMessage(), ex.getMessage(), ex);
      errorQueue.offer(new StageException(Errors.OPC_UA_09, ex.getMessage(), ex));
    }
  }

  private void process(List<DataValue> dataValues, String recordSourceId, String subscribeFieldName) {
    BatchContext batchContext = context.startBatch();
    Record record = context.createRecord(recordSourceId);
    record.set("/", Field.create(new LinkedHashMap<>()));

    int[] idx = { 0 };
    dataValues.forEach((dataValue) -> {
      String fieldName;

      if (conf.readMode.equals(OpcUaReadMode.SUBSCRIBE)) {
        fieldName = subscribeFieldName;
      } else {
        NodeIdConfig nodeIdConfig = conf.nodeIdConfigs.get(idx[0]++);
        fieldName = nodeIdConfig.field;
      }

      if (!fieldName.startsWith("/")) {
        fieldName = "/" + fieldName;
      }

      Field.Type fieldType = Field.Type.STRING;
      Object value = dataValue.getValue().getValue();
      Optional<NodeId> nodeIdOptional =  dataValue.getValue().getDataType();
      int typeId = 12; // default to string type
      if (nodeIdOptional.isPresent()) {
        UInteger identifier = (UInteger)nodeIdOptional.get().getIdentifier();
        typeId = identifier.intValue();
      }

      boolean isArray = false;
      Object [] arrValue = null;
      if (value != null && value.getClass().isArray()) {
        isArray = true;
        arrValue = (Object []) value;
        fieldType = Field.Type.LIST;
      }

      switch (typeId) {
        case 1: // Boolean
          if (isArray) {
            List<Field> boolFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              boolFieldList.add(Field.create(Field.Type.BOOLEAN, anArrValue));
            }
            value = boolFieldList;
          } else {
            fieldType = Field.Type.BOOLEAN;
          }
          break;
        case 2: // SByte
          if (isArray) {
            List<Field> sByteFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              sByteFieldList.add(Field.create(Field.Type.BYTE, anArrValue));
            }
            value = sByteFieldList;
          } else {
            fieldType = Field.Type.BYTE;
          }
          break;
        case 3: // Byte
          if (isArray) {
            List<Field> byteFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                UByte uByte = (UByte) anArrValue;
                anArrValue = uByte.byteValue();
              }
              byteFieldList.add(Field.create(Field.Type.BYTE, anArrValue));
            }
            value = byteFieldList;
          } else {
            fieldType = Field.Type.BYTE;
            if (value != null) {
              UByte uByte = (UByte) value;
              value = uByte.byteValue();
            }
          }
          break;
        case 4: // Int16
          if (isArray) {
            List<Field> integerFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              integerFieldList.add(Field.create(Field.Type.INTEGER, anArrValue));
            }
            value = integerFieldList;
          } else {
            fieldType = Field.Type.INTEGER;
          }
          break;
        case 5: // UInt16
          if (isArray) {
            List<Field> integerFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                UShort uShort = (UShort) anArrValue;
                anArrValue = uShort.intValue();
              }
              integerFieldList.add(Field.create(Field.Type.INTEGER, anArrValue));
            }
            value = integerFieldList;
          } else {
            fieldType = Field.Type.INTEGER;
            if (value != null) {
              UShort uShort = (UShort) value;
              value = uShort.intValue();
            }
          }
          break;
        case 6: // Int32
          if (isArray) {
            List<Field> integerFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              integerFieldList.add(Field.create(Field.Type.INTEGER, anArrValue));
            }
            value = integerFieldList;
          } else {
            fieldType = Field.Type.INTEGER;
          }

          break;
        case 7: // UInt32
          if (isArray) {
            List<Field> integerFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                UInteger uInteger = (UInteger) anArrValue;
                anArrValue = uInteger.intValue();
              }
              integerFieldList.add(Field.create(Field.Type.INTEGER, anArrValue));
            }
            value = integerFieldList;
          } else {
            fieldType = Field.Type.INTEGER;
            if (value != null) {
              UInteger uInteger = (UInteger) value;
              value = uInteger.intValue();
            }
          }
          break;
        case 8: // Int64
          if (isArray) {
            List<Field> longFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              longFieldList.add(Field.create(Field.Type.LONG, anArrValue));
            }
            value = longFieldList;
          } else {
            fieldType = Field.Type.LONG;
          }
          break;
        case 9: // UInt64
          if (isArray) {
            List<Field> longFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                ULong uLong = (ULong) anArrValue;
                anArrValue = uLong.longValue();
              }
              longFieldList.add(Field.create(Field.Type.LONG, anArrValue));
            }
            value = longFieldList;
          } else {
            fieldType = Field.Type.LONG;
            if (value != null) {
              ULong uLong = (ULong) value;
              value = uLong.longValue();
            }
          }
          break;
        case 10: // Float
          if (isArray) {
            List<Field> floatFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              floatFieldList.add(Field.create(Field.Type.FLOAT, anArrValue));
            }
            value = floatFieldList;
          } else {
            fieldType = Field.Type.FLOAT;
          }
          break;
        case 11: // Double
          if (isArray) {
            List<Field> doubleFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              doubleFieldList.add(Field.create(Field.Type.DOUBLE, anArrValue));
            }
            value = doubleFieldList;
          } else {
            fieldType = Field.Type.DOUBLE;
          }
          break;
        case 12: // String
          if (isArray) {
            List<Field> stringFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              stringFieldList.add(Field.create(Field.Type.STRING, anArrValue));
            }
            value = stringFieldList;
          } else {
            fieldType = Field.Type.STRING;
          }
          break;
        case 13: // DateTime
          if (isArray) {
            List<Field> dateTimeFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                DateTime dateTime = (DateTime) anArrValue;
                anArrValue = dateTime.getJavaDate();
              }
              dateTimeFieldList.add(Field.create(Field.Type.DATETIME, anArrValue));
            }
            value = dateTimeFieldList;
          } else {
            fieldType = Field.Type.DATETIME;
            if (value != null) {
              DateTime dateTime = (DateTime) value;
              value = dateTime.getJavaDate();
            }
          }
          break;
        case 14: // Guid
          if (isArray) {
            List<Field> guidFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                anArrValue = anArrValue.toString();
              }
              guidFieldList.add(Field.create(Field.Type.STRING, anArrValue));
            }
            value = guidFieldList;
          } else {
            fieldType = Field.Type.STRING;
            if (value != null) {
              value = value.toString();
            }
          }
          break;
        case 15: // ByteString
          if (isArray) {
            List<Field> byteStringFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                ByteString byteString = (ByteString)anArrValue;
                anArrValue = byteString.bytes();
              }
              byteStringFieldList.add(Field.create(Field.Type.BYTE_ARRAY, anArrValue));
            }
            value = byteStringFieldList;
          } else {
            fieldType = Field.Type.BYTE_ARRAY;
            if (value != null) {
              ByteString byteString = (ByteString)value;
              value = byteString.bytes();
            }
          }
          break;
        default:
          if (isArray) {
            List<Field> stringFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              stringFieldList.add(Field.create(Field.Type.STRING, anArrValue));
            }
            value = stringFieldList;
          } else {
            fieldType = Field.Type.STRING;
          }
      }

      LinkedHashMap<String, Field> valueMap = new LinkedHashMap<>();

      if (dataValue.getSourceTime() != null) {
        valueMap.put(SOURCE_TIME_FIELD_NAME, Field.createDatetime(dataValue.getSourceTime().getJavaDate()));
      } else {
        valueMap.put(SOURCE_TIME_FIELD_NAME, Field.create(Field.Type.LONG, null));
      }

      if (dataValue.getSourcePicoseconds() != null) {
        valueMap.put(SOURCE_PICO_SECONDS_FIELD_NAME, Field.create(dataValue.getSourcePicoseconds().longValue()));
      } else {
        valueMap.put(SOURCE_PICO_SECONDS_FIELD_NAME, Field.create(Field.Type.LONG, 0));
      }

      if (dataValue.getServerTime() != null) {
        valueMap.put(SERVER_TIME_FIELD_NAME, Field.createDatetime(dataValue.getServerTime().getJavaDate()));
      } else {
        valueMap.put(SERVER_TIME_FIELD_NAME, Field.create(Field.Type.LONG, null));
      }

      if (dataValue.getServerPicoseconds() != null) {
        valueMap.put(SERVER_PICO_SECONDS_FIELD_NAME, Field.create(dataValue.getServerPicoseconds().longValue()));
      } else {
        valueMap.put(SERVER_PICO_SECONDS_FIELD_NAME, Field.create(Field.Type.LONG, 0));
      }

      if (dataValue.getStatusCode() != null) {
        valueMap.put(STATUS_CODE_FIELD_NAME, Field.create(dataValue.getStatusCode().getValue()));
        StatusCodes.lookup(dataValue.getStatusCode().getValue()).ifPresent(
            nameAndDesc -> valueMap.put(STATUS_CODE_STR_FIELD_NAME, Field.create(nameAndDesc[0])));
      } else {
        valueMap.put(STATUS_CODE_FIELD_NAME, Field.create(Field.Type.LONG, null));
        valueMap.put(STATUS_CODE_STR_FIELD_NAME, Field.create(Field.Type.STRING, null));
      }

      valueMap.put(VALUE_FIELD_NAME, Field.create(fieldType, value));
      record.set(fieldName, Field.createListMap(valueMap));

    });

    batchContext.getBatchMaker().addRecord(record);
    context.processBatch(batchContext);
  }

  private void dispatchReceiverErrors(long intervalMillis) {
    if (intervalMillis > 0) {
      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException ignored) {
      }
    }
    errorList.clear();
    errorQueue.drainTo(errorList);
    for (Exception exception : errorList) {
      context.reportError(exception);
    }
  }

  private void browseNode(
      NodeId browseRoot,
      List<NodeId> nodeIdList,
      List<ConfigIssue> issues,
      Map<String, Boolean> visitedMap
  ) {
    if (visitedMap.containsKey(browseRoot.getIdentifier().toString())) {
      return;
    }

    visitedMap.put(browseRoot.getIdentifier().toString(), true);

    BrowseDescription browse = new BrowseDescription(
        browseRoot,
        BrowseDirection.Forward,
        Identifiers.References,
        true,
        uint(NodeClass.Object.getValue() | NodeClass.Variable.getValue()),
        uint(BrowseResultMask.All.getValue())
    );
    try {
      BrowseResult browseResult = opcUaClient.browse(browse).get();
      List<ReferenceDescription> references = toList(browseResult.getReferences());

      for (ReferenceDescription rd : references) {
        if (visitedMap.containsKey(rd.getNodeId().getIdentifier().toString())) {
          continue;
        }

        if (rd.getNodeClass().equals(NodeClass.Variable)) {
          NodeId nodeId = null;
          NodeIdConfig nodeIdConfig = new NodeIdConfig();
          switch (rd.getNodeId().getType()) {
            case Numeric:
              nodeId = new NodeId(
                  rd.getNodeId().getNamespaceIndex().intValue(),
                  Integer.parseInt(rd.getNodeId().getIdentifier().toString())
              );
              nodeIdConfig.field = rd.getBrowseName().getName() + "_" + rd.getNodeId().getIdentifier().toString();
              break;
            case String:
              nodeId = new NodeId(
                  rd.getNodeId().getNamespaceIndex().intValue(),
                  rd.getNodeId().getIdentifier().toString()
              );
              nodeIdConfig.field = rd.getNodeId().getIdentifier().toString();
              break;
            case Guid:
              nodeId = new NodeId(
                  rd.getNodeId().getNamespaceIndex().intValue(),
                  UUID.fromString(rd.getNodeId().getIdentifier().toString())
              );
              nodeIdConfig.field = rd.getBrowseName().getName() + "_" + rd.getNodeId().getIdentifier().toString();
              break;
            case Opaque:
              nodeId = new NodeId(
                  rd.getNodeId().getNamespaceIndex().intValue(),
                  new ByteString(rd.getNodeId().getIdentifier().toString().getBytes())
              );
              nodeIdConfig.field = rd.getBrowseName().getName() + "_" + rd.getNodeId().getIdentifier().toString();
              break;
          }
          nodeIdList.add(nodeId);
          conf.nodeIdConfigs.add(nodeIdConfig);
        }

        rd.getNodeId().local().ifPresent(nodeId -> browseNode(nodeId, nodeIdList, issues, visitedMap));
      }
    } catch (InterruptedException | ExecutionException ex) {
      LOG.error("Browsing nodeId={} failed: {}", browseRoot, ex.getMessage(), ex);
      errorQueue.offer(new StageException(Errors.OPC_UA_08, ex.getMessage(), ex));
      issues.add(
          context.createConfigIssue(
              Groups.NODE_IDS.name(),
              "conf.rootIdentifier",
              Errors.OPC_UA_08,
              ex.getMessage()
          )
      );
    }
  }

  private void browseNodes() {
    LinkedHashMap<String, Field> rootFieldMap = new LinkedHashMap<>();
    Map<String, Boolean> visitedMap = new HashMap<>();
    browseNode(Identifiers.RootFolder, rootFieldMap, visitedMap);
    BatchContext batchContext = context.startBatch();
    String requestId = System.currentTimeMillis() + "." + counter.getAndIncrement();
    Record record = context.createRecord(requestId);
    record.set("/", Field.create(rootFieldMap));
    batchContext.getBatchMaker().addRecord(record);
    context.processBatch(batchContext);
  }

  private void browseNode(
      NodeId browseRoot,
      LinkedHashMap<String, Field> rootFieldMap,
      Map<String, Boolean> visitedMap
  ) {
    if (visitedMap.containsKey(browseRoot.getIdentifier().toString())) {
      return;
    }

    visitedMap.put(browseRoot.getIdentifier().toString(), true);

    BrowseDescription browse = new BrowseDescription(
        browseRoot,
        BrowseDirection.Forward,
        Identifiers.References,
        true,
        uint(NodeClass.Object.getValue() | NodeClass.Variable.getValue()),
        uint(BrowseResultMask.All.getValue())
    );

    try {
      BrowseResult browseResult = opcUaClient.browse(browse).get();
      List<ReferenceDescription> references = toList(browseResult.getReferences());

      for (ReferenceDescription rd : references) {
        if (visitedMap.containsKey(rd.getNodeId().getIdentifier().toString())) {
          continue;
        }
        LinkedHashMap<String, Field> fieldMap = new LinkedHashMap<>();
        fieldMap.put("name", Field.create(rd.getBrowseName().getName()));
        fieldMap.put("nameSpaceIndex", Field.create(rd.getNodeId().getNamespaceIndex().intValue()));
        fieldMap.put("identifierType", Field.create(rd.getNodeId().getType().name()));
        fieldMap.put("identifier", Field.create(rd.getNodeId().getIdentifier().toString()));

        // recursively browse to children
        rd.getNodeId().local().ifPresent(nodeId -> {
          if (!nodeId.equals(browseRoot)) {
            LinkedHashMap<String, Field> childrenMap = new LinkedHashMap<>();
            browseNode(nodeId, childrenMap, visitedMap);
            if (!childrenMap.isEmpty()) {
              fieldMap.put("children",  Field.create(childrenMap));
            }
          }
        });

        rootFieldMap.put(rd.getBrowseName().getName(), Field.create(fieldMap));
      }
    } catch (InterruptedException | ExecutionException ex) {
      LOG.error(Errors.OPC_UA_10.getMessage(), browseRoot, ex.getMessage(), ex);
      errorQueue.offer(new StageException(Errors.OPC_UA_10, browseRoot, ex.getMessage(), ex));
    }
  }

  @Override
  public void destroy() {
    if (opcUaClient != null) {
      try {
        destroyed = true;
        opcUaClient.disconnect().get();
      } catch (InterruptedException | ExecutionException ex) {
        LOG.error("Failed during OPC UA Client disconnect call: {}", ex.getMessage());
      }
    }
  }

}
