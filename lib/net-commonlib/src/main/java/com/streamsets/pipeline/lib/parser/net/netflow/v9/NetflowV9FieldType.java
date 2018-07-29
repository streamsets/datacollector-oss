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

import java.util.HashMap;
import java.util.Map;

public enum NetflowV9FieldType {

  // BEGIN DATA FIELD TYPES
  IN_BYTES(1, "N (default is 4)", "Incoming counter with length N x 8 bits for number of bytes associated with an IP Flow."),
  IN_PKTS(2, "N (default is 4)", "Incoming counter with length N x 8 bits for the number of packets associated with an IP Flow"),
  FLOWS(3, "N", "Number of flows that were aggregated; default for N is 4"),
  PROTOCOL(4, "1", "IP protocol byte"),
  SRC_TOS(5, "1", "Type of Service byte setting when entering incoming interface"),
  TCP_FLAGS(6, "1", "Cumulative of all the TCP flags seen for this flow"),
  L4_SRC_PORT(7, "2", "TCP/UDP source port number i.e.: FTP, Telnet,\" or equivalent\""),
  IPV4_SRC_ADDR(8, "4", "IPv4 source address"),
  SRC_MASK(9, "1", "The number of contiguous bits in the source address subnet mask i.e.: the submask in slash notation"),
  INPUT_SNMP(10, "N", "Input interface index; default for N is 2 but higher values could be used"),
  L4_DST_PORT(11, "2", "TCP/UDP destination port number i.e.: FTP, Telnet,\" or equivalent\""),
  IPV4_DST_ADDR(12, "4", "IPv4 destination address"),
  DST_MASK(13, "1", "The number of contiguous bits in the destination address subnet mask i.e.: the submask in slash notation"),
  OUTPUT_SNMP(14, "N", "Output interface index; default for N is 2 but higher values could be used"),
  IPV4_NEXT_HOP(15, "4", "IPv4 address of next-hop router"),
  SRC_AS(16, "N (default is 2)", "Source BGP autonomous system number where N could be 2 or 4"),
  DST_AS(17, "N (default is 2)", "Destination BGP autonomous system number where N could be 2 or 4"),
  BGP_IPV4_NEXT_HOP(18, "4", "Next-hop router's IP in the BGP domain"),
  MUL_DST_PKTS(19, "N (default is 4)", "IP multicast outgoing packet counter with length N x 8 bits for packets associated with the IP Flow"),
  MUL_DST_BYTES(20, "N (default is 4)", "IP multicast outgoing byte counter with length N x 8 bits for bytes associated with the IP Flow"),
  LAST_SWITCHED(21, "4", "System uptime at which the last packet of this flow was switched"),
  FIRST_SWITCHED(22, "4", "System uptime at which the first packet of this flow was switched"),
  OUT_BYTES(23, "N (default is 4)", "Outgoing counter with length N x 8 bits for the number of bytes associated with an IP Flow"),
  OUT_PKTS(24, "N (default is 4)", "Outgoing counter with length N x 8 bits for the number of packets associated with an IP Flow."),
  MIN_PKT_LNGTH(25, "2", "Minimum IP packet length on incoming packets of the flow"),
  MAX_PKT_LNGTH(26, "2", "Maximum IP packet length on incoming packets of the flow"),
  IPV6_SRC_ADDR(27, "16", "IPv6 Source Address"),
  IPV6_DST_ADDR(28, "16", "IPv6 Destination Address"),
  IPV6_SRC_MASK(29, "1", "Length of the IPv6 source mask in contiguous bits"),
  IPV6_DST_MASK(30, "1", "Length of the IPv6 destination mask in contiguous bits"),
  IPV6_FLOW_LABEL(31, "3", "IPv6 flow label as per RFC 2460 definition"),
  ICMP_TYPE(32, "2", "Internet Control Message Protocol (ICMP) packet type; reported as ((ICMP Type*256) + ICMP code)"),
  MUL_IGMP_TYPE(33, "1", "Internet Group Management Protocol (IGMP) packet type"),
  SAMPLING_INTERVAL(34, "4", "When using sampled NetFlow,\" the rate at which packets are sampled i.e.: a value of 100 indicates that one of every 100 packets is sampled\""),
  SAMPLING_ALGORITHM(35, "1", "The type of algorithm used for sampled NetFlow: 0x01 Deterministic Sampling ,\"0x02 Random Sampling\""),
  FLOW_ACTIVE_TIMEOUT(36, "2", "Timeout value (in seconds) for active flow entries in the NetFlow cache"),
  FLOW_INACTIVE_TIMEOUT(37, "2", "Timeout value (in seconds) for inactive flow entries in the NetFlow cache"),
  ENGINE_TYPE(38, "1", "Type of flow switching engine: RP = 0,\" VIP/Linecard = 1\""),
  ENGINE_ID(39, "1", "ID number of the flow switching engine"),
  TOTAL_BYTES_EXP(40, "N (default is 4)", "Counter with length N x 8 bits for bytes for the number of bytes exported by the Observation Domain"),
  TOTAL_PKTS_EXP(41, "N (default is 4)", "Counter with length N x 8 bits for bytes for the number of packets exported by the Observation Domain"),
  TOTAL_FLOWS_EXP(42, "N (default is 4)", "Counter with length N x 8 bits for bytes for the number of flows exported by the Observation Domain"),
  IPV4_SRC_PREFIX(44, "4", "IPv4 source address prefix (specific for Catalyst architecture)"),
  IPV4_DST_PREFIX(45, "4", "IPv4 destination address prefix (specific for Catalyst architecture)"),
  MPLS_TOP_LABEL_TYPE(46, "1", "MPLS Top Label Type: 0x00 UNKNOWN 0x01 TE-MIDPT 0x02 ATOM 0x03 VPN 0x04 BGP 0x05 LDP"),
  MPLS_TOP_LABEL_IP_ADDR(47, "4", "Forwarding Equivalent Class corresponding to the MPLS Top Label"),
  FLOW_SAMPLER_ID(48, "1", "\"Identifier shown in \"\"show flow-sampler\"\"\""),
  FLOW_SAMPLER_MODE(49, "1", "The type of algorithm used for sampling data: 0x02 random sampling. Use in connection with FLOW_SAMPLER_MODE"),
  FLOW_SAMPLER_RANDOM_INTERVAL(50, "4", "Packet interval at which to sample. Use in connection with FLOW_SAMPLER_MODE"),
  MIN_TTL(52, "1", "Minimum TTL on incoming packets of the flow"),
  MAX_TTL(53, "1", "Maximum TTL on incoming packets of the flow"),
  IPV4_IDENT(54, "2", "The IP v4 identification field"),
  DST_TOS(55, "1", "Type of Service byte setting when exiting outgoing interface"),
  IN_SRC_MAC(56, "6", "Incoming source MAC address"),
  OUT_DST_MAC(57, "6", "Outgoing destination MAC address"),
  SRC_VLAN(58, "2", "Virtual LAN identifier associated with ingress interface"),
  DST_VLAN(59, "2", "Virtual LAN identifier associated with egress interface"),
  IP_PROTOCOL_VERSION(60, "1", "Internet Protocol Version Set to 4 for IPv4, set to 6 for IPv6. If not present in the template,\" then version 4 is assumed.\""),
  DIRECTION(61, "1", "Flow direction: 0 - ingress flow,\" 1 - egress flow\""),
  IPV6_NEXT_HOP(62, "16", "IPv6 address of the next-hop router"),
  BGP_IPV6_NEXT_HOP(63, "16", "Next-hop router in the BGP domain"),
  IPV6_OPTION_HEADERS(64, "4", "Bit-encoded field identifying IPv6 option headers found in the flow"),
  MPLS_LABEL_1(70, "3", "MPLS label at position 1 in the stack. This comprises 20 bits of MPLS label,\" 3 EXP (experimental) bits and 1 S (end-of-stack) bit.\""),
  MPLS_LABEL_2(71, "3", "MPLS label at position 2 in the stack. This comprises 20 bits of MPLS label,\" 3 EXP (experimental) bits and 1 S (end-of-stack) bit.\""),
  MPLS_LABEL_3(72, "3", "MPLS label at position 3 in the stack. This comprises 20 bits of MPLS label,\" 3 EXP (experimental) bits and 1 S (end-of-stack) bit.\""),
  MPLS_LABEL_4(73, "3", "MPLS label at position 4 in the stack. This comprises 20 bits of MPLS label,\" 3 EXP (experimental) bits and 1 S (end-of-stack) bit.\""),
  MPLS_LABEL_5(74, "3", "MPLS label at position 5 in the stack. This comprises 20 bits of MPLS label,\" 3 EXP (experimental) bits and 1 S (end-of-stack) bit.\""),
  MPLS_LABEL_6(75, "3", "MPLS label at position 6 in the stack. This comprises 20 bits of MPLS label,\" 3 EXP (experimental) bits and 1 S (end-of-stack) bit.\""),
  MPLS_LABEL_7(76, "3", "MPLS label at position 7 in the stack. This comprises 20 bits of MPLS label,\" 3 EXP (experimental) bits and 1 S (end-of-stack) bit.\""),
  MPLS_LABEL_8(77, "3", "MPLS label at position 8 in the stack. This comprises 20 bits of MPLS label,\" 3 EXP (experimental) bits and 1 S (end-of-stack) bit.\""),
  MPLS_LABEL_9(78, "3", "MPLS label at position 9 in the stack. This comprises 20 bits of MPLS label,\" 3 EXP (experimental) bits and 1 S (end-of-stack) bit.\""),
  MPLS_LABEL_10(79, "3", "MPLS label at position 10 in the stack. This comprises 20 bits of MPLS label,\" 3 EXP (experimental) bits and 1 S (end-of-stack) bit.\""),
  IN_DST_MAC(80, "6", "Incoming destination MAC address"),
  OUT_SRC_MAC(81, "6", "Outgoing source MAC address"),
  IF_NAME(82, "N (default specified in template)", "\"Shortened interface name i.e.: \"\"FE1/0\"\"\""),
  IF_DESC(83, "N (default specified in template)", "\"Full interface name i.e.: \"\"'FastEthernet 1/0\"\"\""),
  SAMPLER_NAME(84, "N (default specified in template)", "Name of the flow sampler"),
  IN_PERMANENT_BYTES(85, "N (default is 4)", "Running byte counter for a permanent flow"),
  IN_PERMANENT_PKTS(86, "N (default is 4)", "Running packet counter for a permanent flow"),
  FRAGMENT_OFFSET(88, "2", "The fragment-offset value from fragmented IP packets"),
  FORWARDING_STATUS(89, "1", "Forwarding status is encoded on 1 byte with the 2 left bits giving the status and the 6 remaining bits giving the reason code." +
      " Status is either unknown (00), Forwarded (10), Dropped (10) or Consumed (11)." +
      " Below is the list of forwarding status values with their means.)," +
      " Unknown," +
      " • 0," +
      " Forwarded)," +
      " • Unknown 64)," +
      " • Forwarded Fragmented 65)," +
      " • Forwarded not Fragmented 66)," +
      " Dropped," +
      " • Unknown 128," +
      " • Drop ACL Deny 129," +
      " • Drop ACL drop 130," +
      " • Drop Unroutable 131," +
      " • Drop Adjacency 132," +
      " • Drop Fragmentation & DF set 133," +
      " • Drop Bad header checksum 134," +
      " • Drop Bad total Length 135," +
      " • Drop Bad Header Length 136," +
      " • Drop bad TTL 137," +
      " • Drop Policer 138," +
      " • Drop WRED 139," +
      " • Drop RPF 140," +
      " • Drop For us 141," +
      " • Drop Bad output interface 142," +
      " • Drop Hardware 143," +
      " Consumed," +
      " • Unknown 192," +
      " • Terminate Punt Adjacency 193," +
      " • Terminate Incomplete Adjacency 194," +
      " • Terminate For us 195"),
  MPLS_PAL_RD(90, "8 (array)", "MPLS PAL Route Distinguisher."),
  MPLS_PREFIX_LEN(91, "1", "Number of consecutive bits in the MPLS prefix length."),
  SRC_TRAFFIC_INDEX(92, "4", "BGP Policy Accounting Source Traffic Index"),
  DST_TRAFFIC_INDEX(93, "4", "BGP Policy Accounting Destination Traffic Index"),
  APPLICATION_DESCRIPTION(94, "N", "Application description."),
  APPLICATION_TAG(95, "1+n", "8 bits of engine ID,\" followed by n bits of classification.\""),
  APPLICATION_NAME(96, "N", "Name associated with a classification."),
  POSTIP_DIFF_SERV_CODE_POINTS(98, "1", "The value of a Differentiated Services Code Point (DSCP) encoded in the Differentiated Services Field,\" after modification.\""),
  REPLICATION_FACTOR(99, "4", "Multicast replication factor."),
  LAYER2_PACKET_SECTION_OFFSET(102, "", "Layer 2 packet section offset. Potentially a generic offset."),
  LAYER2_PACKET_SECTION_SIZE(103, "", "Layer 2 packet section size. Potentially a generic size."),
  LAYER2_PACKET_SECTION_DATA(104, "", "Layer 2 packet section data."),
  // END DATA FIELD TYPES

  // BEGIN SCOPE FIELD TYPES
  SCOPE_SYSTEM(NetflowV9Field.SCOPE_FIELD_OFFSET + 1, "", "System"),
  SCOPE_INTERFACE(NetflowV9Field.SCOPE_FIELD_OFFSET + 2, "", "Interface"),
  SCOPE_LINE_CARD(NetflowV9Field.SCOPE_FIELD_OFFSET + 3, "", "Line Card"),
  SCOPE_NETFLOW_CACHE(NetflowV9Field.SCOPE_FIELD_OFFSET + 4, "", "Netflow Cache"),
  SCOPE_TEMPLATE(NetflowV9Field.SCOPE_FIELD_OFFSET + 5, "", "Template"),
  // END SCOPE FIELD TYPES

  // BEGIN SCOPE FIELD TYPES
  ;

  private final int typeId;
  private final String lengthDescription;
  private final String typeDescription;

  NetflowV9FieldType(int typeId, String lengthDescription, String typeDescription) {
    this.typeId = typeId;
    this.lengthDescription = lengthDescription;
    this.typeDescription = typeDescription;
  }

  public static NetflowV9FieldType getTypeForId(int typeId) {
    return fieldIdToType.get(typeId);
  }

  public static NetflowV9FieldType getScopeTypeForId(int scopeTypeId) {
    return fieldIdToType.get(NetflowV9Field.SCOPE_FIELD_OFFSET + scopeTypeId);
  }

  private static final Map<Integer, NetflowV9FieldType> fieldIdToType = new HashMap();
  static {
    for (NetflowV9FieldType type : NetflowV9FieldType.values()) {
      fieldIdToType.put(type.typeId, type);
    }
  }

}
