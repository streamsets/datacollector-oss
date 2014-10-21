package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.config.RuntimePipelineConfiguration;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

/**
 * Created by harikiran on 10/19/14.
 */
public class TestPipelineResource extends JerseyTest {
  
  private static final String REQUEST_JSON = "{\n" +
    "  \"uuid\" : \"c3e8d544-34c6-452e-b87f-518f4c3cffcb\",\n" +
    "  \"runtimeModuleConfigurations\" : [\n" +
    "    {\n" +
    "      \"generalInformation\" : {\n" +
    "        \"instanceName\" : \"myCsvSource\",\n" +
    "        \"moduleName\" : \"CSVSource\",\n" +
    "        \"moduleVersion\" : \"1.0\",\n" +
    "        \"moduleDescription\" : \"This is CSV Source [comma separated]\"\n" +
    "      },\n" +
    "      \"configInformation\" : {\n" +
    "        \"options\" : [\n" +
    "          {\n" +
    "            \"name\" : \"fileLocation\",\n" +
    "            \"description\" : \"This is the location of the file from which the data must be read\",\n" +
    "            \"shortDescription\" : \"file_location\",\n" +
    "            \"defaultValue\" : \"/etc/data\",\n" +
    "            \"type\" : \"TEXT\",\n" +
    "            \"mandatory\" : \"true\",\n" +
    "            \"group\" : \"FileOptions\"\n" +
    "          },\n" +
    "          {\n" +
    "            \"name\" : \"bufferSize\",\n" +
    "            \"description\" : \"This is the number of bytes that must be read in one go\",\n" +
    "            \"shortDescription\" : \"buffer_size\",\n" +
    "            \"defaultValue\" : \"10000\",\n" +
    "            \"type\" : \"NUMBER\",\n" +
    "            \"mandatory\" : \"true\",\n" +
    "            \"group\" : \"FileOptions\"\n" +
    "          }\n" +
    "        ],\n" +
    "        \"inputLanes\" : [\n" +
    "          \"csv->mask\"\n" +
    "        ],\n" +
    "        \"outputLanes\" : [\n" +
    "          \"csv->mask\"\n" +
    "        ]\n" +
    "      },\n" +
    "      \"uiInformation\" : {\n" +
    "        \"xPos\" : \"100\",\n" +
    "        \"yPos\" : \"100\"\n" +
    "      }\n" +
    "    },\n" +
    "    {\n" +
    "      \"generalInformation\" : {\n" +
    "        \"instanceName\" : \"myMaskingProcessor\",\n" +
    "        \"moduleName\" : \"MaskingProcessor\",\n" +
    "        \"moduleVersion\" : \"1.0\",\n" +
    "        \"moduleDescription\" : \"This is masking processor.\"\n" +
    "      },\n" +
    "      \"configInformation\" : {\n" +
    "        \"options\" : [\n" +
    "          {\n" +
    "            \"name\" : \"mask\",\n" +
    "            \"description\" : \"This is the character used to mask the sensitive data.\",\n" +
    "            \"shortDescription\" : \"mask\",\n" +
    "            \"defaultValue\" : \"*\",\n" +
    "            \"type\" : \"TEXT\",\n" +
    "            \"mandatory\" : \"true\",\n" +
    "            \"group\" : \"MakingOption\"\n" +
    "          }\n" +
    "        ],\n" +
    "        \"inputLanes\" : [\n" +
    "          \"csv->mask\"\n" +
    "        ],\n" +
    "        \"outputLanes\" : [\n" +
    "          \"mask->kafka\"\n" +
    "        ]\n" +
    "      },\n" +
    "      \"uiInformation\" : {\n" +
    "        \"xPos\" : \"500\",\n" +
    "        \"yPos\" : \"500\"\n" +
    "      }\n" +
    "    },\n" +
    "    {\n" +
    "      \"generalInformation\" : {\n" +
    "        \"instanceName\" : \"myKafkaTarget\",\n" +
    "        \"moduleName\" : \"KafkaTarget\",\n" +
    "        \"moduleVersion\" : \"1.0\",\n" +
    "        \"moduleDescription\" : \"This is kafka target.\"\n" +
    "      },\n" +
    "      \"configInformation\" : {\n" +
    "        \"options\" : [\n" +
    "          {\n" +
    "            \"name\" : \"kafkaTopic\",\n" +
    "            \"description\" : \"This is the kafka topic to which the data must be written\",\n" +
    "            \"shortDescription\" : \"kafka_topic\",\n" +
    "            \"defaultValue\" : \"myTopic\",\n" +
    "            \"type\" : \"TEXT\",\n" +
    "            \"mandatory\" : \"true\",\n" +
    "            \"group\" : \"Kafka\"\n" +
    "          },\n" +
    "          {\n" +
    "            \"name\" : \"kafkaHost\",\n" +
    "            \"description\" : \"This is the host on which the kafka cluster is installed.\",\n" +
    "            \"shortDescription\" : \"kafka_host\",\n" +
    "            \"defaultValue\" : \"localhost\",\n" +
    "            \"type\" : \"TEXT\",\n" +
    "            \"mandatory\" : \"true\",\n" +
    "            \"group\" : \"Kafka\"\n" +
    "          }\n" +
    "        ],\n" +
    "        \"inputLanes\" : [\n" +
    "          \"mask->kafka\"\n" +
    "        ],\n" +
    "        \"outputLanes\" : [\n" +
    "          \"kafka->hdfs\"\n" +
    "        ]\n" +
    "      },\n" +
    "      \"uiInformation\" : {\n" +
    "        \"xPos\" : \"900\",\n" +
    "        \"yPos\" : \"900\"\n" +
    "      }\n" +
    "    }\n" +
    "  ],\n" +
    "  \"errors\" : [\n" +
    "    {\n" +
    "      \"module\" : \"myCsvSource\",\n" +
    "      \"errorMessages\" : [\n" +
    "        \"Source cannot have input lanes\"\n" +
    "      ]\n" +
    "    },\n" +
    "    {\n" +
    "      \"module\" : \"myKafkaTarget\",\n" +
    "      \"errorMessages\" : [\n" +
    "        \"Target cannot have output lanes\",\n" +
    "        \"Target topic does not exist\"\n" +
    "      ]\n" +
    "    }\n" +
    "  ]\n" +
    "}";

  @Override
  protected Application configure() {
    return new ResourceConfig(PipelineResource.class);
  }

 @Test
  public void testGetAllPipelines() {
    String pipelineConfigurationString = target("/v1/pipelines").request().get(String.class);
    //returns a List of Maps, where each map corresponds to a module info object
    System.out.println(pipelineConfigurationString);
  }

  @Test
  public void testGetConfiguration() {
    String pipelineConfigurationString = target("/v1/pipelines/myPipeline/config").request().get(String.class);
    //returns a List of Maps, where each map corresponds to a module info object
    System.out.println(pipelineConfigurationString);
  }

  @Test
  public void testSetConfiguration() {
    WebTarget pipelineResource = target("/v1/pipelines").path("{pipelineName}")
      .resolveTemplate("pipelineName","myPipeline").path("/config").queryParam("mode", "preview");
    RuntimePipelineConfiguration r = pipelineResource.request(MediaType.APPLICATION_JSON).post(Entity.json(REQUEST_JSON)
    , RuntimePipelineConfiguration.class);
    //returns a List of Maps, where each map corresponds to a module info object
    System.out.println(r.toString());
  }

}
