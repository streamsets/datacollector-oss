package com.streamsets.pipeline.stage.origin.omniture;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.StageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.AsyncInvoker;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Consumes the HTTP stream chunk by chunk buffering it into a queue for consumption
 * by the origin.
 */
class OmniturePollingConsumer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(OmniturePollingConsumer.class);

  private static final String WSSE_HEADER = "X-WSSE";

  private WebTarget queueResource;
  private WebTarget getResource;

  private final String reportDescription;
  private final long responseTimeoutMillis;
  private final String username;
  private final String sharedSecret;
  private final BlockingQueue<String> entityQueue;

  private volatile boolean stop = false;

  /**
   * Constructor for unauthenticated connections.
   * @param resourceUrl URL of streaming JSON resource.
   * @param reportDescription
   * @param responseTimeoutMillis How long to wait for a response from http endpoint.
   * @param username
   * @param sharedSecret
   * @param entityQueue A queue to place received chunks (usually a single JSON object) into.
   */
  public OmniturePollingConsumer(
      final String resourceUrl,
      final String reportDescription,
      final long responseTimeoutMillis,
      final String username,
      final String sharedSecret,
      BlockingQueue<String> entityQueue
  ) {
    this.responseTimeoutMillis = responseTimeoutMillis;
    this.username = username;
    this.sharedSecret = sharedSecret;
    this.reportDescription = reportDescription;
    this.entityQueue = entityQueue;
    Client client = ClientBuilder.newClient();
    queueResource = client.target(resourceUrl + "?method=Report.Queue");
    getResource = client.target(resourceUrl + "?method=Report.Get");
  }

  @Override
  public void run() {
    try {
      int reportId = queueReport();
      getReport(reportId);

      LOG.debug("HTTP stream consumer closed.");
    } catch (InterruptedException | ExecutionException e) {
      LOG.warn(Errors.OMNITURE_01.getMessage(), e.toString(), e);
    } catch (TimeoutException e) {
      LOG.warn("HTTP request future timed out", e.toString(), e);
    } catch (IOException e) {
      LOG.warn("HTTP request failed", e.toString(), e);
    } catch (StageException e) {
      LOG.warn("Error while generating report", e);
    }
  }

  /**
   * Queue a report using the Report.Queue method. This will post a request with the report description
   * to the Omniture API and return a report ID that can be used to retrieve the report once it's ready.
   * @return report ID
   * @throws IOException
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   */
  public int queueReport() throws IOException, InterruptedException, ExecutionException, TimeoutException, StageException {
    final AsyncInvoker asyncInvoker = queueResource.request()
        .header(WSSE_HEADER, OmnitureAuthUtil.getHeader(username, sharedSecret))
        .async();
    LOG.debug("Queueing report using URL {} with description {}",
        queueResource.getUri().toURL().toString(), reportDescription);
    final Future<Response> responseFuture = asyncInvoker.post(Entity.json(reportDescription));
    Response response = responseFuture.get(responseTimeoutMillis, TimeUnit.MILLISECONDS);

    ObjectMapper mapper = new ObjectMapper();
    String json = response.readEntity(String.class);
    JsonNode root = mapper.readTree(json);

    if (root.has("error")) {
      throw new StageException(Errors.OMNITURE_02,
          root.get("error").get("error_description").asText());
    }

    LOG.info("Omniture report queued");
    return root.get("reportID").asInt();
  }

  /**
   * Posts a request to the Omniture API to get a report back. Reports may take a while to generate,
   * so this will loop on the Report.Get request, and ignore any errors indicating that the report
   * is not yet ready.
   * @param reportId ID of report to get
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   * @throws IOException
   */
  public void getReport(int reportId)
      throws InterruptedException, ExecutionException, TimeoutException, IOException, StageException {
    int waitTime = 1000;
    Response response = null;
    while (!stop) {
      final AsyncInvoker asyncInvoker = getResource.request()
          .header(WSSE_HEADER, OmnitureAuthUtil.getHeader(username, sharedSecret))
          .async();

      LOG.debug("Getting report using URL {} with report ID {}", getResource.getUri().toURL().toString(), reportId);
      final Future<Response> responseFuture = asyncInvoker.post(Entity.json("{ \"reportID\": " + reportId + " }"));
      response = responseFuture.get(responseTimeoutMillis, TimeUnit.MILLISECONDS);
      String input = response.readEntity(String.class);

      ObjectMapper mapper = new ObjectMapper();
      JsonNode root = mapper.readTree(input);

      // If the report has an error field, it means the report has not finished generating
      if (!root.has("error")) {
        boolean accepted = entityQueue.offer(input, responseTimeoutMillis, TimeUnit.MILLISECONDS);
        if (!accepted) {
          LOG.warn("Response buffer full, dropped record.");
        }
        break;
      } else {
        // Exponential backoff while making subsequent Report.Get requests
        if (root.get("error").textValue().equals("report_not_ready")) {
          waitTime *= 2;
          LOG.info("Report not available. Sleeping for {} seconds", waitTime / 1000);
          Thread.sleep(waitTime);
        } else {
          throw new StageException(Errors.OMNITURE_02,
              root.get("error").get("error_description").asText());
        }
      }
    }
    response.close();
  }

  /**
   * Directs the consumer to stop waiting for reports to come back.
   */
  public void stop() { stop = true; }
}