var frisby = require('frisby');

frisby.create('Login to StreamSets Data Collector')
  .get(browser.baseUrl + 'login?j_username=admin&j_password=admin')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'],
      initialPipelineCount = 0;

    /**
     * GET rest/v1/pipeline-library
     */
    frisby.create('Should return all Pipeline Configuration Info.')
      .get(browser.baseUrl + 'rest/v1/pipeline-library', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(pipelineListJSON) {
        expect(pipelineListJSON).toBeDefined();

        pipelineListJSON.forEach(function(pipelineInfo) {
          frisby.create('Should be able to delete pipeline configuration.')
            .delete(browser.baseUrl + 'rest/v1/pipeline-library/' + pipelineInfo.name, {}, {
              headers:  {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Cookie": cookie
              }
            })
            .expectStatus(200)
            .toss();
        });

      })
      .toss();


  })
  .toss();