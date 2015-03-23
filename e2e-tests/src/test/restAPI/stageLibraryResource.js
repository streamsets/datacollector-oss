var frisby = require('frisby');

frisby.create('Login to StreamSets Data Collector')
  .get(browser.baseUrl + 'login?j_username=admin&j_password=admin')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'];

    /**
     * GET rest/v1/definitions
     */
    frisby.create('Should return Pipeline & Stage Library definitions')
      .get(browser.baseUrl + 'rest/v1/definitions', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      //.inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(definitionsJSON) {
        expect(definitionsJSON).toBeDefined();

        expect(definitionsJSON.pipeline).toBeDefined();
        expect(definitionsJSON.pipeline.length === 1).toBeTruthy();

        expect(definitionsJSON.stages).toBeDefined();
        expect(definitionsJSON.stages.length > 1).toBeTruthy();


        var firstStageDefinition = definitionsJSON.stages[0],
          iconURL = browser.baseUrl + 'rest/v1/definitions/stages/icon?name=' + firstStageDefinition.name +
            '&library=' + firstStageDefinition.library + '&version=' + firstStageDefinition.version,
          icon = firstStageDefinition.icon,
          contentType = icon.indexOf('.png') !== -1 ? 'image/png' : 'image/svg+xml';

        /**
         * GET rest/v1/definitions/stages/icon?name=<STAGE_NAME>&library=<LIBRARY_NAME>&version=<VERSION>
         */
        frisby.create('Should return icon for Stage Library')
          .get(iconURL, {
            headers:  {
              "Cookie": cookie
            }
          })
          .expectStatus(200)
          .expectHeaderContains('content-type', contentType)
          .toss();

      })
      .toss();

  })
  .toss();