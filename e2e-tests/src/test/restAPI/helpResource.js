var frisby = require('frisby');

frisby.create('Login to StreamSets Data Collector')
  .get(browser.baseUrl + 'login?j_username=admin&j_password=admin')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'];

    /**
     * GET rest/v1/helpref
     */
    frisby.create('Should return Help Reference data')
      .get(browser.baseUrl + 'rest/v1/helpref', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(helpRefJSON) {
        expect(helpRefJSON).toBeDefined();
        expect(helpRefJSON['pipeline-configuration']).toBeDefined();
      })
      .toss();

  })
  .toss();