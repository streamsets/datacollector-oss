var frisby = require('frisby');

frisby.create('Login to StreamSets Data Collector')
  .get(browser.baseUrl + 'login?j_username=admin&j_password=admin')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'];

    /**
     * GET rest/v1/configuration/all
     */
    frisby.create('Should return SDC Configuration data')
      .get(browser.baseUrl + 'rest/v1/configuration/all', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(configurationJSON) {
        expect(configurationJSON).toBeDefined();
        expect(configurationJSON['ui.refresh.interval.ms']).toBeDefined();
        expect(configurationJSON['ui.local.help.base.url']).toBeDefined();
        expect(configurationJSON['mail.transport.protocol']).toBeDefined();
      })
      .toss();


  })
  .toss();