var frisby = require('frisby');


frisby.create('Trying to access /jmx before login should redirect to login page')
  .get(browser.baseUrl + 'jmx')
  .expectStatus(200)
  .expectHeaderContains('content-type', 'text/html')
  .toss();


frisby.create('Login to StreamSets Data Collector')
  .get(browser.baseUrl + 'login?j_username=admin&j_password=admin')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'];

    /**
     * GET /jmx
     */
    frisby.create('Should return JMX JSON data')
      .get(browser.baseUrl + 'jmx', {
          headers:  {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Cookie": cookie
          }
        })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(jmxJSON) {
        expect(jmxJSON.beans).toBeDefined();
        expect(jmxJSON.beans.length > 0).toBeTruthy();
      })
      .toss();

  })
  .toss();


