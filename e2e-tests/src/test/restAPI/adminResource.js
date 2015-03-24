var frisby = require('frisby');

frisby.create('Login to StreamSets Data Collector using admin role')
  .get(browser.baseUrl + 'login?j_username=admin&j_password=admin')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'];

    /**
     * GET rest/v1/admin/threadsDump
     */
    frisby.create('Should return SDC JVM Thread Dump')
      .get(browser.baseUrl + 'rest/v1/admin/threadsDump', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(threadsJSON) {
        expect(threadsJSON).toBeDefined();
        expect(threadsJSON.length > 1).toBeTruthy();
        expect(threadsJSON[0].threadInfo.threadName).toBeDefined();
        expect(threadsJSON[0].threadInfo.stackTrace).toBeDefined();
      })
      .toss();


  })
  .toss();



frisby.create('Login to StreamSets Data Collector using creator role')
  .get(browser.baseUrl + 'login?j_username=creator&j_password=creator')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'];

    /**
     * GET rest/v1/admin/threadsDump
     */
    frisby.create('Should throw 403 forbidden error when non admin user requests for SDC JVM Thread Dump')
      .get(browser.baseUrl + 'rest/v1/admin/threadsDump', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .expectStatus(403)
      .expectHeaderContains('content-type', 'text/html')
      .toss();


    /**
     * GET rest/v1/admin/shutdown
     */
    frisby.create('Should throw 403 forbidden error when non admin user requests for shutdown')
      .post(browser.baseUrl + 'rest/v1/admin/shutdown', {}, {
        headers:  {
          "Cookie": cookie
        }
      })
      .expectStatus(403)
      .expectHeaderContains('content-type', 'text/html')
      .toss();


  })
  .toss();