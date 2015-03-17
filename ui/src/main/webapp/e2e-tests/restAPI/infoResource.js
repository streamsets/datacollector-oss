var frisby = require('frisby');

frisby.create('Login to StreamSets Data Collector')
  .get(browser.baseUrl + 'login?j_username=admin&j_password=admin')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'];

    /**
     * GET rest/v1/info/sdc
     */
    frisby.create('Should return SDC Build Information data')
      .get(browser.baseUrl + 'rest/v1/info/sdc', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(buildInfoJSON) {
        expect(buildInfoJSON).toBeDefined();
        expect(buildInfoJSON['builtDate']).toBeDefined();
        expect(buildInfoJSON['builtBy']).toBeDefined();
        expect(buildInfoJSON['builtRepoSha']).toBeDefined();
        expect(buildInfoJSON['version']).toBeDefined();
      })
      .toss();


    /**
     * GET rest/v1/info/user
     */
    frisby.create('Should return User Information')
      .get(browser.baseUrl + 'rest/v1/info/user', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(buildInfoJSON) {
        expect(buildInfoJSON).toBeDefined();
        expect(buildInfoJSON['user']).toBeDefined();
        expect(buildInfoJSON['roles']).toBeDefined();
      })
      .toss();


  })
  .toss();
