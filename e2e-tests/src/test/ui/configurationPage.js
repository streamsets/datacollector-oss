describe('StreamSets Data Collector App', function() {

  beforeEach(function() {
    browser.ignoreSynchronization = true;
    //browser.manage().timeouts().pageLoadTimeout(10000);
  });

  afterEach(function() {
    browser.executeScript('window.sessionStorage.clear();');
    browser.executeScript('window.localStorage.clear();');
  });

  describe('Configuration page', function() {

    beforeEach(function() {
      browser.get('/collector/configuration');
    });

    it('should render configuration page view when user navigates to /collector/configuration', function() {
      element.all(by.repeater('configKey in configKeys')).then(function(configurations) {
        expect(configurations.length > 0).toBeTruthy();
      });
    });


  });
});
