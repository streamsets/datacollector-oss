describe('StreamSets Data Collector App', function() {

  beforeEach(function() {
    browser.ignoreSynchronization = true;
    //browser.manage().timeouts().pageLoadTimeout(10000);
  });

  afterEach(function() {
    browser.executeScript('window.sessionStorage.clear();');
    browser.executeScript('window.localStorage.clear();');
  });

  describe('Log page', function() {

    beforeEach(function() {
      browser.get('/collector/logs');
    });

    it('should render log page view when user navigates to /collector/logs', function() {
      element.all(by.repeater('logFile in logFiles')).then(function(logFiles) {
        expect(logFiles.length > 0).toBeTruthy();
      });
    });
  });
});
