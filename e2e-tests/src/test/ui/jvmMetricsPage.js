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
      browser.get('/collector/jvmMetrics');
    });

    it('should render jvmMetrics page view when user navigates to /collector/jvmMetrics', function() {
      browser.sleep(1000);
      element.all(by.repeater('chart in chartList')).then(function(charts) {
        expect(charts.length > 0).toBeTruthy();
      });
    });


  });
});
