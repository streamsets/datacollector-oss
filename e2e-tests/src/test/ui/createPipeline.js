describe('StreamSets Data Collector App', function() {

  beforeEach(function() {
    browser.ignoreSynchronization = true;
    browser.manage().timeouts().pageLoadTimeout(10000);
  });

  afterEach(function() {
    browser.executeScript('window.sessionStorage.clear();');
    browser.executeScript('window.localStorage.clear();');
  });

  browser.get('/');

  describe('Create Pipeline', function() {

    beforeEach(function() {
      browser.get('/');
    });

    it('should be able to create new empty pipeline', function() {
      var pipelineName = 'createPipeline' + (new Date()).getTime();

      browser.sleep(1000);

      element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {
        if(pipelines.length === 0) {
          //If no pipeline
          element.all(by.css('.create-pipeline-btn')).then(function(elements) {
            var importBtnElement = elements[elements.length - 1];
            importBtnElement.click();

            element(by.model('newConfig.name')).sendKeys(pipelineName);
            element(by.model('newConfig.description')).sendKeys('pipeline description');
            element(by.css('button[type="submit"]')).click();

            browser.sleep(1500);
          });
        } else {
          //pipeline exists
          element(by.css('[ng-click="toggleLibraryPanel()"]')).click();

          element.all(by.css('.create-pipeline-btn')).then(function(elements) {
            var createBtnElement = elements[0];
            createBtnElement.click();

            //Fill Create Pipeline Modal Dialog values
            element(by.model('newConfig.name')).sendKeys(pipelineName);
            element(by.model('newConfig.description')).sendKeys('pipeline description');
            element(by.css('button[type="submit"]')).click();
          });

        }
      });
    });


    it('should be able to update Pipeline Configuration', function() {
      browser.sleep(1500);
      element.all(by.repeater('groupNameToLabelMap in configGroupTabs')).then(function(configGroupTabs) {
        configGroupTabs[1].click();
      });
    });


  });
});
