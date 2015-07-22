var frisby = require('frisby'),
  EC = protractor.ExpectedConditions,
  pipelineName = 'createPipeline' + (new Date()).getTime();

describe('StreamSets Data Collector App', function() {

  beforeEach(function() {
    browser.ignoreSynchronization = true;
    //browser.manage().timeouts().pageLoadTimeout(10000);
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

      browser.sleep(1000);

      element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {


        if(pipelines.length === 0) {
          element.all(by.css('.create-pipeline-btn')).then(function(elements) {
            var createBtnElement = elements[elements.length - 1];
            createBtnElement.click();

            element(by.model('newConfig.name')).sendKeys(pipelineName);
            element(by.model('newConfig.description')).sendKeys('pipeline description');
            element(by.css('button[type="submit"]')).click();

            browser.sleep(1500);
          });
        } else {
          element.all(by.css('.create-pipeline-btn-link')).then(function(elements) {
            var createBtnElement = elements[elements.length - 1];
            createBtnElement.click();

            element(by.model('newConfig.name')).sendKeys(pipelineName);
            element(by.model('newConfig.description')).sendKeys('pipeline description');
            element(by.css('button[type="submit"]')).click();

            browser.sleep(1500);
          });
        }
      });


      var detailsTab = element(by.css('.detail-tabs-left'));
      browser.wait(EC.presenceOf(detailsTab), 10000);


      //Set Pipeline Configuration

      //Change description
      element(by.model('pipelineConfig.description')).sendKeys(' updated description');

      var configGroupTabs, constantsFormGroup;
      element.all(by.repeater('groupNameToLabelMap in configGroupTabs'))
        .then(function(_configGroupTabs) {
          configGroupTabs = _configGroupTabs;
          return configGroupTabs[0].element(by.tagName('a')).click();
        })
        .then(function() {
          //Select Constants Tab
          constantsFormGroup = detailsTab.element(by.css('.config_constants'));
          return constantsFormGroup.element(by.css('.btn-default')).click();
        })
        .then(function() {
          return constantsFormGroup.all(by.repeater('mapObject in detailPaneConfig.configuration[configIndex].value track by $index'));
        })
        .then(function(constants) {

          //Add one constant
          constants[0].element(by.model('mapObject.key')).sendKeys('CONSTANTA');
          return browser.executeScript(function() {
            var constantDom = document.getElementsByClassName('config_constants')[0],
              firstConstantValueDom = constantDom.getElementsByClassName('CodeMirror')[0];

            firstConstantValueDom.CodeMirror.setValue('10');
          });
        })
        .then(function() {
          //Click on Errors Group Tab
          return configGroupTabs[1].element(by.tagName('a')).click();
        })
        .then(function() {
          //Select Discard option
          return element(by.cssContainingText('option', 'Discard (Library: Basic)')).click();
        });

      //Add Source
      var selectSourceSelectElement = element(by.model('selectedSource.selected')),
        selectProcessorSelectElement,
        selectTargetSelectElement,
        dataGenConfigs;

      selectSourceSelectElement.element(by.cssContainingText('option', 'Dev Data Generator - Dev (for development only)')).click()
        .then(function() {
          //Add Field1 to the list of fields
          detailsTab = element(by.css('.detail-tabs-left'));
          dataGenConfigs = detailsTab.element(by.css('.config_dataGenConfigs'));

          browser.wait(EC.presenceOf(dataGenConfigs), 10000);

          return browser.executeScript(function() {
            var dataGenConfigsDom = document.getElementsByClassName('config_dataGenConfigs')[0],
              firstFieldDom = dataGenConfigsDom.getElementsByClassName('CodeMirror')[0];
            firstFieldDom.CodeMirror.setValue('field1');
          });

        })
        .then(function() {
          browser.wait(EC.presenceOf(element(by.css('select[name="newPipelineProcessor"]'))), 10000);
          selectProcessorSelectElement = element(by.css('select[name="newPipelineProcessor"]'));
          return selectProcessorSelectElement.element(by.cssContainingText('option', 'Dev Random Error - Dev (for development only)')).click()
        })
        .then(function() {
          browser.wait(EC.presenceOf(element(by.css('select[name="newPipelineProcessor"]'))), 10000);
          return selectProcessorSelectElement.element(by.cssContainingText('option', 'Dev Record Creator - Dev (for development only)')).click()
        })
        .then(function() {
          browser.wait(EC.presenceOf(element(by.css('select[name="newPipelineTarget"]'))), 10000);
          selectTargetSelectElement = element(by.css('select[name="newPipelineTarget"]'));
          return selectTargetSelectElement.element(by.cssContainingText('option', 'Trash - Basic')).click()
        });


      browser.sleep(1500);

    });

  });
});


//Verify the Pipeline Creation in UI by fetching the Pipeline Configuration using REST API



frisby.create('Login to StreamSets Data Collector')
  .get(browser.baseUrl + 'login?j_username=admin&j_password=admin')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'],
      initialPipelineCount = 0;



    /**
     * GET rest/v1/pipeline-library/<PIPELINE_NAME>
     */
    frisby.create('Should be able to fetch pipeline configuration.')
      .get(browser.baseUrl + 'rest/v1/pipeline-library/' + pipelineName, {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(pipelineJSON) {
        var pipeline1ConfigJSON = pipelineJSON;

        expect(pipelineJSON).toBeDefined();
        expect(pipelineJSON.info.name).toEqual(pipelineName);
        expect(pipelineJSON.info.description).toEqual('pipeline description updated description');


        //Check Pipeline configurations
        expect(pipelineJSON.configuration).toBeDefined();
        pipelineJSON.configuration.forEach(function(config) {
          if(config.name === 'badRecordsHandling') {
            expect(config.value).toBeDefined();
            expect(config.value === 'streamsets-datacollector-basic-lib::com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget::1').toBeTruthy();
          } else if(config.name === 'constants') {
            expect(config.value).toBeDefined();
            expect(config.value.length === 1).toBeTruthy();
            expect(config.value[0].key === 'CONSTANTA').toBeTruthy();
            expect(config.value[0].value === '10').toBeTruthy();
          }
        });

        //Check stage instances
        expect(pipelineJSON.stages).toBeDefined();
        expect(pipelineJSON.stages.length === 4).toBeTruthy();
      })
      .toss();

  })
  .toss();
