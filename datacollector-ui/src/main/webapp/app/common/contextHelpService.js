/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Service for launching Contextual Help.
 */

angular.module('dataCollectorApp.common')
  .service('contextHelpService', function($rootScope, $q, configuration, api, pipelineConstant, pipelineService) {

    // pre-populate with some static configurations
    var helpIds = {
        "pipeline-configuration": "index.html#datacollector/UserGuide/Pipeline_Configuration/PipelineConfiguration_title.html#task_xlv_jdw_kq",
        "pipeline-preview": "index.html#datacollector/UserGuide/Data_Preview/DataPreview_Title.html#concept_jtn_s3m_lq",
        "pipeline-snapshot": "index.html#datacollector/UserGuide/Pipeline_Monitoring/PipelineMonitoring_title.html#task_wvz_rfp_tq",
        "pipeline-monitoring": "index.html#datacollector/UserGuide/Pipeline_Monitoring/PipelineMonitoring_title.html#concept_hsp_tnt_lq",
        "errors-tab": "index.html#datacollector/UserGuide/Pipeline_Monitoring/PipelineMonitoring_title.html#concept_pd3_crv_yr",
        "data-rules-tab": "index.html#datacollector/UserGuide/Alerts/RulesAlerts_title.html#concept_tpm_rsk_zq",
        "metric-rules-tab": "index.html#datacollector/UserGuide/Alerts/RulesAlerts_title.html#concept_abj_nsk_zq",
        "data-drift-rules-tab": "index.html#datacollector/UserGuide/Alerts/RulesAlerts_title.html#concept_wbz_mkk_p5"
      },
      buildInfo = {},
      helpWindow;

    this.configInitPromise = $q.all([
      api.admin.getBuildInfo(),
      pipelineService.init(),
      configuration.init()]
    ).then(function(results) {
      var stageConfigDefinitions = pipelineService.getStageDefinitions();
      angular.forEach(stageConfigDefinitions, function(stageConfigDefinition) {
        helpIds[stageConfigDefinition.name] = stageConfigDefinition.onlineHelpRefUrl;
      });
      buildInfo = results[0].data;
    });

    this.launchHelp = function(stagename) {
      this.configInitPromise.then(function() {
        var uiHelpBaseURL, helpURL,
          relativeURL = helpIds[stagename];

        uiHelpBaseURL = getHelpBaseUrl();

        helpURL = uiHelpBaseURL + '/' + (relativeURL || 'index.html');

        if(typeof(helpWindow) == 'undefined' || helpWindow.closed) {
          helpWindow = window.open(helpURL);
        } else {
          helpWindow.location.href = helpURL;
          helpWindow.focus();
        }

      });
    };

    this.launchHelpContents = function() {
      this.configInitPromise.then(function() {
        var uiHelpBaseURL, helpURL;
        uiHelpBaseURL = getHelpBaseUrl();
        helpURL = uiHelpBaseURL + '/index.html';

        if(typeof(helpWindow) == 'undefined' || helpWindow.closed) {
          helpWindow = window.open(helpURL);
        } else {
          helpWindow.location.href = helpURL;
          helpWindow.focus();
        }

      });
    };

    var getHelpBaseUrl = function() {
      var uiHelpBaseURL;
      if ($rootScope.$storage.helpLocation === pipelineConstant.HOSTED_HELP && navigator && navigator.onLine) {
        if (buildInfo.version.indexOf('-SNAPSHOT') === -1) {
          uiHelpBaseURL = 'https://www.streamsets.com/documentation/datacollector/' + buildInfo.version + '/help';
        } else {
          uiHelpBaseURL = 'https://streamsets.com/documentation/datacollector/latest/help/';
        }
      } else {
        uiHelpBaseURL = configuration.getUILocalHelpBaseURL();
      }
      return uiHelpBaseURL;
    };

  });
