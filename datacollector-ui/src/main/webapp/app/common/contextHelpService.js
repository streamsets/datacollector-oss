/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
        "pipeline-configuration": "index.html#Pipeline_Configuration/ConfiguringAPipeline.html",
        "pipeline-preview": "index.html#Data_Preview/PreviewingaSingleStage.html#task_cxd_p25_qq",
        "pipeline-snapshot": "index.html#Pipeline_Monitoring/ReviewingSnapshotData.html",
        "pipeline-monitoring": "index.html#Pipeline_Monitoring/PipelineMonitoring.html#concept_hsp_tnt_lq",
        "errors-tab": "index.html#Pipeline_Monitoring/MonitoringErrors.html#concept_pd3_crv_yr",
        "data-rules-tab": "index.html#Alerts/DataAlerts.html#concept_tpm_rsk_zq",
        "metric-rules-tab": "index.html#Alerts/MetricAlerts.html#concept_abj_nsk_zq"
      },
      buildInfo = {},
      helpWindow;

    this.configInitPromise = $q.all([api.admin.getBuildInfo(), pipelineService.init(),
      configuration.init()]).then(function(results) {
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

        if ($rootScope.$storage.helpLocation === pipelineConstant.HOSTED_HELP) {
          uiHelpBaseURL = 'https://www.streamsets.com/documentation/datacollector/' + buildInfo.version + '/help';
        } else {
          uiHelpBaseURL = configuration.getUILocalHelpBaseURL();
        }

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
        if ($rootScope.$storage.helpLocation === pipelineConstant.HOSTED_HELP) {
          uiHelpBaseURL = 'https://www.streamsets.com/documentation/datacollector/' + buildInfo.version + '/help';
        } else {
          uiHelpBaseURL = configuration.getUILocalHelpBaseURL();
        }

        helpURL = uiHelpBaseURL + '/index.html';

        if(typeof(helpWindow) == 'undefined' || helpWindow.closed) {
          helpWindow = window.open(helpURL);
        } else {
          helpWindow.location.href = helpURL;
          helpWindow.focus();
        }

      });
    };

  });