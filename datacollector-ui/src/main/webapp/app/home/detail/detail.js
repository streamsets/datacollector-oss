/**
 * Controller for Detail Pane.
 */

angular
  .module('dataCollectorApp.home')

  .controller('DetailController', function ($scope, $rootScope, _, pipelineConstant, api, contextHelpService) {
    var infoTab =  {
        name:'info',
        template:'app/home/detail/info/info.tpl.html',
        iconClass: 'fa fa-info-circle'
      },
      historyTab = {
        name:'history',
        template:'app/home/detail/history/history.tpl.html',
        iconClass: 'fa fa-history'
      },
      configurationTab = {
        name:'configuration',
        template:'app/home/detail/configuration/configuration.tpl.html',
        iconClass: 'fa fa-gear'
      },
      rawPreviewTab = {
        name:'rawPreview',
        template:'app/home/detail/rawPreview/rawPreview.tpl.html',
        iconClass: 'fa fa-eye'
      },
      summaryTab = {
        name:'summary',
        template:'app/home/detail/summary/summary.tpl.html',
        iconClass: 'fa fa-bar-chart',
        active: true,
        helpId: 'pipeline-monitoring'
      },
      errorTab = {
        name:'errors',
        template:'app/home/detail/badRecords/badRecords.tpl.html',
        iconClass: 'fa fa-exclamation-triangle',
        helpId: 'errors-tab'
      },
      dataSummaryTab = {
        name:'summary',
        template:'app/home/detail/dataSummary/dataSummary.tpl.html',
        iconClass: 'fa fa-bar-chart',
        active: true,
        helpId: 'pipeline-monitoring'
      },
      dataRulesTab = {
        name:'dataRules',
        template:'app/home/detail/rules/dataRules/dataRules.tpl.html',
        iconClass: 'fa fa-list',
        helpId: 'data-rules-tab'
      },
      metricAlertRulesTab = {
        name:'metricAlertRules',
        template:'app/home/detail/rules/metricAlert/metricAlert.tpl.html',
        iconClass: 'fa fa-list',
        helpId: 'metric-rules-tab'
      },
      rulesTab = {
        name:'rules',
        template:'app/home/detail/rules/rules.tpl.html',
        iconClass: 'fa fa-list',
        helpId: 'metric-rules-tab'
      },
      emailIdsTab = {
        name:'emailIDs',
        template:'app/home/detail/rules/emailIDs/emailIDs.tpl.html',
        iconClass: 'fa fa-envelope-o'
      };

    /**
     * Returns list tabs based on type.
     *
     * @param type
     * @param isPipelineRunning
     * @returns {*}
     */
    var getDetailTabsList = function(type, isPipelineRunning) {
      var tabsList = [],
        sdcExecutionMode = $rootScope.common.sdcExecutionMode;
      switch(type) {
        case pipelineConstant.PIPELINE:
          if(isPipelineRunning) {
            if(sdcExecutionMode === pipelineConstant.CLUSTER ) {
              tabsList = [summaryTab, infoTab, configurationTab, historyTab];
            } else {
              tabsList = [summaryTab, errorTab, infoTab, configurationTab, rulesTab, historyTab];
            }
          } else {
            tabsList = [infoTab, configurationTab, rulesTab, historyTab];
          }

          return tabsList;
        case pipelineConstant.STAGE_INSTANCE:
          if(isPipelineRunning) {
            if(sdcExecutionMode === pipelineConstant.CLUSTER ) {
              tabsList = [summaryTab, infoTab, configurationTab];
            } else {
              tabsList = [summaryTab, errorTab, infoTab, configurationTab];
            }
          } else {
            tabsList = [infoTab, configurationTab];
          }

          if($scope.detailPaneConfigDefn.rawSourceDefinition) {
            tabsList.push(rawPreviewTab);
          }

          return tabsList;
        case pipelineConstant.LINK:
          if(isPipelineRunning) {
            if(sdcExecutionMode === pipelineConstant.CLUSTER ) {
              return [dataRulesTab, infoTab];
            } else {
              return [dataSummaryTab, dataRulesTab, infoTab];
            }
          } else {
            return [infoTab, dataRulesTab];
          }
          break;
      }
    };

    angular.extend($scope, {
      detailPaneTabs: getDetailTabsList(pipelineConstant.PIPELINE, false),

      /**
       * Returns label for Detail Pane
       */
      getDetailPaneLabel: function() {
        var selectedType = $scope.selectedType,
          selectedObject = $scope.selectedObject;

        if(selectedObject) {
          switch(selectedType) {
            case pipelineConstant.PIPELINE:
              return selectedObject.info.name;
            case pipelineConstant.STAGE_INSTANCE:
              return selectedObject.uiInfo.label;
            case pipelineConstant.LINK:
              return 'Stream ( ' + selectedObject.source.uiInfo.label + ' - ' + selectedObject.target.uiInfo.label + ' )';
          }
        }
      },

      getStageLibraryLabel: function(stageInstance) {
        var nameLabelMap = _.find($scope.stageLibraryList, function(labelMap) {
         return stageInstance.library === labelMap.library;
        });

        return nameLabelMap.libraryLabel;
      },

      /**
       * Checks if configuration has any issue.
       *
       * @param {Object} stageInstance - The Pipeline Configuration/Stage Configuration Object.
       * @returns {Boolean} - Returns true if configuration has any issue otherwise false.
       */
      hasConfigurationIssues: function(stageInstance) {
        var config = $scope.pipelineConfig,
          commonErrors = $rootScope.common.errors,
          issuesMap,
          issues = [];


        if(commonErrors && commonErrors.length && commonErrors[0].pipelineIssues) {
          issuesMap = commonErrors[0];
        } else if(config && config.issues){
          issuesMap = config.issues;
        }

        if(issuesMap) {
          if(stageInstance.instanceName && issuesMap.stageIssues &&
            issuesMap.stageIssues[stageInstance.instanceName]) {
            issues = issuesMap.stageIssues[stageInstance.instanceName];
          } else if(!stageInstance.instanceName && issuesMap.pipelineIssues){
            issues.push.apply(issues, issuesMap.pipelineIssues);

            if(config.errorStage && issuesMap.stageIssues && issuesMap.stageIssues[config.errorStage.instanceName]) {
              issues.push.apply(issues, issuesMap.stageIssues[config.errorStage.instanceName]);
            }
          }
        }

        return _.find(issues, function(issue) {
          return issue.configName;
        });
      },

      /**
       * Returns true to display icon on tab.
       *
       * @param tab
       * @returns {*}
       */
      showWarning: function(tab) {
        if(tab.name === 'configuration') {
          return $scope.hasConfigurationIssues($scope.detailPaneConfig);
        }
        return false;
      },

      /**
       * Select the Rules Tab
       * @param triggeredAlert
       */
      selectRulesTab: function(triggeredAlert) {
        angular.forEach($scope.detailPaneTabs, function(tab) {
          if(tab.name === 'dataRules' || tab.name === 'metricAlertRules') {
            tab.active = true;
          }
        });
      },

      /**
       * Launch Contextual Help
       */
      launchHelp: function() {
        var helpId = '',
          selectedObject = $scope.selectedObject,
          activeTab = _.find($scope.detailPaneTabs, function(tab) {
            return tab.active;
          });

        switch($scope.selectedType) {
          case pipelineConstant.PIPELINE:
            if(activeTab.helpId) {
              helpId = activeTab.helpId;
            } else {
              helpId = 'pipeline-configuration';
            }
            break;
          case pipelineConstant.STAGE_INSTANCE:
            helpId = selectedObject.library + '@' + selectedObject.stageName + '@' + selectedObject.stageVersion;
            break;
          case pipelineConstant.LINK:
            helpId = activeTab.helpId;
        }

        contextHelpService.launchHelp(helpId);

      },

      /**
       * Launch Settings for Summary Tab
       */
      launchSettings: function() {
        $scope.$broadcast('launchSummarySettings');
      }
    });

    $scope.$on('onSelectionChange', function(event, options) {
      $scope.detailPaneTabs = getDetailTabsList(options.type, $scope.isPipelineRunning, options.selectedObject);

      if(options.detailTabName) {
        angular.forEach($scope.detailPaneTabs, function(tab) {
          if(tab.name === options.detailTabName) {
            tab.active = true;
          }
        });
      }

      if(options.type === pipelineConstant.LINK && !$scope.isPipelineRunning && $scope.detailPaneTabs.length > 1) {
        $scope.detailPaneTabs[1].active = true;
      }

      //To fix NVD3 JS errors - https://github.com/novus/nvd3/pull/396
      window.nv.charts = {};
      window.nv.graphs = [];
      window.nv.logs = {};
      window.onresize = null;
    });

    $scope.$watch('isPipelineRunning', function(newValue) {
      var tabs = $scope.detailPaneTabs = getDetailTabsList($scope.selectedType, newValue);

      if(newValue || $scope.detailPaneTabs.length < 2 ) {
        angular.forEach(tabs, function(tab) {
          tab.active = (tab.name === 'summary');
        });
      } else if($scope.detailPaneTabs.length > 1) {
        angular.forEach(tabs, function(tab) {
          tab.active = (tab.name === 'configuration');
        });
      }
    });

  });