/**
 * Controller for Detail Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('DetailController', function ($scope, $rootScope, _, pipelineConstant, api) {
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
        iconClass: 'fa fa-gear',
        active: true
      },
      rawPreviewTab = {
        name:'rawPreview',
        template:'app/home/detail/rawPreview/rawPreview.tpl.html',
        iconClass: 'fa fa-eye'
      },
      summaryTab = {
        name:'summary',
        template:'app/home/detail/summary/summary.tpl.html',
        iconClass: 'fa fa-bar-chart'
      },
      errorTab = {
        name:'errors',
        template:'app/home/detail/badRecords/badRecords.tpl.html',
        iconClass: 'fa fa-exclamation-triangle'
      },
      dataSummaryTab = {
        name:'summary',
        template:'app/home/detail/dataSummary/dataSummary.tpl.html',
        iconClass: 'fa fa-bar-chart',
        active: true
      },
      alertsTab = {
        name:'alerts',
        template:'app/home/detail/alerts/alerts.tpl.html',
        iconClass: 'glyphicon glyphicon-exclamation-sign'
      },
      rulesTab = {
        name:'rules',
        template:'app/home/detail/rules/rules.tpl.html',
        iconClass: 'fa fa-list'
      };

    /**
     * Returns list tabs based on type.
     *
     * @param type
     * @param isPipelineRunning
     * @returns {*}
     */
    var getDetailTabsList = function(type, isPipelineRunning) {
      var tabsList = [];
      switch(type) {
        case pipelineConstant.PIPELINE:
          if(isPipelineRunning) {
            tabsList = [summaryTab, errorTab, infoTab, configurationTab, rulesTab, historyTab];
          } else {
            tabsList = [infoTab, configurationTab, rulesTab, historyTab];
          }

          return tabsList;
        case pipelineConstant.STAGE_INSTANCE:
          if(isPipelineRunning) {
            tabsList = [summaryTab, errorTab, infoTab, configurationTab];
          } else {
            tabsList = [infoTab, configurationTab];
          }

          if($scope.detailPaneConfigDefn.rawSourceDefinition) {
            tabsList.push(rawPreviewTab);
          }

          return tabsList;
        case pipelineConstant.LINK:
          if(isPipelineRunning) {
            return [dataSummaryTab, rulesTab, infoTab];
          } else {
            return [infoTab, rulesTab];
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

      /**
       * Checks if configuration has any issue.
       *
       * @param {Object} configObject - The Pipeline Configuration/Stage Configuration Object.
       * @returns {Boolean} - Returns true if configuration has any issue otherwise false.
       */
      hasConfigurationIssues: function(stageInstance) {
        var config = $scope.pipelineConfig,
          issues;

        if(config && config.issues) {
          if(stageInstance.instanceName && config.issues.stageIssues &&
            config.issues.stageIssues && config.issues.stageIssues[stageInstance.instanceName]) {
            issues = config.issues.stageIssues[stageInstance.instanceName];
          } else if(config.issues.pipelineIssues){
            issues = config.issues.pipelineIssues;
          }
        }

        return _.find(issues, function(issue) {
          return issue.level === 'STAGE_CONFIG';
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
       * Delete Triggered Alert
       */
      deleteTriggeredAlert: function(triggeredAlert) {
        api.pipelineAgent.deleteAlert($scope.pipelineConfig.info.name, triggeredAlert.rule.id)
          .success(function() {
            //Alert deleted successfully
          })
          .error(function(data, status, headers, config) {
            $rootScope.common.errors = [data];
          });
      },

      /**
       * Select the Rules Tab
       * @param triggeredAlert
       */
      selectRulesTab: function(triggeredAlert) {
        angular.forEach($scope.detailPaneTabs, function(tab) {
          if(tab.name === 'rules') {
            tab.active = true;
          }
        });
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

      //To fix NVD3 JS errors - https://github.com/novus/nvd3/pull/396
      window.nv.charts = {};
      window.nv.graphs = [];
      window.nv.logs = {};
      window.onresize = null;
    });

    $scope.$watch('isPipelineRunning', function(newValue) {
      $scope.detailPaneTabs = getDetailTabsList($scope.selectedType, newValue);

      if(newValue || $scope.detailPaneTabs.length < 2 ) {
        $scope.detailPaneTabs[0].active = true;
      } else if($scope.detailPaneTabs.length > 1) {
        $scope.detailPaneTabs[1].active = true;
      }

    });

    $scope.$on('showBadRecordsSelected', function() {
      angular.forEach($scope.detailPaneTabs, function(tab) {
        if(tab.name === 'errors') {
          tab.active = true;
        }
      });
    });


  });