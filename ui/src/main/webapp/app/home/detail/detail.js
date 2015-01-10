/**
 * Controller for Detail Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('DetailController', function ($scope, $rootScope, _, pipelineConstant) {
    var
      pipelineTabsEdit = [
        {
          name:'configuration',
          template:'app/home/detail/configuration/configuration.tpl.html',
          iconClass: 'fa fa-gear fa-12x'
        },
        {
          name:'history',
          template:'app/home/detail/history/history.tpl.html',
          iconClass: 'fa fa-history fa-12x'
        }
      ],
      pipelineTabsRunning = [
        {
          name:'summary',
          template:'app/home/detail/summary/summary.tpl.html',
          iconClass: 'fa fa-bar-chart fa-12x'
        },
        {
          name:'errors',
          template:'app/home/detail/badRecords/badRecords.tpl.html',
          iconClass: 'fa fa-exclamation-triangle fa-12x'
        },
        {
          name:'configuration',
          template:'app/home/detail/configuration/configuration.tpl.html',
          iconClass: 'fa fa-gear fa-12x'
        },
        {
          name:'history',
          template:'app/home/detail/history/history.tpl.html',
          iconClass: 'fa fa-history fa-12x'
        }
      ],
      stageTabsEdit = [{
        name:'configuration',
        template:'app/home/detail/configuration/configuration.tpl.html',
        iconClass: 'fa fa-gear fa-12x'
      }],
      stageTabsRunning = [
        {
          name:'summary',
          template:'app/home/detail/summary/summary.tpl.html',
          iconClass: 'fa fa-bar-chart fa-12x'
        },
        {
          name:'errors',
          template:'app/home/detail/badRecords/badRecords.tpl.html',
          iconClass: 'fa fa-exclamation-triangle fa-12x'
        },
        {
          name:'configuration',
          template:'app/home/detail/configuration/configuration.tpl.html',
          iconClass: 'fa fa-gear fa-12x'
        }
      ],
      linkTabsRunning = [
        {
          name:'summary',
          template:'app/home/detail/dataSummary/dataSummary.tpl.html',
          iconClass: 'fa fa-bar-chart fa-12x'
        },
        {
          name:'alerts',
          template:'app/home/detail/alerts/alerts.tpl.html',
          iconClass: 'glyphicon glyphicon-exclamation-sign fa-12x'
        },
        {
          name:'rules',
          template:'app/home/detail/rules/rules.tpl.html',
          iconClass: 'fa fa-list fa-12x'
        },
        {
          name:'configuration',
          template:'app/home/detail/configuration/configuration.tpl.html',
          iconClass: 'fa fa-gear fa-12x'
        }
      ];

    angular.extend($scope, {
      detailPaneTabs: pipelineTabsEdit,

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
              return 'Link ( ' + selectedObject.source.uiInfo.label + ' - ' + selectedObject.target.uiInfo.label + ' )';
          }
        }
      },

      /**
       * Checks if configuration has any issue.
       *
       * @param {Object} configObject - The Pipeline Configuration/Stage Configuration Object.
       * @returns {Boolean} - Returns true if configuration has any issue otherwise false.
       */
      hasConfigurationIssues: function(configObject) {
        var config = $scope.pipelineConfig,
          issues;

        if(config && config.issues) {
          if(configObject.instanceName && config.issues.stageIssues &&
            config.issues.stageIssues && config.issues.stageIssues[configObject.instanceName]) {
            issues = config.issues.stageIssues[configObject.instanceName];
          } else if(config.issues.pipelineIssues){
            issues = config.issues.pipelineIssues;
          }
        }

        return _.find(issues, function(issue) {
          return issue.level === 'STAGE_CONFIG';
        });
      },

      showWarning: function(tab) {
        if(tab.name === 'configuration') {
          return $scope.hasConfigurationIssues($scope.detailPaneConfig);
        }
        return false;
      }
    });

    var getDetailTabsList = function(type, isPipelineRunning) {
      switch(type) {
        case pipelineConstant.PIPELINE:
          if(isPipelineRunning) {
            return pipelineTabsRunning;
          } else {
            return pipelineTabsEdit;
          }
          break;
        case pipelineConstant.STAGE_INSTANCE:
          if(isPipelineRunning) {
            return stageTabsRunning;
          } else {
            return stageTabsEdit;
          }
          break;
        case pipelineConstant.LINK:
          if(isPipelineRunning) {
            return linkTabsRunning;
          } else {
            return stageTabsEdit;
          }
          break;
      }
    };

    $scope.$on('onSelectionChange', function(event, selectedObject, type) {
      $scope.detailPaneTabs = getDetailTabsList(type, $scope.isPipelineRunning);
    });

    $scope.$watch('isPipelineRunning', function(newValue) {
      $scope.detailPaneTabs = getDetailTabsList($scope.selectedType, newValue);
    });

    $scope.$on('showBadRecordsSelected', function() {
      angular.forEach($scope.detailPaneTabs, function(tab) {
        if(tab.name === 'errors') {
          tab.active = true;
        }
      });
    });


  });