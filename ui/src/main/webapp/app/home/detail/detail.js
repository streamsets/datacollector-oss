/**
 * Controller for Detail Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('DetailController', function ($scope, $rootScope, _) {
    var detailTabsInEditMode = [{
        name:'configuration',
        template:'app/home/detail/configuration/configuration.tpl.html'
      }
      ],
      detailTabsInRunningMode = [
        {
          name:'summary',
          template:'app/home/detail/summary/summary.tpl.html'
        },
        {
          name:'errors',
          template:'app/home/detail/badRecords/badRecords.tpl.html'
        },
        {
          name:'alerts',
          template:'app/home/detail/alerts/alerts.tpl.html'
        },
        {
          name:'rules',
          template:'app/home/detail/rules/rules.tpl.html'
        },
        {
          name:'configuration',
          template:'app/home/detail/configuration/configuration.tpl.html'
        }
      ];

    angular.extend($scope, {
      detailPaneTabs: detailTabsInEditMode,

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

    $scope.$watch('isPipelineRunning', function(newValue) {
      if(newValue) {
        $scope.detailPaneTabs = detailTabsInRunningMode;
      } else {
        $scope.detailPaneTabs = detailTabsInEditMode;
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