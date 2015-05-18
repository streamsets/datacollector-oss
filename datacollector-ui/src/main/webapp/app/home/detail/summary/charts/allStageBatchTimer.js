/**
 * Controller for Batch Timer Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('AllStageBatchTimerChartController', function($rootScope, $scope) {
    angular.extend($scope, {
      chartData: [],

      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          return '<p>Batches Timer</p><p>' + $scope.getDurationLabel(x) + ' mean: ' + e.value.toFixed(2) + '</p>';
        };
      }
    });

    $scope.$on('summaryDataUpdated', function() {
      var pipelineMetrics = $rootScope.common.pipelineMetrics,
        stages = $scope.pipelineConfig.stages,
        values = [];

      if(!pipelineMetrics.timers) {
        return;
      }

      angular.forEach(stages, function(stage) {
        var stageTimer = pipelineMetrics.timers['stage.' + stage.instanceName + '.batchProcessing.timer'];

        if(stageTimer) {
          values.push([stage.uiInfo.label,
            stageTimer.mean
          ]);
        }
      });

      $scope.chartData = [
        {
          key: "Batch Throughput",
          values: values
        }
      ];
    });
  });