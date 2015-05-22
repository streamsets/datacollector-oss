/**
 * Controller for Batch Timer Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('AllStageBatchTimerChartController', function($rootScope, $scope) {
    angular.extend($scope, {
      chartData: [],

      getLabel: function(){
        return function(d) {
          return d.key;
        };
      },

      getValue: function() {
        return function(d){
          return d.value.toFixed(2);
        };
      },

      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          return '<p>' + key + '</p><p>' + y.value +  ' seconds</p>';
        };
      }
    });

    var stages = $scope.pipelineConfig.stages;

    angular.forEach(stages, function(stage) {
      $scope.chartData.push({
        instanceName: stage.instanceName,
        key: stage.uiInfo.label,
        value: 0
      });
    });

    $scope.$on('summaryDataUpdated', function() {
      var pipelineMetrics = $rootScope.common.pipelineMetrics,
        values = [];

      if(!pipelineMetrics.timers) {
        return;
      }

      angular.forEach($scope.chartData, function(data) {
        var stageTimer = pipelineMetrics.timers['stage.' + data.instanceName + '.batchProcessing.timer'];
        if(stageTimer) {
          data.value = stageTimer.mean;
        }
        values.push(data);
      });

      $scope.chartData = values;
      $scope.totalValue = (pipelineMetrics.timers['pipeline.batchProcessing.timer'].mean).toFixed(2);

    });

  });