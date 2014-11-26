/**
 * Controller for Summary Tab.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('SummaryController', function ($scope, $rootScope) {
    angular.extend($scope, {
      summaryCounters: {},
      summaryMeters: {},
      summaryTimer: {}
    });

    /**
     * Update Summary Tab Data
     */
    var updateSummaryData = function() {
      var timerProperty,
        currentSelection = $scope.detailPaneConfig,
        isStageSelected = (currentSelection && currentSelection.stages === undefined);

      //meters
      if(isStageSelected) {
        $scope.summaryMeters = {
          batchCount:
            $rootScope.common.pipelineMetrics.meters['pipeline.batchCount.meter'],
          inputRecords:
            $rootScope.common.pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.inputRecords.meter'],

          outputRecords:
            $rootScope.common.pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.outputRecords.meter'],

          errorRecords:
            $rootScope.common.pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.errorRecords.meter']
        };
      } else {
        $scope.summaryMeters = {
          batchCount:
            $rootScope.common.pipelineMetrics.meters['pipeline.batchCount.meter'],
          inputRecords:
            $rootScope.common.pipelineMetrics.meters['pipeline.batchInputRecords.meter'],

          outputRecords:
            $rootScope.common.pipelineMetrics.meters['pipeline.batchOutputRecords.meter'],

          errorRecords:
            $rootScope.common.pipelineMetrics.meters['pipeline.batchErrorRecords.meter']
        };
      }

      //timers
      timerProperty = 'pipeline.batchProcessing.timer';
      if(isStageSelected) {
        timerProperty = 'stage.' + currentSelection.instanceName + '.batchProcessing.timer';
      }

      $scope.summaryTimer = $rootScope.common.pipelineMetrics.timers[timerProperty];

      $scope.$broadcast('summaryDataUpdated');
    };

    $scope.$on('onStageSelection', function() {
      if($scope.isPipelineRunning && $rootScope.common.pipelineMetrics) {
        updateSummaryData();
      }
    });

    $rootScope.$watch('common.pipelineMetrics', function() {
      if($scope.isPipelineRunning && $rootScope.common.pipelineMetrics) {
        updateSummaryData();
      }
    });

  })

  .controller('RecordProcessedPieChart', function($scope) {
    var colorArray = ['#5cb85c', '#FF3333'];

    angular.extend($scope, {
      pieChartData: [],

      getLabel: function(){
        return function(d) {
          switch(d.key) {
            case 'goodRecords':
              return 'Good Records';
            case 'badRecords':
              return 'Bad Records';
          }
        };
      },

      getValue: function() {
        return function(d){
          return d.value;
        };
      },

      getColor: function() {
        return function(d, i) {
          return colorArray[i];
        };
      }
    });

    $scope.$on('summaryDataUpdated', function() {
      $scope.pieChartData = [
        {
          key: "goodRecords",
          value: $scope.summaryMeters.outputRecords.count || 1
        },
        {
          key: "badRecords",
          value: $scope.summaryMeters.errorRecords.count
        }
      ];
    });

  })

  .controller('RecordProcessedBarChart', function($scope) {
    var colorArray = ['#1f77b4', '#5cb85c', '#FF3333'];

    angular.extend($scope, {
      barChartData: [],

      getColor: function() {
        return function(d, i) {
          return colorArray[i];
        };
      }
    });

    $scope.$on('summaryDataUpdated', function() {
      $scope.barChartData = [
        {
          key: "Processed Records",
          values: [
            ["Input Records" , $scope.summaryMeters.inputRecords.count ],
            ["Output Records" , $scope.summaryMeters.outputRecords.count ],
            ["Bad Records" , $scope.summaryMeters.errorRecords.count ]
          ]
        }
      ];
    });

  });

