/**
 * Controller for Record Processed Bar Chart.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('RecordProcessedBarChartController', function($scope) {
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