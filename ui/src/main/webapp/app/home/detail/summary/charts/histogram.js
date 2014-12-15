/**
 * Controller for Histogram Chart.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('HistogramChartController', function($scope, $translate) {

    var recordType,
      label = {
        'inputRecords' : 'Input Records Per Batch',
        'outputRecords': 'Output Records Per Batch',
        'errorRecords' : 'Error Records Per Batch',
        'errors' :  'Errors Per Batch'
      },
      color = {
        'inputRecords' :'#1f77b4',
        'outputRecords': '#5cb85c',
        'errorRecords' :'#FF3333',
        'errors' :'#FF3333'
      };


    angular.forEach(label, function(value, key) {
      $translate('home.detailPane.summaryTab.histogram.' + key).then(function(translation) {
        label[key] = [translation];
      });
    });

    angular.extend($scope, {
      histogramData: [],
      timerData:[],

      init: function(type) {
        recordType = type;
      },

      getColor: function() {
        return function() {
          return color[recordType];
        };
      }
    });


    $scope.$on('summaryDataUpdated', function() {
      var data = $scope.summaryHistograms[recordType];

      if(data) {
        $scope.timerData = [
          {
            key: label[recordType],
            values: [
              ["99.9%" , data.p999 ],
              ["99%" , data.p99 ],
              ["98%" , data.p98 ],
              ["95%" , data.p95 ],
              ["75%" , data.p75 ],
              ["50%" , data.p50 ]
            ]
          }
        ];
      }
    });

  });