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
        'errors' :'#d62728'
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
        return function(d) {
          return d.color;
        };
      },

      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          return '<p>' + key + ' </p><p>' + x + ': ' + y +'</p>';
        };
      }
    });


    $scope.$on('summaryDataUpdated', function() {
      var list = [];

      angular.forEach($scope.histogramList, function(recordType) {
        var data = $scope.summaryHistograms[recordType];
        list.push({
          key: label[recordType],
          values: [
            ["Mean" , data.mean ],
            ["Std Dev" , data.stddev ],
            ["99.9%" , data.p999 ],
            ["99%" , data.p99 ],
            ["98%" , data.p98 ],
            ["95%" , data.p95 ],
            ["75%" , data.p75 ],
            ["50%" , data.p50 ]
          ],
          color: color[recordType]
        });
      });

      $scope.timerData = list;

    });

  });