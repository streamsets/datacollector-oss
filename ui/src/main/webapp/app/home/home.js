/**
 * Home module for displaying home page content
 */

angular
  .module('pipelineAgentApp.home',[
    'ngRoute',
    'ngTagsInput',
    'jsonFormatter',
    'splitterDirectives',
    'tabDirectives',
    'pipelineGraphDirectives',
    'underscore'
  ])
  .config(['$routeProvider', function($routeProvider) {
    $routeProvider.when("/",
      {
        templateUrl: "app/home/home.tpl.html",
        controller: "HomeController"
      }
    );
  }])
  .controller('HomeController', function($scope, api, _) {
    $scope.isPipelineRunning = false;
    $scope.isPipelineInValid = true;
    $scope.stageLibraries = [];

    api.pipelineAgent.getStageLibrary().success(function(res){
      console.log('From GET /api/stage-library');
      console.log(res);
      $scope.stageLibraries = res;

      $scope.sources = _.filter(res, function(stageLibrary) {
        return (stageLibrary.type === 'SOURCE');
      });

      $scope.processors = _.filter(res, function(stageLibrary) {
        return (stageLibrary.type === 'PROCESSOR');
      });

      $scope.targets = _.filter(res, function(stageLibrary) {
        return (stageLibrary.type === 'TARGET');
      });

    });

    //Temp
    $scope.config = {
      source: {
        name: 'File Source'
      }
    };

    api.pipelineAgent.getConfig().success(function (res) {
      //$scope.config = res;
      console.log($scope.config);
    });


    $scope.attributes = [{
      name: 'first',
      type: 'String'
    },{
      name: 'last',
      type: 'String'
    },{
      name: 'ssn',
      type: 'String'
    },{
      name: 'address1',
      type: 'String'
    },{
      name: 'address2',
      type: 'String'
    },{
      name: 'city',
      type: 'String'
    },{
      name: 'state',
      type: 'String'
    },{
      name: 'zip',
      type: 'String'
    },{
      name: 'phone',
      type: 'String'
    }];

    $scope.preview = {
      first: "John",
      last: "Smith",
      ssn: "xxx-xx-xxxx",
      address1: "1234 Main St",
      address2: "APT #567",
      city: "San Francisco",
      state: "CA",
      zip: "94014",
      phone: "650-123-4567"
    };

    $scope.createPipelineAgent = function() {
      $scope.config = {
        source: {
          name: 'File Source'
        }
      };
    };

    /*var xLoc = 200,
      yLoc = 100;

    var nodes = [{
      label: 'Log File Source',
      id: 100,
      x: xLoc,
      y: yLoc
    },{
      label: 'Field Type Converter',
      id: 101,
      x: xLoc + 300,
      y: yLoc
    },{
      label: 'HBase Target',
      id: 102,
      x: xLoc + 300 + 300,
      y: yLoc
    }];

    var edges = [{
      source: nodes[0],
      target: nodes[1]
    }, {
      source: nodes[1],
      target: nodes[2]
    }];*/

    $scope.pipelineConfig = {
      nodes : [],
      edges: []
    };

    var pipelineConfig = {
      label : 'Pipeline Configuration'
    };

    $scope.detailPaneConfig = pipelineConfig;

    $scope.$on('onNodeSelection', function(event, selectedNode){
      $scope.detailPaneConfig = selectedNode;

      console.log(selectedNode);
    });

    $scope.$on('onRemoveNodeSelection', function(){
      $scope.detailPaneConfig = pipelineConfig;
    });

    $scope.addStage = function(stage) {
      $scope.$broadcast('addNode', stage);
      $scope.detailPaneConfig = stage;
    };

  });