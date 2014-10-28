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
    'pipelineGraphDirectives'
  ])
  .config(['$routeProvider', function($routeProvider){
    $routeProvider.when("/",
      {
        templateUrl: "app/home/home.tpl.html",
        controller: "HomeController"
      }
    );
  }])
  .controller('HomeController', function($scope, api){

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

    var xLoc = 200,
      yLoc = 100;

    var nodes = [{
      title: 'File Source',
      id: 100,
      x: xLoc,
      y: yLoc
    },{
      title: 'PII Processor',
      id: 101,
      x: xLoc + 300,
      y: yLoc
    },{
      title: 'Kafka Target',
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
    }];

    $scope.pipelineConfig = {
      nodes : nodes,
      edges: edges
    };
  });