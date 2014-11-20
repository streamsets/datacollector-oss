describe('Controller: modules/home/HomeCtrl', function () {
  var $rootScope, $scope, $controller, $httpBackend, mockedApi;


  beforeEach(module('pipelineAgentApp'));

  beforeEach(inject(function (_$rootScope_, _$controller_, _$httpBackend_, api, _) {
    $rootScope = _$rootScope_;
    $scope = $rootScope.$new();
    $controller = _$controller_;
    $httpBackend = _$httpBackend_;
    mockedApi = api;

    $httpBackend.expectGET('i18n/en.json').respond({});
    $httpBackend.expectGET('rest/v1/definitions').respond({
      pipeline: [
        {
          configDefinitions: [
            {
              name: "deliveryGuarantee",
              type: "MODEL",
              label: "Delivery Guarantee",
              description: "The delivery guarantee option",
              required: true,
              group: "",
              fieldName: "",
              model: {
                modelType: "DROPDOWN",
                fieldModifierType: "PROVIDED",
                valuesProviderClass: "",
                values: [
                  "ATLEAST_ONCE",
                  "ATMOST_ONCE"
                ],
                labels: [
                  "ATLEAST ONCE",
                  "ATMOST ONCE"
                ]
              },
              defaultValue: "ATLEAST_ONCE"
            },
            {
              name: "Stop On Error",
              type: "BOOLEAN",
              label: "Stop on Error",
              description: "The Stop on Error option",
              required: true,
              group: "",
              fieldName: "",
              model: null,
              defaultValue: "true"
            }
          ]
        }
      ],
      stages: [
        {
          className: "com.streamsets.pipeline.lib.basics.FieldTypeConverterProcessor",
          name: "fieldTypeConverter",
          version: "1.0.0",
          label: "Field Type Converter",
          description: "",
          type: "PROCESSOR",
          configDefinitions: [
            {
              name: "fieldsToConvert",
              type: "MODEL",
              label: "Fields to convert",
              description: "",
              required: true,
              group: "",
              fieldName: "fields",
              model: {
                modelType: "FIELD_MODIFIER",
                fieldModifierType: "PROVIDED",
                valuesProviderClass: "com.streamsets.pipeline.lib.basics.ConverterValuesProvider",
                values: [
                  "BOOLEAN",
                  "CHAR",
                  "BYTE",
                  "SHORT",
                  "INTEGER",
                  "LONG",
                  "FLOAT",
                  "DOUBLE",
                  "DATE",
                  "DATETIME",
                  "DECIMAL",
                  "STRING",
                  "BYTE_ARRAY"
                ],
                labels: [
                  "BOOLEAN",
                  "CHAR",
                  "BYTE",
                  "SHORT",
                  "INTEGER",
                  "LONG",
                  "FLOAT",
                  "DOUBLE",
                  "DATE",
                  "DATETIME",
                  "DECIMAL",
                  "STRING",
                  "BYTE_ARRAY"
                ]
              },
              defaultValue: ""
            }
          ],
          onError: "DROP_RECORD",
          icon: "",
          library: "streamsets-basic"
        },
        {
          className: "com.streamsets.pipeline.lib.basics.FieldRemoverProcessor",
          name: "fieldRemover",
          version: "1.0.0",
          label: "Field Remover",
          description: "",
          type: "PROCESSOR",
          configDefinitions: [
            {
              name: "fieldsToRemove",
              type: "MODEL",
              label: "Fields to remove",
              description: "",
              required: true,
              group: "",
              fieldName: "fields",
              model: {
                modelType: "FIELD_SELECTOR",
                fieldModifierType: null,
                valuesProviderClass: null,
                values: null,
                labels: null
              },
              defaultValue: ""
            }
          ],
          onError: "DROP_RECORD",
          icon: "",
          library: "streamsets-basic"
        },
        {
          className: "com.streamsets.pipeline.lib.basics.RandomSource",
          name: "randomSource",
          version: "1.0.0",
          label: "Random Record Source",
          description: "",
          type: "SOURCE",
          configDefinitions: [
            {
              name: "recordFields",
              type: "STRING",
              label: "Record fields to generate, comma separated",
              description: "",
              required: true,
              group: "",
              fieldName: "fields",
              model: null,
              defaultValue: ""
            }
          ],
          onError: "DROP_RECORD",
          icon: "",
          library: "streamsets-basic"
        },
        {
          className: "com.streamsets.pipeline.lib.basics.IdentityProcessor",
          name: "identityProcessor",
          version: "1.0.0",
          label: "Identity",
          description: "It echoes every record it receives preserving the lanes",
          type: "PROCESSOR",
          configDefinitions: [ ],
          onError: "DROP_RECORD",
          icon: "",
          library: "streamsets-basic"
        },
        {
          className: "com.streamsets.pipeline.lib.basics.HbaseTarget",
          name: "hbaseTarget",
          version: "1.0.0",
          label: "Hbase Target",
          description: "",
          type: "TARGET",
          configDefinitions: [
            {
              name: "uri",
              type: "STRING",
              label: "Hbase URI",
              description: "Hbase server URI",
              required: true,
              group: "",
              fieldName: "uri",
              model: null,
              defaultValue: ""
            },
            {
              name: "security",
              type: "BOOLEAN",
              label: "Security enabled",
              description: "Kerberos enabled for Hbase",
              required: true,
              group: "",
              fieldName: "security",
              model: null,
              defaultValue: "false"
            },
            {
              name: "table",
              type: "STRING",
              label: "Table",
              description: "Hbase table name",
              required: true,
              group: "",
              fieldName: "table",
              model: null,
              defaultValue: ""
            }
          ],
          onError: "DROP_RECORD",
          icon: "HbaseTarget.svg",
          library: "streamsets-basic"
        },
        {
          className: "com.streamsets.pipeline.lib.basics.LogSource",
          name: "logSource",
          version: "1.0.1",
          label: "Log files Source",
          description: "",
          type: "SOURCE",
          configDefinitions: [
            {
              name: "logsDir",
              type: "STRING",
              label: "Logs directory",
              description: "",
              required: true,
              group: "",
              fieldName: "logsDir",
              model: null,
              defaultValue: ""
            },
            {
              name: "rotationFreq",
              type: "INTEGER",
              label: "Rotation frequency (in hr)",
              description: "",
              required: true,
              group: "",
              fieldName: "rotationFrequency",
              model: null,
              defaultValue: ""
            }
          ],
          onError: "DROP_RECORD",
          icon: "",
          library: "streamsets-basic"
        },
        {
          className: "com.streamsets.pipeline.lib.basics.NullTarget",
          name: "nullTarget",
          version: "1.0.0",
          label: "Null",
          description: "It discards all records",
          type: "TARGET",
          configDefinitions: [ ],
          onError: "DROP_RECORD",
          icon: "",
          library: "streamsets-basic"
        }
      ]
    });

    $httpBackend.expectGET('rest/v1/pipelines').respond(
      [ {
        "name" : "xyz",
        "description" : "asdsad",
        "created" : 1416442346879,
        "lastModified" : 1416442355947,
        "creator" : "nobody",
        "lastModifier" : "nobody",
        "lastRev" : "0",
        "uuid" : "b8b47507-56f3-4478-87ea-9e59865b6458",
        "valid" : true
      } ]);

    $httpBackend.expectGET('rest/v1/pipelines/xyz').respond({
      uuid: "cdf08ac9-2a97-4167-8a2c-ed48cfcf600e",
      info: {
        name: 'xyz'
      },
      stages: [
        {
          instanceName: "Log files Source1",
          library: "streamsets-basic",
          stageName: "logSource",
          stageVersion: "1.0.1",
          configuration: [
            {
              name: "logsDir",
              value: null
            },
            {
              name: "rotationFreq",
              value: null
            }
          ],
          uiInfo: {
            xPos: 200,
            yPos: 70,
            inputConnectors: [ ],
            outputConnectors: [
              "01"
            ]
          },
          inputLanes: [ ],
          outputLanes: [
            "Log files Source1outputLane"
          ]
        },
        {
          instanceName: "Field Type Converter2",
          library: "streamsets-basic",
          stageName: "fieldTypeConverter",
          stageVersion: "1.0.0",
          configuration: [
            {
              name: "fieldsToConvert",
              value: null
            }
          ],
          uiInfo: {
            xPos: 500,
            yPos: 70,
            inputConnectors: [
              "i1"
            ],
            outputConnectors: [
              "01"
            ]
          },
          inputLanes: [
            "Log files Source1outputLane"
          ],
          outputLanes: [
            "Field Type Converter2outputLane"
          ]
        },
        {
          instanceName: "Hbase Target3",
          library: "streamsets-basic",
          stageName: "hbaseTarget",
          stageVersion: "1.0.0",
          configuration: [
            {
              name: "uri",
              value: null
            },
            {
              name: "security",
              value: null
            },
            {
              name: "table",
              value: null
            }
          ],
          uiInfo: {
            xPos: 800,
            yPos: 70,
            inputConnectors: [
              "i1"
            ],
            outputConnectors: [ ]
          },
          inputLanes: [
            "Field Type Converter2outputLane"
          ],
          outputLanes: [ ]
        }
      ],
      onError: "DROP_RECORD",
      issues: {
        "Log files Source1": [
          "Instance 'Log files Source1', stage requires a configuration value for 'logsDir'",
          "Instance 'Log files Source1', stage requires a configuration value for 'rotationFreq'"
        ],
        "Field Type Converter2": [
          "Instance 'Field Type Converter2', stage requires a configuration value for 'fieldsToConvert'"
        ],
        "Hbase Target3": [
          "Instance 'Hbase Target3', stage requires a configuration value for 'uri'",
          "Instance 'Hbase Target3', stage requires a configuration value for 'security'",
          "Instance 'Hbase Target3', stage requires a configuration value for 'table'"
        ]
      },
      valid: false
    });

    $controller('HomeController', {
      '$rootScope': $rootScope,
      '$scope': $scope,
      'api': mockedApi,
      '_': _
    });
  }));

  afterEach(function () {
    $httpBackend.verifyNoOutstandingExpectation();
    $httpBackend.verifyNoOutstandingRequest();
  });

  it('should make home menu item active.', function () {
    expect($rootScope.common.active.home === 'active').toBeTruthy();
    $httpBackend.flush();
  });

  it('should call the getStageLibrary api function to fetch all config definitions of Pipeline & Stage Libraries', function () {
    $httpBackend.flush();

    expect($scope.stageLibraries).toBeDefined();
    expect($scope.stageLibraries.length).toEqual(7);

    expect($scope.sources).toBeDefined();
    expect($scope.sources.length).toEqual(2);

    expect($scope.processors).toBeDefined();
    expect($scope.processors.length).toEqual(3);

    expect($scope.targets).toBeDefined();
    expect($scope.targets.length).toEqual(2);

  });

  it('should call the getPipelineConfig api function to get Pipeline Configuration', function () {
    expect($scope.pipelineConfig).toBeUndefined();

    $httpBackend.flush();

    expect($scope.pipelineConfig).toBeDefined();
    expect($scope.pipelineConfig.stages.length).toEqual(3);

    //TODO: Fix issuesLength issue
    //expect($scope.issuesLength).toBeDefined();
    //expect($scope.issuesLength).toEqual(6);
  });


  it('should set detail pane configuration to Pipeline Configuration', function() {
    $httpBackend.flush();
    expect($scope.detailPaneConfig).toEqual($scope.pipelineConfig);
  });


  it('should add new instance of Source Stage when it is selected from dropdown', function() {
    $httpBackend.flush();
    expect($scope.pipelineConfig.stages.length).toEqual(3);

    spyOn($scope, '$broadcast');
    $scope.addStageInstance($scope.sources[0]);

    //expect($scope.$broadcast).toHaveBeenCalledWith('addNode');
  });


  //Clicking on Source/Processor/Target dropdown should create new stage instance object based on stage definition.

  //Changing/Dragging the stage instance in Graph should save the pipeline graph

  //Connection to stage instances to save the pipeline graph

  //Deleting stage instance should save the pipeline graph

  //


  it('should call the savePipelineConfig api function to save pipeline configuration whenever there is change in model', function () {
    $httpBackend.flush();
  });


});