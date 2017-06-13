/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
module.exports = function(grunt) {

  grunt.loadNpmTasks('grunt-protractor-runner');
  grunt.loadNpmTasks('grunt-shell');

  grunt.initConfig({
    shell: {
      dockerBuild: {
        command: 'docker build -t streamsets/sdc ../release'
      },
      dockerComposeUp: {
        command: 'docker-compose up -d'
      },
      dockerComposeStop: {
        command: 'docker-compose stop'
      }
    },
    protractor: {
      options: {
        configFile: "src/test/protractor.conf.js",
        keepAlive: false,
        noColor: false,
        args: {
          baseUrl: 'http://localhost:18630/'
        }
      },
      docker: {
        options: {
          args: {
            baseUrl: 'http://192.168.59.103:18630/'
          }
        }
      },
      chrome: {
        options: {
          args: {
            baseUrl: 'http://localhost:18630/',
            browser: 'chrome',
            suite: 'restAPI,ui'
          }
        }
      },
      safari: {
        options: {
          args: {
            baseUrl: 'http://localhost:18630/',
            browser: 'safari',
            suite: 'restAPI,ui'
          }
        }
      },
      firefox: {
        options: {
          args: {
            baseUrl: 'http://localhost:18630/',
            browser: 'firefox',
            suite: 'restAPI,ui'
          }
        }
      },
      phantomjs: {
        options: {
          args: {
            baseUrl: 'http://localhost:18630/',
            browser: 'phantomjs',
            suite: 'restAPI'
          }
        }
      },
      clean: {
        options: {
          args: {
            baseUrl: 'http://localhost:18630/',
            browser: 'chrome',
            suite: 'clean'
          }
        }
      }
    }
  });

  /*grunt.registerTask( 'test', ['shell:dockerBuild', 'shell:dockerComposeUp', 'protractor:docker',
    'shell:dockerComposeStop']);*/

  grunt.registerTask( 'chrome', ['protractor:chrome']);

  grunt.registerTask( 'firefox', ['protractor:firefox']);

  grunt.registerTask( 'safari', ['protractor:safari']);

  grunt.registerTask( 'phantomjs', ['protractor:phantomjs']);

  grunt.registerTask( 'clean', ['protractor:clean']);

  grunt.registerTask( 'test', [
    'protractor:clean', 'protractor:chrome',
    'protractor:clean', 'protractor:firefox',
    'protractor:clean', 'protractor:safari']);

};


