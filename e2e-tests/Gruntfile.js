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
      local: {
        options: {
          args: {
            baseUrl: 'http://localhost:18630/'
          }
        }
      }
    }
  });

  grunt.registerTask( 'test', ['shell:dockerBuild', 'shell:dockerComposeUp', 'protractor:docker',
    'shell:dockerComposeStop']);

  grunt.registerTask( 'local', ['protractor:local']);
};


