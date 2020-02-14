## Development Requirements

|Dependency|OS X Installation|
|:--|:--|
|node.js|`brew install nodejs`|
|gulp|`npm install -g bower`|
|jspm|`npm install -g grunt-cli`|

## Development

### Live Reload

You will first need the backend running, which you can learn about in [BUILD.md](BUILD.md). But to have the front-end
load your updated code automatically, you need to add an extra Java option before starting it.

```
export SDC_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=1044 -Dsdc.static-web.dir=<path_to_root>/datacollector/datacollector-ui/target/dist"
dist/target/streamsets-datacollector-3.14.0-SNAPSHOT/streamsets-datacollector-3.14.0-SNAPSHOT/bin/streamsets dc
```

If you are in the datacollector-ui folder, this is

```
export SDC_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=1044 -Dsdc.static-web.dir=$(pwd)/target/dist"
../dist/target/streamsets-datacollector-3.14.0-SNAPSHOT/streamsets-datacollector-3.14.0-SNAPSHOT/bin/streamsets dc
```

Then run

`grunt watch`

### Production build

`mvn clean install`