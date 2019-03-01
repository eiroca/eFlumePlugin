# Flume plugin

Flume pluigin

Compilation and packaging
----------
```
  $ mvn package
```

Deployment
----------

Copy eFlumePlugin-<version>.jar in target folder into flume plugins dir folder
```
  $ mkdir -p $FLUME_HOME/plugins.d/eFlumePlugin/lib
  $ cp eFlumePlugin-0.0.3.jar $FLUME_HOME/plugins.d/eFlumePlugin/lib
```

