# Sequel-drill Adapter

This Sequel adapter supports a subset of Drill features, including:

* querying files using all Sequel queries/operators (although queries against Hive tables have not been tested)
* deleting files

For now, the following features are not supported by this adapter:

* Importing files/`create table as` (although you can generate these queries by hand and execute them with a `@db.run("[CREATE TABLE dfs.workspace.`name` AS SELECT...]")` command) 
* Geospatial queries have not been tested, but will be fully supported in the future
* Hive specific features (e.g. DESCRIBE)

### Docker usage

A Docker image called "drill-hdfs" (link to Dockerhub) has been created for testing purposes, consisting of both Apache Drill and a bare-bones Hadoop service including the HDFS file system. If you wish to use the HDFS file system with Drill for testing purposes, you will need to update Drill's DFS storage plugin config [here](http://localhost:8047/storage/dfs). The "connection" field should be set to `hdfs://localhost:8020/`

