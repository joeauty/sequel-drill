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

If you wish to use the `tmp` workspace you'll also need to create this directory. You can do so as follows:

```
docker exec -it [drill image id] hdfs dfs -mkdir /tmp
```

### Known issues/caveats/things

* Numbers in Drill are enclosed in quotation marks within its JSON output. If this is an issue you might need to cast within your application, this adapter will not attempt to manipulate output, with one exception (below)
* Drill normally returns query results in a JSON array entitled "rows" and includes the column names in a JSON array entitled "columns". A lot of applications expect a more conventional SQL-ey output where column names aren't included, so we are omitting the columns in responses returned to ensure greater compability
* Generated queries are manipulated to do the following:
  * strip quotation marks
  * generate Drill workspaces (i.e. dfs.[workspace].[filename])
  * fix aggregate queries (count/sum/min/max/avg) for Drill compatability
  * replace `!=` operators with `<>`
  
Sorry, the query manipulation is pretty hacky, but it works. Please let me know if you know of a better way to do this.