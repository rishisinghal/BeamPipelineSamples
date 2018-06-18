Beam Data Samples 
==================

### Samples for Apache Beam/Dataflow 

- StarterPipelinePubSub - Read from Pub/Sub the device telemetry data and write it to BigQuery and Bigtable. Avro is used to define the data schema.


- StarterPipelineMySQL - Read from MySQL database and write to BigQuery using JDBCIO. 


- StarterPipelineMySQLNested - Read from MySQL database, created nested repeating tables and write to BigQuery using JDBCIO. 


### How to compile
```bash
mvn clean package
```