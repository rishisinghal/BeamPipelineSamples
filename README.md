Beam Data Samples 
==================

### Samples for Apache Beam/Dataflow 

- StarterPipelinePubSub - Read from Pub/Sub the device telemetry data and write it to BigQuery and Bigtable. Avro is used to define the data schema.


- StarterPipelineDb - Read from MySQL database and write to BigQuery using JDBCIO. Uses [Employee database](https://relational.fit.cvut.cz/dataset/Employee) employees table.


- StarterPipelineDbNestedBQ - Read from MySQL database, created nested repeating tables and write to BigQuery using JDBCIO. Uses [Employee database](https://relational.fit.cvut.cz/dataset/Employee) employees table. 


- StarterPipelineCsvAvro - Read from CSV file in GCS, use OpenCSV to parse and write to BigQuery. Uses [Employee database](https://relational.fit.cvut.cz/dataset/Employee) employees table as CSV data. Avro is used to define the data schema.

### How to compile
```bash
mvn clean package
```