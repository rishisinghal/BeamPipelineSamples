Beam Data Samples 
==================

### Samples for Apache Beam/Dataflow 

- StarterPipelinePubSub - Read from Pub/Sub the device telemetry data and write it to BigQuery and Bigtable. Avro is used to define the data schema.


- StarterPipelineDb - Read from MySQL database using JDBCIO and write to BigQuery using BigQueryIO. Uses [Employee database](https://relational.fit.cvut.cz/dataset/Employee) employees table.


- StarterPipelineDbNestedBQ - Read from MySQL database using JDBCIO, create nested repeating tables and write to BigQuery using BigQueryIO. Uses [Employee database](https://relational.fit.cvut.cz/dataset/Employee) employees table. 


- StarterPipelineCsvAvro - Read from CSV file present in GCS, use OpenCSV to parse the line and write to BigQuery using BigQueryIO. Uses [Employee database](https://relational.fit.cvut.cz/dataset/Employee) employees table as CSV data. Avro is used to define the data schema.


- StarterPipelineDbInterleaveSpanner - Read from MySQL database using JDBCIO, write to Spanner using SpannerIO. Uses [Employee database](https://relational.fit.cvut.cz/dataset/Employee) employees table. Two interleaved tables are to be pre-created in Spanner. Avro is used to define the data schema.

	    - emp Table with schema:
	       *emp_id: INT64 NOT NULL
	        birth_date: STRING 
	        first_name: STRING 
	        
	    - dept Table with schema:
	        emp_id: INT64 NOT NULL
	       *dept: INT64 
	        join_date: STRING 

### How to compile
```bash
mvn clean package
```

### Deploy in Cloud Composer
Check the [deployDF.py](src/main/python/workflow/deployDF.py) file
