Beam Data Samples 
==================

### Samples for Apache Beam/Dataflow 

- [PipelineCsvAvroProtobuf](src/main/java/com/sample/beam/df/PipelineCsvAvroProtobuf.java) - Read from CSV file present in GCS, convert it to Protobuf and write back to GCS. Uses AvroIO to write the file.


- [PipelineAvroProtobufParquet](src/main/java/com/sample/beam/df/PipelineAvroProtobufParquet.java) - Read from Protobuf file present in GCS, convert it to parquet and write back to GCS. PipelineCsvAvroProtobuf was used to generate the file.


- [PipelineProtobufParquet](src/main/java/com/sample/beam/df/PipelineProtobufParquet.java) - Read from Protobuf file present in GCS, convert it to parquet and write back to GCS. The Protobuf file was NOT written using Avro.


- [PipelineCsvParquet](src/main/java/com/sample/beam/df/PipelineCsvParquet.java) - Read from CSV file present in GCS, convert it to Parquet and write back to GCS.


- [PipelinePubSubBtBq](src/main/java/com/sample/beam/df/PipelinePubSubBtBq.java) - Read from Pub/Sub the device telemetry data and write it to BigQuery and Bigtable. Avro is used to define the data schema.


- [PipelineDbBq](src/main/java/com/sample/beam/df/PipelineDbBq.java) - Read from MySQL database using JDBCIO and write to BigQuery using BigQueryIO. Uses [Employee database](https://relational.fit.cvut.cz/dataset/Employee) employees table.


- [PipelineDbNestedBQ](src/main/java/com/sample/beam/df/PipelineDbNestedBQ.java) - Read from MySQL database using JDBCIO, create nested repeating tables and write to BigQuery using BigQueryIO. Uses [Employee database](https://relational.fit.cvut.cz/dataset/Employee) employees table. 


- [PipelineCsvAvroBq](src/main/java/com/sample/beam/df/PipelineCsvAvroBq.java) - Read from CSV file present in GCS, use OpenCSV to parse the line and write to BigQuery using BigQueryIO. Uses [Employee database](https://relational.fit.cvut.cz/dataset/Employee) employees table as CSV data. Avro is used to define the data schema.


- [PipelineDbInterleaveSpanner](src/main/java/com/sample/beam/df/PipelineDbInterleaveSpanner.java) - Read from MySQL database using JDBCIO, write to Spanner using SpannerIO. Uses [Employee database](https://relational.fit.cvut.cz/dataset/Employee) employees table. Two interleaved tables are to be pre-created in Spanner. Avro is used to define the data schema.

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
