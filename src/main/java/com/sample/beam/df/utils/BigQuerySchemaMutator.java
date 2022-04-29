package com.sample.beam.df.utils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.BigQuery;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Map;
import javax.annotation.Nullable;

import com.sample.beam.df.process.PrintFailedInsertProcess;
import com.sample.beam.df.process.PrintProcess;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BigQuerySchemaMutator} class is a {@link PTransform} which given a PCollection of
 * TableRows, will compare the TableRows to an existing table schema in order to dynamically perform
 * schema updates on the output table.
 */
@AutoValue
public abstract class BigQuerySchemaMutator extends PTransform<PCollection<BigQueryInsertError>,
        PCollection<TableRowWithSchema>> {

    @Nullable
    abstract BigQuery getBigQueryService();

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySchemaMutator.class);
//    abstract PCollectionView<Map<BigInteger, TableRowWithSchema>> getIncomingRecordsView();
    private static String gcsLocation;
    abstract Builder toBuilder();

    /**
     * The builder for the {@link BigQuerySchemaMutator} class.
     */
    @AutoValue.Builder
    public abstract static class Builder {
        abstract Builder setBigQueryService(BigQuery bigQuery);
        abstract BigQuerySchemaMutator build();
    }

    public static BigQuerySchemaMutator mutateWithSchema(String gcsLoc) {
        gcsLocation = gcsLoc;
        return new com.sample.beam.df.utils.AutoValue_BigQuerySchemaMutator.Builder()
//                .setIncomingRecordsView(incomingRecordsView)
                .build();
    }

    /**
     * @param bigQuery
     * @return
     */
    public BigQuerySchemaMutator withBigQueryService(BigQuery bigQuery) {
        return toBuilder().setBigQueryService(bigQuery).build();
    }

    @Override
    public PCollection<TableRowWithSchema> expand(PCollection<BigQueryInsertError> input) {

//        PCollection<TableRowWithSchema> mutatedRecords
        PCollectionTuple eventPayloads = input.apply(
                "FailedInsertToTableRowWithSchema",
                ParDo.of(new FailedInsertToTableRowWithSchema())
                        .withOutputTags(FailedInsertToTableRowWithSchema.failedSchemaTupleTag,
                                TupleTagList.of(FailedInsertToTableRowWithSchema.otherFailedTupleTag)));

        PCollection<TableRowWithSchema> schemaModRecords = eventPayloads.get(FailedInsertToTableRowWithSchema.failedSchemaTupleTag)
                .apply("GroupRecords", GroupByKey.create())
                .apply("MutateSchema", ParDo.of(new TableRowSchemaMutator(getBigQueryService())));

        eventPayloads.get(FailedInsertToTableRowWithSchema.otherFailedTupleTag)
                .apply(Values.create())
                .apply("Parse and to KV", ParDo.of(new DoFn<TableRowWithSchema, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        TableRowWithSchema ele = c.element();
                        c.output(ele.toString());
                    }
                }))
				.apply("Write to GCS", TextIO.write().to(gcsLocation)
				.withWindowedWrites()
				.withNumShards(1)
				.withCompression(Compression.GZIP));


//                .apply("Parse and to KV", ParDo.of(new DoFn<TableRowWithSchema, KV<String, TableRowWithSchema>>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c) {
//                        TableRowWithSchema ele = c.element();
//                        String hashId = String.valueOf(ele.getTableRow().hashCode()); // this is the session
//                        c.output(KV.of(hashId, ele));
//                    }
//                }))
//                .apply("Timer", ParDo.of(new DoFn<KV<String, TableRowWithSchema>, TableRowWithSchema>() {
//                            private final Duration BUFFER_TIME = Duration.standardMinutes(4);
//
//                            @TimerId("timer")
//                            private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);
//
//                            @StateId("buffer")
//                            private final StateSpec<BagState<TableRowWithSchema>> bufferedEvents = StateSpecs.bag();
//
//                            @ProcessElement
//                            public void processElement(ProcessContext c,
//                                                       @TimerId("timer") Timer timer,
//                                                       @StateId("buffer") BagState<TableRowWithSchema> buffer) {
//
//                                timer.offset(BUFFER_TIME).setRelative();
//                                buffer.add(c.element().getValue()); // add to buffer the unique id
//                                LOG.info("TIMER: buffer at " + Instant.now().toString()+" "+c.element().getValue());
//                            }
//
//                            // This method is call when timers expire
//                            @OnTimer("timer")
//                            public void onTimer(OnTimerContext c,
//                                                @StateId("buffer") BagState<TableRowWithSchema> buffer)  {
//
//                                LOG.info("TIMER Expired:"+buffer.isEmpty().read());
//                                for (TableRowWithSchema id : buffer.read()) {
//                                    LOG.info("TIMER: Releasing " + id.toString() + " from buffer at " + Instant.now().toString());
//                                    c.output(id);
//                                }
//                                LOG.info("Added from BAG");
//                                buffer.clear(); // clearing buffer
//                            }
//                        })
//                );
        return schemaModRecords;
    }
}
