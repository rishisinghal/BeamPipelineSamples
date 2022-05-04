package com.sample.beam.df;

import com.google.firestore.v1.DatabaseRootName;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import com.sample.beam.df.utils.FirestoreTypesPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FirestoreMappingPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(FirestoreMappingPipeline.class);


    public static void main(String [] args) throws Exception {
        FirestoreTypesPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FirestoreTypesPipelineOptions.class);
        run(options);
    }

    public static PipelineResult run(FirestoreTypesPipelineOptions options) throws Exception {

        String collectionGroupId = "tokens";
        Pipeline p =Pipeline.create(options);
        String srcFirestoreCollection = String.format("projects/%s/databases/(default)/documents",
                options.as(DataflowPipelineOptions.class).getProject());


//        PCollection<String> allDocs =  p.apply(Create.of(collectionGroupId))
//                .apply(new CreatePartitionQueryRequest(2))
//                .apply(FirestoreIO.v1().read().partitionQuery().withNameOnlyQuery().build())
//                .apply(FirestoreIO.v1().read().runQuery().build())
//                .apply(MapElements.into(TypeDescriptors.strings()).via(
//                        (runQueryResponse) -> runQueryResponse.getDocument().getName()));


        PCollection<String> allDocs =  p.apply(Create.of(collectionGroupId))
                .apply(new CreateRunQueryRequest(srcFirestoreCollection))
                .apply(FirestoreIO.v1().read().runQuery().build())
                .apply(MapElements.into(TypeDescriptors.strings()).via(
                        (runQueryResponse) -> runQueryResponse.getDocument().toString()));


        allDocs.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String word = c.element();
                LOG.info("Dats is:"+word);
            }
            }));

        return p.run();
    }
}

final class CreateRunQueryRequest extends PTransform<PCollection<String>, PCollection<RunQueryRequest>> {

    private static final Logger LOG = LoggerFactory.getLogger(CreateRunQueryRequest.class);
    private final String srcFirestoreCollection;

    public CreateRunQueryRequest(String srcFirestoreCollection) {
        this.srcFirestoreCollection = srcFirestoreCollection;
    }

    @Override
    public PCollection<RunQueryRequest> expand(PCollection<String> input) {
        return input.apply("create queries", ParDo.of(new DoFn<String, RunQueryRequest>() {

            @ProcessElement
            public void processElement(ProcessContext ctx) {
                String collectionId = ctx.element();
                String project = ctx.getPipelineOptions().as(GcpOptions.class).getProject();
                DatabaseRootName db = DatabaseRootName.of(project, "(default)");
                LOG.info("DB is:"+db.toString());

                StructuredQuery.Builder query = StructuredQuery.newBuilder()
                        .addFrom(StructuredQuery.CollectionSelector.newBuilder()
                                .setCollectionId(collectionId)
                                .setAllDescendants(true)
                        );

                LOG.info("Query is:"+query.toString());

                RunQueryRequest request = RunQueryRequest.newBuilder()
                        .setStructuredQuery(query)
                        .setParent(srcFirestoreCollection)
                        .build();

                LOG.info("Request is:"+request.toString());

                ctx.output(request);
            }
        }));
    }

//final class CreatePartitionQueryRequest extends PTransform<PCollection<String>, PCollection<PartitionQueryRequest>> {
//
//    private final long partitionCount;
//    private static final Logger LOG = LoggerFactory.getLogger(CreatePartitionQueryRequest.class);
//
//
//    public CreatePartitionQueryRequest(long partitionCount) {
//        this.partitionCount = partitionCount;
//    }
//
//    @Override
//    public PCollection<PartitionQueryRequest> expand(PCollection<String> input) {
//        return input.apply("create queries", ParDo.of(new DoFn<String, PartitionQueryRequest>() {
//
//            @ProcessElement
//            public void processElement(ProcessContext ctx) {
//                String collectionId = ctx.element();
//                String project = ctx.getPipelineOptions().as(GcpOptions.class).getProject();
//                DatabaseRootName db = DatabaseRootName.of(project, "(default)");
//                LOG.info("DB is:"+db.toString());
//
//                StructuredQuery.Builder query = StructuredQuery.newBuilder()
//                        .addFrom(StructuredQuery.CollectionSelector.newBuilder()
//                                        .setCollectionId(collectionId)
//                                        .setAllDescendants(true)
//                        )
//                        .addOrderBy(StructuredQuery.Order.newBuilder()
//                                        .setField(StructuredQuery.FieldReference.newBuilder().setFieldPath("__name__").build())
//                                        .setDirection(StructuredQuery.Direction.ASCENDING)
//                                        .build());
//
//                LOG.info("Query is:"+query.toString());
//
//                PartitionQueryRequest request = PartitionQueryRequest.newBuilder()
//                        .setParent(db.toString() + "/documents")
//                        .setPartitionCount(partitionCount - 1)
//                        .setStructuredQuery(query)
//                        .build();
//
//                LOG.info("Request is:"+request.toString());
//
//                ctx.output(request);
//            }
//        }));
//    }
//}

}

