package com.sample.beam.df.utils;

import com.google.api.services.bigquery.model.TableSchema;
import com.sample.beam.df.shared.CustomDestination;
import com.sample.beam.df.shared.TableMetaData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CustomDestinationCoder extends CustomCoder<CustomDestination> {

    // Coders for the member types
    private static final AvroCoder<TableMetaData> TABLE_METADATA_CODER = AvroCoder.of(TableMetaData.class);
    private static final GenericJsonCoder<TableSchema> TABLE_SCHEMA_CODER =
            GenericJsonCoder.of(TableSchema.class);

    // Singleton instances
    private static final CustomDestinationCoder INSTANCE = new CustomDestinationCoder();
    private static final TypeDescriptor<CustomDestination> TYPE_DESCRIPTOR =
            new TypeDescriptor<com.sample.beam.df.shared.CustomDestination>() {
            };

    public static CustomDestinationCoder of() {
        return INSTANCE;
    }

    public static void registerCoder(Pipeline pipeline) {
        CustomDestinationCoder coder = CustomDestinationCoder.of();

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);
    }

    @Override
    public void encode(CustomDestination value, OutputStream outStream) throws CoderException, IOException {
        if (value == null) {
            throw new CoderException("The CustomDestinationCoder cannot encode a null object!");
        }

        TABLE_METADATA_CODER.encode(value.getTableMetaData(), outStream);
        TABLE_SCHEMA_CODER.encode(value.getTableSchema(), outStream);

    }

    @Override
    public CustomDestination decode(InputStream inStream) throws CoderException, IOException {
        TableMetaData tableMetaData = TABLE_METADATA_CODER.decode(inStream);
        TableSchema tableSchema = TABLE_SCHEMA_CODER.decode(inStream);
        CustomDestination customDestination = new CustomDestination();
        customDestination.setTableSchema(tableSchema);
        customDestination.setTableMetaData(tableMetaData);
        return customDestination;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        return;
    }

    @Override
    public TypeDescriptor<CustomDestination> getEncodedTypeDescriptor() {
        return TYPE_DESCRIPTOR;
    }
}
