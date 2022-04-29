package com.sample.beam.df.process;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.opencsv.CSVParser;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class GCSFileProcess extends DoFn<FileIO.ReadableFile, String> {

	private static final long serialVersionUID = 1462827258689031685L;
	private static final Logger LOG = LoggerFactory.getLogger(GCSFileProcess.class);
	Storage gcsStorage;

	@Setup
	public void initGCS() {
		gcsStorage = StorageOptions.getDefaultInstance().getService();
	}

	@ProcessElement
	public void processElement(ProcessContext c) {
		FileIO.ReadableFile file = c.element();
		String removedGsStr = file.getMetadata().resourceId().toString().replace("gs://", "");
		String bucketName = removedGsStr.substring(0,removedGsStr.indexOf("/") );
		String path =  removedGsStr.substring(removedGsStr.indexOf("/")+1);
		Blob fileBlob = gcsStorage.get(bucketName, path);
		Date createTime = new Date(fileBlob.getCreateTime());
		LOG.info("File bucket is:"+file.getMetadata().toString()+" created time:"+createTime.toString());
		c.output(file.getMetadata().toString());
	}

}
