package com.example;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3BlobStore implements BlobStore {

	private AmazonS3 s3Client;

	public S3BlobStore() {
		s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
	}
	
	public void createBucket(String bucketName) {
		if(!s3Client.doesBucketExist(bucketName)) {
			s3Client.createBucket(new CreateBucketRequest(bucketName));	
        }
	}

	@Override
	public void put(String bucket, String key, File data) {
		s3Client.putObject(new PutObjectRequest(bucket, key, data));	
	}

	//try to copy image into disk
	@Override
	public File get(String bucket, String key) {
		S3Object s3object = s3Client.getObject(new GetObjectRequest(bucket, key));
		File file = new File("/image.png");
		
		try (InputStream inputStream = s3object.getObjectContent();
			OutputStream outputStream = new FileOutputStream(file)){
			
			byte[] buffer = new byte[4 * 1024];
            int bytesRead;
            while( (bytesRead = inputStream.read(buffer)) > 0 ){
            	outputStream.write(buffer, 0, bytesRead);
            }
            
		} catch (IOException e) {
			throw new RuntimeException("get(String bucket, String key): " + e.getMessage());
		}
		
		return file;
	}
}