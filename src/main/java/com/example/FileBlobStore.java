package com.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/*
 * For example,
 * http://doc.s3.amazonaws.com/2006-03-01/AmazonS3.wsdl, 
 * "doc" is the bucket 
 * "2006-03-01/AmazonS3.wsdl" is the key
*/

public class FileBlobStore implements BlobStore {
	
	private static final String LOCK = ".lock";
	
	private String path;
	
	public FileBlobStore(String path) {
		this.path = path;
		
		File file = new File(this.path);
		file.mkdirs();
	}
	
	public void createBucket(String bucket) {
		File file = new File(this.path + File.separator + bucket);
		file.mkdirs();
		
		System.out.println("createBucket: " + file.getAbsolutePath());
	}
	
	@Override
	public void put(String bucket, String key, File data) {
		
		File lock = getLockFile(bucket);
		
		Path to = Paths.get(path + File.separator + bucket + File.separator + key);
		
		if(Files.notExists(to)) {
			try {
				Files.createDirectories(to.getParent());
				Files.createFile(to);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		
		lock(lock);
		try (InputStream inputStream = new FileInputStream(data);
			OutputStream outputStream = new FileOutputStream(to.toFile())) {
			
			byte[] buffer = new byte[4 * 1024];
            int bytesRead;
            while( (bytesRead = inputStream.read(buffer)) > 0 ) {
            	outputStream.write(buffer, 0, bytesRead);
            }
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			unlock(lock);
		}
	}

	@Override
	public File get(String bucket, String key) {
		File lock = getLockFile(bucket);
		lock(lock);
		try {
			return getKeyFile(bucket, key);
		} finally {
			unlock(lock);
		}
	}

	private void lock(File lock) {
		while (!lock.mkdir()) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}

	private void unlock(File lock) {
		lock.delete();
	}
	
	private File getKeyFile(String bucketName, String key) {
		return new File(path + File.separator + bucketName + File.separator + key);
	}
	
	private File getLockFile(String bucketName) {
		return new File(path + File.separator + bucketName + File.separator + LOCK);
	}
}