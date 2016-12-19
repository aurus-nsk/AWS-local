package com.example;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

public class FileBlobStoreTest {
	
	private FileBlobStore blobStore;
	String path;
	
	@Before
	public void setUp() throws Exception {
		this.path = "s3";
		blobStore = new FileBlobStore(path);
	}
	
	@Test
	public void putAndGet() {
		String bucket = "bigBucket";
		blobStore.createBucket(bucket);
		
		
		File data = new File("C://image.jpg");
		
		blobStore.put(bucket, "images/cursor.jpg", data);
		File result = blobStore.get(bucket, "images/cursor.jpg");
		
		assertEquals(data.length(), result.length());
	}	
	
	@Test
	public void test() {
		int a[] = new int[]{1,2,3,4,5,6,7,8,9};
		int b[] = Arrays.copyOfRange(a, 3,5);
		Arrays.fill(b, 777);
		//System.out.println(f);
		System.out.println(Arrays.toString(b));
	}
}