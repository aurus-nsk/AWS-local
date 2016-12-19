package com.example;

import java.io.File;

public interface BlobStore {

	public void put(String bucket, String key, File data);
	public File get(String bucket, String key);
}