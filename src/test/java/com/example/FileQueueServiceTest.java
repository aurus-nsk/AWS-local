package com.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Queue;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.annotations.VisibleForTesting;

public class FileQueueServiceTest {

	private FileImpl service;

	@Mock
	VisibilityCollaborator mockVisibilityCollaborator;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		String path = "queues";
		
		service = new FileImpl(30000L, path);
		service.setVisibilityCollaborator(mockVisibilityCollaborator);

		// Let's do a synchronous answer
		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				return null;
			}
		}).when(mockVisibilityCollaborator)
		.makeMessageVisibleAsynchronously(any(TimerTask.class), anyLong());
	}

	@Test
	public void visibilityTimeoutTest() throws IOException {
		String queue = "visibilityTimeoutTest";
		deleteQueueIfExists(service.getPath(), queue);
		try{
			// Let's call the method under test
			service.createQueue(queue);
			service.push(queue, "Timeout visibility test");
			Message msg1 = service.pull(queue);

			// Verify state and interaction
			verify(mockVisibilityCollaborator, times(1)).makeMessageVisibleAsynchronously(any(TimerTask.class), anyLong());

			// Check that the message is available again
			service.new VisibilityTask(queue, msg1).run();
			Message msg2 = service.pull(queue);

			assertEquals(msg1.getBody(),msg2.getBody());
			assertNotEquals(msg1.getReceiptHandle(),msg2.getReceiptHandle());
		} finally {
			deleteQueueIfExists(service.getPath(), queue);
		}
	}

	@Test
	public void pullFromEmptyQueue() throws IOException {
		String queue = "pullFromEmptyQueue";
		deleteQueueIfExists(service.getPath(), queue);
		try{
			service.createQueue(queue);
			Message msg = service.pull(queue);
			assertEquals(msg, null);
		} finally {
			deleteQueueIfExists(service.getPath(), queue);
		}
	}

	@Test
	public void delete() throws IOException {
		String queue = "delete";
		deleteQueueIfExists(service.getPath(), queue);
		try{
			// Let's call the method under test
			service.createQueue(queue);
			service.push(queue, "one");
			service.push(queue, "two");
			Message msg1 = service.pull(queue);
			service.delete(queue, msg1.getReceiptHandle());

			// Verify state and interaction
			verify(mockVisibilityCollaborator, times(1)).makeMessageVisibleAsynchronously(any(TimerTask.class), anyLong());

			//run synchronously the async method
			service.new VisibilityTask(queue, msg1).run();

			// Check that the message isn't available again
			Message msg2 = service.pull(queue);
			assertNotEquals(msg1.getBody(),msg2.getBody());
		} finally {
			deleteQueueIfExists(service.getPath(), queue);
		}
	}

	@Test
	public void deleteFromEmptyQueue() throws IOException {
		String queue = "deleteFromEmptyQueue";
		deleteQueueIfExists(service.getPath(), queue);
		try{
			service.createQueue(queue);
			service.delete(queue, "wrongReceiptHandle");
		} finally {
			deleteQueueIfExists(service.getPath(), queue);
		}
	}
	
	@Test
	public void createPushCreate() throws IOException {
		String queue = "createPushCreate";
		deleteQueueIfExists(service.getPath(), queue);
		try {
			String one = "one";
			service.createQueue(queue);
			service.push(queue, one);
			service.createQueue(queue);
			Message msg = service.pull(queue);
		
			assertEquals(one, msg.getBody());
		} finally {
			deleteQueueIfExists(service.getPath(), queue);
		}
	}
	
	@Test 
	public void concurrentPush() throws IOException {
		String queue = "concurrentPush";
		deleteQueueIfExists(service.getPath(), queue);
		try{
			service.createQueue(queue);
			final int producers = 10;

			Thread producer1 = new Thread(new Runnable() {
				@Override
				public void run() {
					for(int i=0; i<producers; ++i){
						service.push(queue, "Producer1: test message #" + i);
					}
				}
			});
			
			Thread producer2 = new Thread(new Runnable() {
				@Override
				public void run() {
					for(int i=0; i<producers; ++i){
						service.push(queue, "Producer2: test message #" + i);
					}
				}
			});
			
			//produce
			producer1.start();  
			producer2.start();
			try {
				producer1.join();
				producer2.join();
			} catch (InterruptedException e) {
				System.out.println("concurrentPush: " + e.getMessage());
			}
			
			//consume
			int consumed = 0;
			Message msg = service.pull(queue);
			while(msg != null) {
				++consumed;
				msg = service.pull(queue);
			}
			
			assertEquals(consumed, producers*2);
		} finally {
			deleteQueueIfExists(service.getPath(), queue);
		}
	}
	
	@Test 
	public void concurrentPull() throws IOException {
		String queue = "concurrentPull";
		deleteQueueIfExists(service.getPath(), queue);
		try{
			service.createQueue(queue);
			final int consumers = 10;
			final int producers = consumers*2;
			Queue<Message> consumedMessagesList = new ConcurrentLinkedQueue<Message>();
			
			Thread consumer1 = new Thread(new Runnable() {
				@Override
				public void run() {
					for(int i=0; i<consumers; ++i){
						Message msg = service.pull(queue);
						if (msg != null) {
							consumedMessagesList.add(msg);
						}
					}
				}
			});
			
			Thread consumer2 = new Thread(new Runnable() {
				@Override
				public void run() {
					for(int i=0; i<consumers; ++i){
						Message msg = service.pull(queue);
						if (msg != null) {
							consumedMessagesList.add(msg);
						}
					}
				}
			});
			
			//produce
			for(int i=0; i<producers; ++i){
				service.push(queue, "Producer: test message #" + i);
			}
			
			//consume
			consumer1.start(); 
			consumer2.start();
			
			try {
				consumer1.join();
				consumer2.join();
			} catch (InterruptedException e) {
				System.out.println("concurrentPull: " + e.getMessage());
			}
			
			Message nullMsg = service.pull(queue);
			assertEquals(nullMsg, null);
			assertEquals(consumedMessagesList.size(), producers);
		} finally {
			deleteQueueIfExists(service.getPath(), queue);
		}
	}
	
	@Test
	public void inter() throws IOException {
		String path = service.getPath();
		
		FileImpl service1 = new FileImpl(30000L, path);
		service1.setVisibilityCollaborator(mockVisibilityCollaborator);
		
		FileImpl service2 = new FileImpl(30000L, path);
		service2.setVisibilityCollaborator(mockVisibilityCollaborator);
		
		String queue = "inter";
		deleteQueueIfExists(path, queue);
		try {
			service.createQueue(queue);
			
			service1.push(queue, "one");
			service2.push(queue, "two");
			
			Message msg2 = service2.pull(queue);
			Message msg1 = service1.pull(queue);
			
			service1.delete(queue, msg2.getReceiptHandle());
			service2.delete(queue, msg1.getReceiptHandle());
		
			assertEquals(msg2.getBody(), "one");
			assertEquals(msg1.getBody(), "two");
		} finally {
			deleteQueueIfExists(path, queue);
		}
	}
	
	@Test
	public void deleteFromInvalidQueue() {
		service.delete("invalidQueueName", "invalidReceiptHandle");
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void pushNull() throws IOException {
		String queue = "pushNull";
		deleteQueueIfExists(service.getPath(), queue);
		try {
			service.createQueue(queue);
			service.push(queue, null);
		} finally {
			deleteQueueIfExists(service.getPath(), queue);
		}
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void pushIntoInvalidQueue() {
		service.push("invalidQueueName", "message");
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void pullFromInvalidQueue() {
		service.pull("invalidQueueName");
	}
	
	/**
	 * It is not inter-process safe when used concurrently in multiple VMs!
	 * Delete the directory recursively.
	 * @return <code>true</code> if the directory(queue) is successfully deleted;
	 */
	@VisibleForTesting
	private boolean deleteQueueIfExists(String pathToQueue, String queueName) {
		boolean result = false;
		Path directory = Paths.get(pathToQueue + File.separator +  queueName);
		if ( !Files.exists(directory)) {
		    return result;
		}
		
		try {
			Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					Files.delete(dir);
					return FileVisitResult.CONTINUE;
				}

			} );
			result = true;
		} catch (IOException e) {
			System.out.println("'deleteQueueIfExists' throws an Exception: " + e.getMessage());
		}
		return result;
	}
}