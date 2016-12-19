package com.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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

public class InMemoryQueueServiceTest {
  
	private InMemoryImpl service;

	@Mock
	VisibilityCollaborator mockVisibilityCollaborator;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		service = InMemoryImpl.INSTANCE;
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
	public void visibilityTimeoutTest() {
		String queue = "visibilityTimeoutTest";
		
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
	}
	
	@Test
	public void pullFromEmptyQueue() {
		String queue = "pullFromEmptyQueue";
		service.createQueue(queue);
		Message msg = service.pull(queue);
		assertEquals(msg, null);
	}
	
	@Test
	public void delete() {
		String queue = "delete";
		service.createQueue(queue);
		service.push(queue, "one");
		service.push(queue, "two");
		Message msg1 = service.pull(queue);
		service.delete(queue, msg1.getReceiptHandle());

		// Verify state and interaction
		verify(mockVisibilityCollaborator, times(1)).makeMessageVisibleAsynchronously(any(TimerTask.class), anyLong());

		//Run synchronously the async method 'makeMessageVisibleAsynchronously()'
		service.new VisibilityTask(queue, msg1).run();

		// Check that the message isn't available again
		Message msg2 = service.pull(queue);
		assertNotEquals(msg1.getBody(),msg2.getBody());
	}
	
	@Test
	public void deleteFromEmptyQueue() {
		String queue = "deleteFromEmptyQueue";
		service.createQueue(queue);
		service.delete(queue, "wrongReceiptHandle");
	}

	@Test
	public void createPushCreate() {
		String queue = "createPushCreate";
		String one = "one";
		service.createQueue(queue);
		service.push(queue, one);
		service.createQueue(queue);
		Message msg = service.pull(queue);

		assertEquals(one, msg.getBody());
	}
	
	@Test 
	public void concurrentPush() {
		String queue = "concurrentPush";
		service.createQueue(queue);
		final int producers = 50;
		
		Thread producer1 = new Thread(new Runnable() {
			@Override
			public void run() {
				for(int i=0; i<producers; ++i){
					service.push(queue, "Producer: test message #" + i);
				}
			}
		});

		Thread producer2 = new Thread(new Runnable() {
			@Override
			public void run() {
				for(int i=0; i<producers; ++i){
					service.push(queue, "Producer: test message #" + i);
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
		
		assertEquals(consumed, 2*producers);
	}
	
	@Test 
	public void concurrentPull() {
		String queue = "concurrentPull";
		service.createQueue(queue);
		Queue<Message> consumedMessagesList = new ConcurrentLinkedQueue<Message>();
		final int consumers = 50;
		final int producers = consumers*2;
		
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
		for(int i=0; i < producers; ++i){
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

		assertEquals(consumedMessagesList.size(), producers);
	}
	
	@Test
	public void deleteFromInvalidQueue() {
		service.delete("invalidQueueName", "invalidReceiptHandle");
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void pushNull() {
		String queue = "pushNull";
		service.createQueue(queue);
		service.push(queue, null);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void pushIntoInvalidQueue() {
		service.push("invalidQueueName", "message");
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void pullFromInvalidQueue() {
		service.pull("invalidQueueName");
	}
}