package com.example;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.base.Strings;

public class InMemoryImpl implements QueueServiceAbstract {

	public final static InMemoryImpl INSTANCE = new InMemoryImpl();

	private final static long DEFAULT_VISIBILITY_TIMEOUT_MILLIS = 30000L;
	/*queueName->Deque<messageBody>*/
	private final Map<String, Deque<String>> messages = new HashMap<String, Deque<String>>();
	/*receiptHandle->Task*/
	private final Map<String, TimerTask> invisibleMessages = new HashMap<String, TimerTask>();
	/*queueName->VisibilityTimeout*/
	private final Map<String, Long> queueVisibilityTimeout = new HashMap<String, Long>();

	private VisibilityCollaborator visibilityCollaborator = new VisibilityCollaborator();

	private final ReadWriteLock readWriteLock =  new ReentrantReadWriteLock();
	private final Lock read  = readWriteLock.readLock();
	private final Lock write = readWriteLock.writeLock();

	/**
	 * @return Singleton instance of <code>InMemoryQueueService</code>
	 */
	public static QueueServiceAbstract getInMemoryQueueServiceInstance() {
		return INSTANCE;
	}

	private InMemoryImpl() {/*Exists only to defeat instantiation*/}

	public void createQueue(String queueName) {
		createQueue(queueName, DEFAULT_VISIBILITY_TIMEOUT_MILLIS);
	}

	public void createQueue(String queueName, long visibilityTimeoutMillis) {
		if (Strings.isNullOrEmpty(queueName) || visibilityTimeoutMillis <= 0) 
			throw new IllegalArgumentException();

		write.lock();
		try {
			queueVisibilityTimeout.put(queueName, new Long(visibilityTimeoutMillis));
			messages.putIfAbsent(queueName, new LinkedList<String>());
		} finally {
			write.unlock();
		}
	}

	public void push(String queueName, String msg) {
		if(Strings.isNullOrEmpty(queueName) || Strings.isNullOrEmpty(msg)) 
			throw new IllegalArgumentException();

		write.lock();
		try {
			Deque<String> deque = messages.get(queueName);
			if (deque != null) {
				deque.add(msg);
			} else {
				throw new IllegalArgumentException();
			}
		} finally {
			write.unlock();
		}	
	}

	public Message pull(String queueName) {
		if (Strings.isNullOrEmpty(queueName)) 
			throw new IllegalArgumentException();

		final String messageBody;
		read.lock();
		try{
			if (messages.get(queueName) == null)
				throw new IllegalArgumentException();

			Deque<String> deque = messages.get(queueName);
			if (deque.isEmpty()){ 
				return null;
			}
			messageBody = deque.poll();
		} finally {
			read.unlock();
		}

		//save the message into the invisible queue and make it (asynchronously) visible after timeout
		Message message = new Message();
		message.setBody(messageBody);
		message.setReceiptHandle(UUID.randomUUID().toString());

		VisibilityTask task = new VisibilityTask(queueName, message);
		invisibleMessages.put(message.getReceiptHandle(), task);

		long visibilityTimeoutMillis = queueVisibilityTimeout.get(queueName);
		visibilityCollaborator.makeMessageVisibleAsynchronously(task, visibilityTimeoutMillis);

		return message;
	}

	public void delete(String queue, String receiptHandle) {
		if (Strings.isNullOrEmpty(receiptHandle) || Strings.isNullOrEmpty(queue)) 
			throw new IllegalArgumentException();

		write.lock();
		try {
			if (invisibleMessages.get(receiptHandle) == null) 
				return;

			TimerTask task = invisibleMessages.get(receiptHandle);
			if (task != null) {
				task.cancel();
				invisibleMessages.remove(receiptHandle);
			}
		} finally {
			write.unlock();
		}
	}

	public VisibilityCollaborator getVisibilityCollaborator() {
		return visibilityCollaborator;
	}

	public void setVisibilityCollaborator(VisibilityCollaborator visibilityCollaborator) {
		this.visibilityCollaborator = visibilityCollaborator;
	}

	class VisibilityTask extends TimerTask {
		private final String queueName;
		private final Message message;

		public VisibilityTask(String queueName, Message message) {
			this.message = message;
			this.queueName = queueName;
		}

		@Override
		public void run() {
			String receiptHandle = message.getReceiptHandle();
			String messageBody = message.getBody();
			String queueName = this.queueName;

			write.lock();
			try {
				TimerTask task = invisibleMessages.get(receiptHandle);
				if (task != null && messageBody != null) {
					invisibleMessages.remove(receiptHandle);
					Deque<String> deque = messages.get(queueName);
					if (deque != null)
						deque.addFirst(messageBody);
				}
			} finally {
				write.unlock();
			}
		}
	}
}