package com.example;

import com.amazonaws.services.sqs.model.Message;

public interface QueueServiceAbstract {
	/**
	 * pushes a message onto a queue.
	 */
	void push(String queue, String messageBody);
	
	/**
	 * retrieves a single message from a queue.
	 */
	Message pull(String queue);
	
	/**
	 * deletes a message from the queue that was received by pull().
	 */
	void delete(String queue, String receiptHandle);
}
