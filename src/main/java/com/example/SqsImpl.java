package com.example;

import java.util.List;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SqsImpl implements QueueServiceAbstract {
	private AmazonSQSClient sqsClient;
	
	public SqsImpl(AmazonSQSClient sqsClient) {
		this.sqsClient = sqsClient;
	}
	
	public String createQueue(String queueName) {
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
		return sqsClient.createQueue(createQueueRequest).getQueueUrl();
	}

	@Override
	public void push(String queueUrl, String messageBody) {
		sqsClient.sendMessage(new SendMessageRequest(queueUrl, messageBody));
	}

	@Override
	public Message pull(String queueUrl) {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
		List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
		return (messages.isEmpty() ? null : messages.get(0));
	}

	@Override
	public void delete(String queueUrl, String messageReceiptHandle) {
		sqsClient.deleteMessage(new DeleteMessageRequest(queueUrl, messageReceiptHandle));
	}
}