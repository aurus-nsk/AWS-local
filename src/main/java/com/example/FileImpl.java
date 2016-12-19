package com.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.TimerTask;
import java.util.UUID;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.base.Strings;

public class FileImpl implements QueueServiceAbstract {
	
	private static final String LOCK = ".lock";
	private static final String MESSAGES = "messages";
	private static final String COPY_MESSAGES = "copy_messages";
	private static final String INVISIBLE_MESSAGES = "invisible_messages";
	private static final String COPY_INVISIBLE_MESSAGES = "copy_invisible_messages";
	
	private VisibilityCollaborator visibilityCollaborator = new VisibilityCollaborator();
	private long visibilityTimeoutMillis;
	private String path;

	public FileImpl(long visibilityTimeoutMillis, String path) {
		if ((visibilityTimeoutMillis <= 0L) || Strings.isNullOrEmpty(path)) {
			throw new IllegalArgumentException();
		}
		
		this.visibilityTimeoutMillis = visibilityTimeoutMillis;
		this.path = path;
		
		File file = new File(path);
		file.mkdir();
	}
	
	public void createQueue(String queue) throws IOException {
		if (Strings.isNullOrEmpty(queue)) 
			throw new IllegalArgumentException();

		//create directory
		File file = new File(path + File.separator + queue);
		if (file.mkdir()) {

			//create queues(files)
			File messages = getMessagesFile(queue);
			File invisibleMessages = getInvisibleMessagesFile(queue);

			messages.createNewFile();
			invisibleMessages.createNewFile();
		}
	}
	
	@Override
	public void push(String queue, String messageBody) {
		if (Strings.isNullOrEmpty(queue) || Strings.isNullOrEmpty(messageBody))
			throw new IllegalArgumentException();
		
		File messages = getMessagesFile(queue);
		if ( !messages.exists())
		    throw new IllegalArgumentException("Queue '" + queue + "' doesn't exist.");
		
		File lock = getLockFile(queue);
		lock(lock);
		try (PrintWriter pw = new PrintWriter(new FileWriter(messages, true))){
			pw.println(new Record(System.currentTimeMillis()," ",messageBody));
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			unlock(lock);
		}
	}
	
	@Override
	public Message pull(String queue) {
		if (Strings.isNullOrEmpty(queue)) 
			throw new IllegalArgumentException();
		
		File messages = getMessagesFile(queue);
		if ( !messages.exists()) 
		    throw new IllegalArgumentException("Queue '" + queue + "' doesn't exist.");
		
		File copyMessages = getCopyMessagesFile(queue);
		File invisibleMessages = getInvisibleMessagesFile(queue);
		File lock = getLockFile(queue);
		
		lock(lock);
		Record record = null;
		String recordStr = null;
		try (BufferedReader reader = new BufferedReader(new FileReader(messages));
				PrintWriter copyWriter = new PrintWriter(new FileWriter(copyMessages, false));
				PrintWriter invisibleWriter = new PrintWriter(new FileWriter(invisibleMessages, true))) {

			recordStr = reader.readLine();
			if (recordStr == null) { //empty queue
				copyMessages.delete();
				return null; 
			}
			
			//read first record and save it
			record = Record.createRecord(recordStr);
			//copy the rest records into a new copy of the file 'copyMessages'
			String line = reader.readLine();
			while (line != null) {
				copyWriter.println(line);
				line = reader.readLine();
			}
			//rename a new copy of the file back to 'messages'
			reader.close();
			copyWriter.close();
			messages.delete();
			copyMessages.renameTo(messages);

			//save this message into an invisible queue 'invisibleMessages'
			String receiptHandle = UUID.randomUUID().toString();
			record.setReceiptHandle(receiptHandle);
			record.setVisibileFrom(System.currentTimeMillis() + this.visibilityTimeoutMillis);
			invisibleWriter.println(record);


		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			unlock(lock);
		}
		
		Message message = new Message();
		message.setReceiptHandle(record.getReceiptHandle());
		message.setBody(record.getMessageBody());
		
		visibilityCollaborator.makeMessageVisibleAsynchronously(new VisibilityTask(queue, message), visibilityTimeoutMillis);
		
		return message;
	}

	@Override
	public void delete(String queue, String receiptHandle) {
		if (Strings.isNullOrEmpty(queue) || Strings.isNullOrEmpty(receiptHandle))
			throw new IllegalArgumentException();	
		
		File invisibleMessages = getInvisibleMessagesFile(queue);
		if ( !invisibleMessages.exists())
		    return;
		
		File lock = getLockFile(queue);
		File copyInvisibleMessages = getCopyInvisibleMessagesFile(queue);
		
		lock(lock);
		try (BufferedReader invisibleMessagesReader = new BufferedReader(new FileReader(invisibleMessages));
			 PrintWriter copyInvisibleMessagesWriter = new PrintWriter(new FileWriter(copyInvisibleMessages, false));) {

			String line = invisibleMessagesReader.readLine(); 
	        while (line != null) {
	        	if ( !Record.createRecord(line).getReceiptHandle().equals(receiptHandle)) {
	        		copyInvisibleMessagesWriter.println(line);
	        	}
	        	line = invisibleMessagesReader.readLine(); 
	        }
	        
	        //rename a new copy of the file back to 'invisibleMessages'
	        invisibleMessagesReader.close();
			copyInvisibleMessagesWriter.close();
			invisibleMessages.delete();
			copyInvisibleMessages.renameTo(invisibleMessages);
		} catch (IOException  e) {
			throw new RuntimeException(e);
		} finally {
			unlock(lock);
		}
	}
	
	public String getPath() {
		return path;
	}

	private void lock(File lock) {
		while (!lock.mkdir()) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void unlock(File lock) {
		lock.delete();
	}
	
	private File getLockFile(String queue) {
		return new File(path + File.separator + queue + File.separator + LOCK);
	}

	private File getMessagesFile(String queue) {
		return new File(path + File.separator + queue + File.separator + MESSAGES);
	}
	
	private File getCopyMessagesFile(String queue) {
		return new File(path + File.separator +  queue + File.separator + COPY_MESSAGES);
	}
	
	private File getInvisibleMessagesFile(String queue) {
		return new File(path + File.separator + queue + File.separator + INVISIBLE_MESSAGES);
	}
	
	private File getCopyInvisibleMessagesFile(String queue) {
		return new File(path + File.separator + queue + File.separator + COPY_INVISIBLE_MESSAGES);
	}
	
	class VisibilityTask extends TimerTask {
		private Message message;
		private String queue;

		public VisibilityTask(String queue, Message message) {
			this.message = message;
			this.queue = queue;
		}

		@Override
		public void run() {
			File lock = getLockFile(queue);
			File invisibleMessages = getInvisibleMessagesFile(queue);
			File copyInvisibleMessages = getCopyInvisibleMessagesFile(queue);
			File messages = getMessagesFile(queue);
			File copyMessages = getCopyMessagesFile(queue);
			
			lock(lock);
			try (BufferedReader invisibleMessagesReader = new BufferedReader(new FileReader(invisibleMessages));
				 BufferedReader messagesReader = new BufferedReader(new FileReader(messages));
				 PrintWriter copyInvisibleMessagesWriter = new PrintWriter(new FileWriter(copyInvisibleMessages, false));
				 PrintWriter copyMessagesWriter = new PrintWriter(new FileWriter(copyMessages, false))) {

				//read all invisible messages from 'invisibleMessages'
				//find there messages with an expired visibilityTimeout and save them into the List
				LinkedList<Record> visibleRecords = new LinkedList<Record>();
				String invisibleMessagesline = invisibleMessagesReader.readLine();
				Record record = null;
				while (invisibleMessagesline != null) {
					record = Record.createRecord(invisibleMessagesline);
					
					if (record.getReceiptHandle().equals(this.message.getReceiptHandle()) ){
						visibleRecords.addLast(record);
					} else {
						copyInvisibleMessagesWriter.println(invisibleMessagesline);
					}
					invisibleMessagesline = invisibleMessagesReader.readLine();
				}

				//rename a new copy of the file back to 'invisibleMessages'
				invisibleMessagesReader.close();
				copyInvisibleMessagesWriter.close();
				invisibleMessages.delete();
				copyInvisibleMessages.renameTo(invisibleMessages);

				//write visible messages from the List and from the 'messages' into the new copy of file 'copyMessages'
				for (Record visibleRecord : visibleRecords) {
					copyMessagesWriter.println(visibleRecord.toString());
				}
				String messagesLine = messagesReader.readLine();
				while (messagesLine != null) {
					copyMessagesWriter.println(messagesLine);
					messagesLine = messagesReader.readLine();
				}

				//rename a new copy of the file back to 'messages'
				messagesReader.close();
				copyMessagesWriter.close();
				messages.delete();
				copyMessages.renameTo(messages);
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				unlock(lock);
			}
		}
	}
	
	public VisibilityCollaborator getVisibilityCollaborator() {
		return visibilityCollaborator;
	}

	public void setVisibilityCollaborator(VisibilityCollaborator visibilityCollaborator) {
		this.visibilityCollaborator = visibilityCollaborator;
	}
}