package com.example;

import java.io.Serializable;
import java.util.regex.Pattern;

class Record implements Serializable {

	private static final String SEPARATOR = "|";
	private long visibileFrom;
	private String receiptHandle;
	private String messageBody;
	
	public Record(long visibileFrom , String receiptHandle, String messageBody) {
		this.visibileFrom  = visibileFrom ;
		this.receiptHandle = receiptHandle;
		this.messageBody = messageBody;
	}
	public Record() {
	}
	
	public long getVisibileFrom () {
		return visibileFrom ;
	}
	public void setVisibileFrom (long visibileFrom ) {
		this.visibileFrom  = visibileFrom ;
	}
	public String getReceiptHandle() {
		return receiptHandle;
	}
	public void setReceiptHandle(String receiptHandle) {
		this.receiptHandle = receiptHandle;
	}
	public String getMessageBody() {
		return messageBody;
	}
	public void setMessageBody(String messageBody) {
		this.messageBody = messageBody;
	}
	
	@Override
	public String toString() {
		return this.visibileFrom + SEPARATOR + this.receiptHandle + SEPARATOR + this.messageBody;  
	}
	
	/**
	 * @param <code>line</code> - it is the return value of <code>this.toString()</code>
	 * @return Record
	 */
	public static Record createRecord(String line) {
		String[] strings = line.split(Pattern.quote(SEPARATOR));
		return new Record(Long.parseLong(strings[0]), strings[1], strings[2]);
	}
}
