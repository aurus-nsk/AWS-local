package com.example;

import java.util.Timer;
import java.util.TimerTask;

public class VisibilityCollaborator {
	private Timer timer = new Timer();
	
	public void makeMessageVisibleAsynchronously(TimerTask task, long visibilityTimeout) {
		timer.schedule(task, visibilityTimeout);
	}
}