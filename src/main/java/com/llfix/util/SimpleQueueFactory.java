package com.llfix.util;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SimpleQueueFactory<T> implements IQueueFactory<T> {

	@Override
	public Queue<T> getQueue(String name) {
		return new ConcurrentLinkedQueue<T>();
	}

	@Override
	public void returnQueue(String name) {
		
	}

}
