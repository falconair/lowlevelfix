package com.llfix.util;

import com.llfix.IQueueFactory;
import com.llfix.ISimpleQueue;


public class MemoryQueueFactory<T> implements IQueueFactory<T> {

	@Override
	public ISimpleQueue<T> getQueue(String name) {
		return new MemoryQueue<T>();
	}

	@Override
	public void returnQueue(String name) {
		
	}

}
