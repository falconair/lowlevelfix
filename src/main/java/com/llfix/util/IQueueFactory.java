package com.llfix.util;

import java.util.Queue;

public interface IQueueFactory<T> {

	public Queue<T> getQueue(String name);
	public void returnQueue(String name);
}
