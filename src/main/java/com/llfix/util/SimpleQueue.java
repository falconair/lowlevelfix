package com.llfix.util;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class SimpleQueue<E> implements ISimpleQueue<E> {

	private final Queue<E> q = new ConcurrentLinkedQueue<E>();
	
	@Override
	public boolean offer(E e) {
		return q.offer(e);
	}

	@Override
	public Iterator<E> iterator() {
		return q.iterator();
	}

}