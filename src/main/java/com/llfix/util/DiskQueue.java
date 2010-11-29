package com.llfix.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DiskQueue<E> implements ISimpleQueue<E> {

	private final Queue<E> q;
	private final BufferedWriter writer;
	
	public DiskQueue(String name) throws IOException{
		q = new ConcurrentLinkedQueue<E>();
		final File file = new File(name);
		if(file.exists()){
			//TODO: load file into q
			//then close file (to be opened below again)
		}
		writer = new BufferedWriter(new FileWriter(file));
	}
	
	@Override
	public Iterator<E> iterator() {
		return q.iterator();
	}

	@Override
	public boolean offer(E e) {
		try {
			writer.write(e.toString());
		} catch (IOException e1) {
			return false;
			//TODO: Record this exception
		}
		return q.offer(e);
	}

}
