package com.llfix.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class DiskQueueFactory implements IQueueFactory<String> {

	private Map<String,DiskQueue> lookup = new HashMap<String,DiskQueue>();
	private final String directory;
	
	public DiskQueueFactory(){
		this(null);
	}
	
	public DiskQueueFactory(String directory){
		this.directory = directory;
	}
	
	@Override
	public ISimpleQueue<String> getQueue(String name) throws Exception {
		final DiskQueue dq =  DiskQueue.getInstance(directory , name);
		lookup.put(name, dq);
		return dq;
	}

	@Override
	public void returnQueue(String name)  {
		DiskQueue dq = lookup.get(name);
		if(dq!=null)
			try {
				dq.close();
			} catch (IOException e) {}//TODO logit
		
	}

}
