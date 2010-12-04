package com.llfix.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.llfix.handlers.FIXSessionProcessor;

public class DiskQueue implements ISimpleQueue<String> {
	
	final static Logger logger = LoggerFactory.getLogger(DiskQueue.class);

	private final BufferedWriter writer;
	private final List<String> contents = new ArrayList<String>();
	private static final Set<String> openFiles = new HashSet<String>();
	private final String name;
	
	public static synchronized DiskQueue getInstance(String name) throws Exception{
		return getInstance("",name);
	}
	
	public static synchronized DiskQueue getInstance(String directory, String name) throws Exception{
		if(openFiles.contains(name)) throw new Exception("File "+name+" already open");
		openFiles.add(name);
		return new DiskQueue(directory, name);
	}
	
	private DiskQueue(String directory, String name) throws Exception{
		
		this.name = name;
		logger.info("Creating file "+name);
		
		final File file = new File(directory, name);
		if(file.exists()){
			final BufferedReader in = new BufferedReader(new FileReader(file));
			String line;
			while((line = in.readLine()) != null){
				contents.add(line);
			}
			in.close();
		}
		writer = new BufferedWriter(new FileWriter(file,true));
	}
	
	@Override
	public Iterator<String> iterator() {
		return contents.iterator();
	}

	@Override
	public void offer(String e) throws Exception {
		contents.add(e);
		writer.append(e+"\n");//TODO find a better way to do platform independent newline
		writer.flush();
	}
	
	public void close() throws IOException{
		writer.close();
		openFiles.remove(name);
	}
	
	@Override
	protected void finalize() throws Throwable {
		try{
			writer.close();
			openFiles.remove(name);
		}
		catch(Exception e){
			//TODO: ignore?
		}
		super.finalize();
	}

}
