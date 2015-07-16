package org.shirdrn.solr.indexing.common;

import org.shirdrn.solr.indexing.indexer.AbstractIndexer.Status;


public abstract class AbstractArgsAssembler<T> implements ArgsAssembler<T> {

	protected String type;
	protected String name;
	protected int argCount;
	private Status status = Status.UNKNOWN;
	
	public AbstractArgsAssembler() {
		super();
	}
	
	@Override
	public String getType() {
		return type;
	}

	@Override
	public String getName() {
		return name;
	}

	public String getClassname() {
		return this.getClass().getName();
	}

	@Override
	public T assemble(String[] args) throws Exception {
		if(args.length < argCount) {
			StringBuffer usage = new StringBuffer();
			usage.append(getClassname()).append(" ")
			.append(getUsageArgList());
			System.err.println(usage.toString());
			System.exit(-1);
		}
		return null;
	}
	
	@Override
	public int getRequiredArgCount() {
		return argCount;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public Status getStatus() {
		return status;
	}
	
}
