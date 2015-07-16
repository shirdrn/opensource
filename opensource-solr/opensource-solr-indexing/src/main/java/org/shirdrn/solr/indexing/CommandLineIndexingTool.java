package org.shirdrn.solr.indexing;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.solr.indexing.common.ArgsAssembler;
import org.shirdrn.solr.indexing.indexer.AbstractIndexer;
import org.shirdrn.solr.indexing.indexer.component.DBIndexer;
import org.shirdrn.solr.indexing.indexer.component.FilesIndexer;
import org.shirdrn.solr.indexing.indexer.component.IndexDeletionTool;
import org.shirdrn.solr.indexing.indexer.component.MultiThreadedDBIndexer;
import org.shirdrn.solr.indexing.indexer.component.MultiThreadedFilesIndexer;
import org.shirdrn.solr.indexing.indexer.mapred.SolrCloudIndexer;

public class CommandLineIndexingTool {

	private static final Log LOG = LogFactory.getLog(CommandLineIndexingTool.class);
	private static final Map<String, ArgsAssembler<? extends AbstractIndexer>> ASSEMBLERS = new LinkedHashMap<>();
	private static final Map<String, Object> OTHERS_ASSEMBLERS = new LinkedHashMap<>();
	// <type, name> pairs
	private static final Map<String, String> TYPE_TO_NAME_MAPPING = new LinkedHashMap<>();
	
	// load assembler classes
	static {
		SolrCloudIndexer indexer = new SolrCloudIndexer();
		register(indexer.getType(), indexer.getName(), indexer);
		register(new DBIndexer.Assembler());
		register(new MultiThreadedDBIndexer.Assembler());
		register(new FilesIndexer.Assembler());
		register(new MultiThreadedFilesIndexer.Assembler());
		
		register(new IndexDeletionTool.Assembler());
	}
	
	private static void register(ArgsAssembler<? extends AbstractIndexer> assembler) {
		register(assembler.getType(), assembler.getName(), assembler);
	}
	
	public static void register(String type, String name, 
			final ArgsAssembler<? extends AbstractIndexer> assembler) {
		putTypeNamePair(type, name);
		ASSEMBLERS.put(type, assembler);
	}

	public static <T> void register(String type, String name, 
			final T assembler) {
		putTypeNamePair(type, name);
		OTHERS_ASSEMBLERS.put(type, assembler);
	}
	
	private static void putTypeNamePair(String type, String name) {
		if(!TYPE_TO_NAME_MAPPING.containsKey(type)) {
			TYPE_TO_NAME_MAPPING.put(type, name);
		} else {
			throw new RuntimeException("Type " + type + " already existed!");
		}
	}
	
	private static String getClassname() {
		return CommandLineIndexingTool.class.getName();
	}
	
	private static void printUsage(String type, ArgsAssembler<? extends AbstractIndexer> assembler) {
		if(assembler == null) {
			printHelp();
		} else {
			StringBuffer usage = new StringBuffer();
			usage.append("Usage:\n")
			.append("  ").append(getClassname()).append(" ")
			.append(type).append(" ")
			.append(assembler.getUsageArgList()).append("\n");
			usage.append("  Examples:\n");
			for(String example : assembler.showCLIExamples()) {
				usage.append("      ").append(example).append("\n");
			}
			System.err.println(usage.toString());
			System.exit(-1);
		}
	}
	
	private static void printHelp() {
		StringBuffer usage = new StringBuffer();
		usage.append("Usage:\n")
		.append("  ").append(getClassname()).append(" <type> <arg1> <arg2> ... <argN>\n")
		.append("  Type:\n");
		contactTypeTips(usage);
		usage.append("\n")
		.append("  Usage of types:\n");
		Iterator<Entry<String, ArgsAssembler<? extends AbstractIndexer>>> iter = 
				ASSEMBLERS.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<String, ArgsAssembler<? extends AbstractIndexer>> pair = iter.next();
			String type = pair.getKey();
			usage.append("    ").append(type).append(" ")
			.append(pair.getValue().getUsageArgList()).append("\n")
			.append("      Examples:\n");
			String[] examples = pair.getValue().showCLIExamples();
			for(String example : examples) {
				usage.append("        ").append(example).append("\n");
			}
		}
		for(Entry<String, Object> pair : OTHERS_ASSEMBLERS.entrySet()) {
			String type = pair.getKey();
			Object value = pair.getValue();
			if(value instanceof ArgsAssembler) {
				@SuppressWarnings("rawtypes")
				ArgsAssembler<?> assembler = (ArgsAssembler) value;
				usage.append("    ").append(type).append(" ")
				.append(assembler.getUsageArgList()).append("\n")
				.append("      Examples:\n");
				String[] examples = assembler.showCLIExamples();
				for(String example : examples) {
					usage.append("        ").append(example).append("\n");
				}
			}
		}
		System.err.println(usage.toString());
		System.exit(0);
	}
	
	private static void contactTypeTips(StringBuffer usage) {
		usage.append("\t");
		Iterator<Entry<String, String>> iter = TYPE_TO_NAME_MAPPING.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<String, String> pair = iter.next();
			usage.append(pair.getKey()).append("-").append(pair.getValue()).append("\n\t");
		}
	}
	
	private static String[] arrayRebuild(String[] src) {
		String[] dst = new String[src.length - 2];
		if(src.length > 1) {
			System.arraycopy(src, 2, dst, 0, dst.length);
		}
		return dst;
	}
	
	private static void checkArgs(String[] args,
			ArgsAssembler<? extends AbstractIndexer> assembler) {
		int actualCount = args.length;
		if(actualCount < assembler.getRequiredArgCount() + 2) {
			printUsage(args[1], assembler);
		}
	}
	
	public static void main(String[] args) throws IOException {
		String type = null;
		LOG.info("Original input parameters:");
		StringBuffer params = new StringBuffer();
		params.append("  ");
		for(String arg : args) {
			params.append(arg).append(" ");
		}
		LOG.info(params.toString().trim());
		
		if(args.length <= 1) {
			printHelp();
		}
		if(args.length == 2) {
			String cmd = args[1];
			if(cmd.equalsIgnoreCase("help") 
					|| cmd.equalsIgnoreCase("-help") 
					|| cmd.equalsIgnoreCase("--help")) {
				printHelp();
			}
		}
		type = args[1];
		String[] newArgs = arrayRebuild(args);
		LOG.info("Rebuilt input parameters:");
		params = new StringBuffer();
		params.append("  ");
		for(String arg : newArgs) {
			params.append(arg).append(" ");
		}
		LOG.info(params.toString().trim());
		
		// check register table ASSEMBLERS
		ArgsAssembler<? extends AbstractIndexer> assembler = ASSEMBLERS.get(type);
		if(assembler != null) {
			run(assembler, args, newArgs);
			return;
		}
		
		// check other cases
		Object other = OTHERS_ASSEMBLERS.get(type);
		if(other != null) {
			invoke(other, newArgs);
			return;
		}
		
		printHelp();
		
	}

	private static void invoke(Object other, String[] newArgs) {
		try {
			Method method = other.getClass().getDeclaredMethod("assembleAndRun", newArgs.getClass());
			method.invoke(other, new Object[] {newArgs});
		} catch (NoSuchMethodException | SecurityException
				| IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw new RuntimeException("", e);
		}
	}

	private static void run(ArgsAssembler<? extends AbstractIndexer> assembler, 
			String[] args, String[] newArgs) {
		AbstractIndexer indexer = null;
		try {
			checkArgs(args, assembler);
			indexer = assembler.assemble(newArgs);
		} catch (Exception e) {
			printUsage(args[0], assembler);
		}
		try {
			indexer.indexDocs();
			indexer.close();
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
	
}
