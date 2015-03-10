package org.shirdrn.solr.indexing.common.config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.solr.indexing.common.DocCreator;
import org.shirdrn.solr.indexing.common.ValueHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class FieldMappingBuilder {
	
	private static final Log LOG = LogFactory.getLog(FieldMappingBuilder.class);
	private static final String ELEM_EXTERNAL_NAME = "externalName";
	private static final String ELEM_SOLR_NAME = "solrName";
	private static final String ELEM_SOLR_TYPE = "solrType";
	private static final Map<String, ValueHandler<?>> valueHandlers = new HashMap<>(0);
	private static final ValueHandler<?> defaultValueHandler = new StringValueHandler();
	static {
		valueHandlers.put("string", defaultValueHandler);
		valueHandlers.put("byte", new ByteValueHandler());
		valueHandlers.put("short", new ShortValueHandler());
		valueHandlers.put("int", new IntValueHandler());
		valueHandlers.put("long", new LongValueHandler());
		valueHandlers.put("float", new FloatValueHandler());
		valueHandlers.put("double", new DoubleValueHandler());
		LOG.info("Loaded value handlers: " + valueHandlers.values());
	}
	private final LinkedHashMap<String, MappedField> mappedFields = new LinkedHashMap<>(0);
	private final LinkedHashMap<Integer, MappedField> fieldIndexes = new LinkedHashMap<>(0);
	private DocCreator<?> docCreator;
	private boolean initialized = false;
	
	public synchronized void build() {
		if(!initialized) {
			initialized = true;
			parse("schema-mapping.xml");
			printMetadata();
		}
	}
	
	public synchronized void build(String path) {
		if(!initialized) {
			initialized = true;
			File file = new File(path);
			parse(file);
			printMetadata();
		}
	}

	private void printMetadata() {
		if(!mappedFields.isEmpty()) {
			LOG.info("Load mapped fields with type:");
			for(Map.Entry<String, MappedField> entry : mappedFields.entrySet()) {
				LOG.info(" " + entry.getKey() + "\t=>\t" + entry.getValue().solrType);
			}
		}
		if(!fieldIndexes.isEmpty()) {
			LOG.info("Load mapped fields with index:");
			for(Map.Entry<Integer, MappedField> entry : fieldIndexes.entrySet()) {
				LOG.info(" " + entry.getKey() + "\t=>\t" + entry.getValue().field);
			}
		}
	}
	
	private void parse(File file) {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance(); 
		try {
			DocumentBuilder builder=factory.newDocumentBuilder(); 
			Document doc = builder.parse(file);
			buildXML(doc);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void parse(String file) {
		InputStream in = null;
		try {
			in = getClass().getClassLoader().getResourceAsStream(file);
			DocumentBuilderFactory factory=DocumentBuilderFactory.newInstance(); 
			DocumentBuilder builder=factory.newDocumentBuilder(); 
			Document doc = builder.parse(in);
			buildXML(doc);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(in != null) {
					in.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void buildXML(Document doc) {
		NodeList nl = doc.getElementsByTagName("mapping");
		for(int i = 0; i < nl.getLength(); i++) {
			Node mappings = nl.item(i);
			NodeList fieldsNodeList = mappings.getChildNodes();
			for(int j = 0; j < fieldsNodeList.getLength(); j++) {
				Node fieldsNode = fieldsNodeList.item(j);
				NodeList fnList = fieldsNode.getChildNodes();
				int index = 0;
				for(int k = 0; k < fnList.getLength(); k++) {
					Node fn = fnList.item(k);
					NodeList leafs = fn.getChildNodes();
					MappedField mapped = new MappedField();
					for (int l = 0; l < leafs.getLength(); l++) {
						Node elem = leafs.item(l);
						String name = elem.getNodeName();
						String value = elem.getTextContent();
						if(name != null && value != null) {
							name = name.trim();
							value = value.trim();
							if(!name.isEmpty() && !value.isEmpty()) {
								if(name.equals(ELEM_EXTERNAL_NAME)) {
									mapped.externalName = value;
								} else if(name.equals(ELEM_SOLR_NAME)) {
									mapped.solrName = value;
								} else if(name.equals(ELEM_SOLR_TYPE)) {
									mapped.solrType = value;
									if(valueHandlers.get(value) != null) {
										mapped.valueHandler = valueHandlers.get(value);
									}
								}
								LOG.info("Read field: name=" + name + ", value=" + value);
							}
						}
					}
					String field = mapped.solrName;
					if(field == null) {
						field = mapped.externalName;
					}
					if(field != null) {
						mapped.field = field;
						mappedFields.put(field, mapped);
						fieldIndexes.put(index++, mapped);
					}
				}
			}
		}		
	}
	
	public class MappedField {
		
		String field;
		String externalName;
		String solrName;
		String solrType;
		ValueHandler<?> valueHandler = defaultValueHandler;
		
		public String getField() {
			return field;
		}
		public void setField(String field) {
			this.field = field;
		}
		public String getExternalName() {
			return externalName;
		}
		public String getSolrName() {
			return solrName;
		}
		public String getSolrType() {
			return solrType;
		}
		public ValueHandler<?> getValueHandler() {
			return valueHandler;
		}
		
		@Override
		public String toString() {
			return "[field=" + field +
					",externalName=" + externalName + 
					",solrName=" + solrName + 
					",solrType=" + solrType + 
					",valueHandler=" + (valueHandler==null ? "" : valueHandler.getClass().getSimpleName()) +
					"]";
		}
	}
	
	public ValueHandler<?> getValueHandlerByType(String solrType) {
		return valueHandlers.get(solrType);
	}
	
	public MappedField getMappedField(String field) {
		return mappedFields.get(field);
	}
	
	public Set<String> getFields() {
		return mappedFields.keySet();
	}
	
	public MappedField getMappedField(int index) {
		return fieldIndexes.get(index);
	}
	
	public static class StringValueHandler implements ValueHandler<String> {
		@Override
		public String handle(String value) {
			if(value.startsWith("\"") && value.endsWith("\"")) {
				value = value.substring(1, value.length()-1);
			}
			return value;
		}
		@Override
		public String toString() {
			return "string";
		}
	}
	
	public static class ShortValueHandler implements ValueHandler<Short> {
		@Override
		public Short handle(String value) {
			return Short.parseShort(value);
		}
		@Override
		public String toString() {
			return "short";
		}
	}
	
	public static class ByteValueHandler implements ValueHandler<Byte> {
		@Override
		public Byte handle(String value) {
			return Byte.parseByte(value);
		}
		@Override
		public String toString() {
			return "byte";
		}
	}
	
	public static class IntValueHandler implements ValueHandler<Integer> {
		@Override
		public Integer handle(String value) {
			return Integer.parseInt(value);
		}
		@Override
		public String toString() {
			return "int";
		}
	}
	
	public static class LongValueHandler implements ValueHandler<Long> {
		@Override
		public Long handle(String value) {
			return Long.parseLong(value);
		}
		@Override
		public String toString() {
			return "long";
		}
	}
	
	public static class FloatValueHandler implements ValueHandler<Float> {
		@Override
		public Float handle(String value) {
			return Float.parseFloat(value);
		}
		@Override
		public String toString() {
			return "float";
		}
	}
	
	public static class DoubleValueHandler implements ValueHandler<Double> {
		@Override
		public Double handle(String value) {
			return Double.parseDouble(value);
		}
		@Override
		public String toString() {
			return "double";
		}
	}
	
	public DocCreator<?> getDocCreator() {
		return docCreator;
	}

	public void setDocCreator(DocCreator<?> docCreator) {
		this.docCreator = docCreator;
	}

}
