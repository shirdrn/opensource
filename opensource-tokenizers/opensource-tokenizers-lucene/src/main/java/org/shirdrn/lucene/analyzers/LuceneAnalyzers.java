package org.shirdrn.lucene.analyzers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;

import com.google.common.base.Throwables;

public class LuceneAnalyzers {

	private static final Log LOG = LogFactory.getLog(LuceneAnalyzers.class);
	private final Analyzer analyzer;
	
	public LuceneAnalyzers() {
		try {
			analyzer = new SmartChineseAnalyzer(false);
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}
	
	private void analyze(String sentence) throws IOException {
		TokenStream tokenStream = analyzer.tokenStream("", new StringReader(sentence));
		tokenStream.reset();
		tokenStream.addAttribute(CharTermAttribute.class); 
		while (tokenStream.incrementToken()) {  
			CharTermAttributeImpl attr = (CharTermAttributeImpl) tokenStream.getAttribute(CharTermAttribute.class);  
			String word = attr.toString().trim();
			if(!word.isEmpty()) {
				LOG.info("Analyzed: " + word);
			}
			tokenStream.end();
		}
		tokenStream.close();
	}
	
	public void process() {
		BufferedReader br = null;
		InputStream in = null;
		try {
			in = this.getClass().getClassLoader().getResourceAsStream("keywords.txt");
			br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
			String line = null;
			while((line = br.readLine()) != null) {
				analyze(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}

	
	public static void main(String[] args) {
		new LuceneAnalyzers().process();;
	}

}
