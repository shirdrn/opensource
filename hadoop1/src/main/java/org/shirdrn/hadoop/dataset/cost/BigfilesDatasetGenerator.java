package org.shirdrn.hadoop.dataset.cost;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.shirdrn.hadoop.dataset.CountryCodeSet;

public class BigfilesDatasetGenerator {

	final BufferedWriter w;
	
	public BigfilesDatasetGenerator(String output) throws IOException {
		super();
		w = new BufferedWriter(new FileWriter(output));
	}
	
	static String SEPARATOR = " ";
	
	public void generate(long count) throws IOException {
		Random rCode = new Random();
		Random r = new Random();
		List<String> codeSet = CountryCodeSet.newInstance().getCountryCodes();
		int codeCount = codeSet.size();
		for(long i=0; i<count; i++) {
			int index = rCode.nextInt(codeCount);
			String code = codeSet.get(index);
			long value = r.nextInt(999999999);
			StringBuilder builder = new StringBuilder();
			builder.append(code).append(SEPARATOR);
			long time = System.nanoTime();
			builder.append(time-value/Math.max(1, (int) Math.sqrt(value))).append(SEPARATOR)
			.append(time).append(SEPARATOR).append(value);
			w.write(builder.toString());
			w.newLine();
		}
		w.close();
	}
	
	public static void main(String[] args) throws IOException {
		String output = "/home/shirdrn/dataset/data_10m";
		BigfilesDatasetGenerator gen = new BigfilesDatasetGenerator(output);
		gen.generate(10000000);
	}
}
