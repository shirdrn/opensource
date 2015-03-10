package org.shirdrn.hadoop.dataset.cost;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.shirdrn.hadoop.dataset.CountryCodeSet;
import org.shirdrn.hadoop.utils.FileUtils;

public class SmallfilesDatasetGenerator {

	private String output;
	
	public SmallfilesDatasetGenerator(String output) throws IOException {
		super();
		this.output = output;
	}
	
	static String SEPARATOR = " ";
	
	public void generate(String file, long count) throws IOException {
		String f = this.output + FileUtils.getPathSeparator() + file;
		final BufferedWriter w = new BufferedWriter(new FileWriter(f));
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
		String output = "E:\\Dataset\\Smallfiles";
		String prefix = "data_50000_";
		SmallfilesDatasetGenerator gen = new SmallfilesDatasetGenerator(output);
		int lineCountPerFile = 30000;
		for(int i=0; i<1000; i++) {
			String file = prefix + String.format("%03d", i);
			gen.generate(file, lineCountPerFile);
		}
	}
}
