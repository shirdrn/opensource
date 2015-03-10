package org.shirdrn.hadoop.dataset;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.shirdrn.hadoop.utils.FileUtils;

public class CountryCodeSet {
	
	private static List<String> CODES = new ArrayList<String>();
	private static final String WORKSPACE = System.getProperty("user.dir");
	private static final String SEPARATOR = FileUtils.getPathSeparator();
	private static final String CODE_FILE = WORKSPACE + SEPARATOR
			+ "src" + SEPARATOR + "main" + SEPARATOR + "resources"
			+ SEPARATOR + "country_codes";
	private static final CountryCodeSet INSTANCE = new CountryCodeSet();

	static {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(
					CODE_FILE)));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (!line.trim().isEmpty()) {
					CODES.add(line.trim());
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private CountryCodeSet() {
		super();
	}

	public static CountryCodeSet newInstance() {
		return INSTANCE;
	}

	public List<String> getCountryCodes() {
		return CODES;
	}
}
