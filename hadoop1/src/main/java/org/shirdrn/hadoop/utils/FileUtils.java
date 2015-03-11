package org.shirdrn.hadoop.utils;

public class FileUtils {

	public static String getPathSeparator() {
		String os = System.getProperty("os.name");
		String pathSeparator = null;
		if (os.contains("windows")) {
			pathSeparator = "\\";
		} else {
			pathSeparator = "/";
		}
		return pathSeparator;
	}

}
