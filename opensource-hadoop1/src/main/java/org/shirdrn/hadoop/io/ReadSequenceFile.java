package org.shirdrn.hadoop.io;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class ReadSequenceFile {

	public static void main(String[] args) {
		
		String targetFile = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = null; 
		
		SequenceFile.Reader reader = null;
		
		try {
			fs = FileSystem.get(URI.create(targetFile), conf);
			Path path = new Path(targetFile);
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			while (reader.next(key, value)) {
				System.out.printf("domain=" + key + ",len=" + value);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
		
	}
}
