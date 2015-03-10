package org.shirdrn.hadoop.extremum;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class ExtremumOutputFormat extends TextOutputFormat<Text, Extremum> {

	@Override
	public RecordWriter<Text, Extremum> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		String fieldSeparator= conf.get("mapred.textoutputformat.separator", "\t");
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			extension = codec.getDefaultExtension();
		}
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, false);
		if (!isCompressed) {
			return new ExtremumRecordWriter(fileOut, fieldSeparator);
		} else {
			DataOutputStream out = new DataOutputStream(codec.createOutputStream(fileOut));	
			return new ExtremumRecordWriter(out, fieldSeparator);			
		}
	}
	
	public static class ExtremumRecordWriter extends RecordWriter<Text, Extremum> {

		private static final String CHARSET = "UTF-8";
		protected DataOutputStream out;
	    private final byte[] fieldSeparator;
	    private static final byte[] NEWLINE;
		static {
			try {
				NEWLINE = "\n".getBytes(CHARSET);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + CHARSET + " encoding.");
			}
		}

	    public ExtremumRecordWriter(DataOutputStream out) {
	    	this(out, "\t");
	    }
	    
		public ExtremumRecordWriter(DataOutputStream out, String fieldSeparator) {
			super();
			this.out = out;
			try {
				this.fieldSeparator = fieldSeparator.getBytes(CHARSET);
			} catch (UnsupportedEncodingException e) {
				throw new IllegalArgumentException("can't find " + CHARSET + " encoding.");
			}
		}

		@Override
		public synchronized void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			out.close();			
		}

		@Override
		public synchronized void write(Text key, Extremum value) throws IOException,
				InterruptedException {
			if(key != null) {
				out.write(key.getBytes(), 0, key.getLength());
				out.write(fieldSeparator);
			}
			if(value !=null) {
				out.write(value.getMinValue().toString().getBytes());
				out.write(fieldSeparator);
				out.write(value.getMaxValue().toString().getBytes());
			}
			out.write(NEWLINE);
		}
		
	}

}
