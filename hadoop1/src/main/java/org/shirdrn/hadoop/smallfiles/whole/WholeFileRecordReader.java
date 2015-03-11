package org.shirdrn.hadoop.smallfiles.whole;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends
		RecordReader<NullWritable, BytesWritable> {

	private FileSplit fileSplit;
	private JobContext jobContext;
	private NullWritable currentKey = NullWritable.get();
	private BytesWritable currentValue;
	private boolean finishConverting = false;

	@Override
	public NullWritable getCurrentKey() throws IOException,
			InterruptedException {
		return currentKey;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		return currentValue;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;	
		this.jobContext = context;
		context.getConfiguration().set("map.input.file", fileSplit.getPath().getName());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!finishConverting) {
			currentValue = new BytesWritable();
			int len = (int) fileSplit.getLength();
			byte[] content = new byte[len];
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(jobContext.getConfiguration());
			FSDataInputStream in = null;
			try {
				in = fs.open(file);
				IOUtils.readFully(in, content, 0, len);
				currentValue.set(content, 0, len);
			} finally {
				if(in != null) {
					IOUtils.closeStream(in);
				}
			}
			finishConverting = true;
			return true;
		}
		return false;
	}

	@Override
	public float getProgress() throws IOException {
		float progress = 0;
		if(finishConverting) {
			progress = 1;
		}
		return progress;
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
