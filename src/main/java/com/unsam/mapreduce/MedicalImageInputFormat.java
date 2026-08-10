package com.unsam.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import java.io.*;
public class MedicalImageInputFormat extends FileInputFormat<Text, BytesWritable> {
@Override
protected boolean isSplitable(JobContext context, Path file) {
// Images are atomic units - don't split them
return false;
}
@Override
public RecordReader<Text, BytesWritable> createRecordReader(
InputSplit split,
TaskAttemptContext context) throws IOException, InterruptedException {
return new MedicalImageRecordReader();
}
}
class MedicalImageRecordReader extends RecordReader<Text, BytesWritable> {
private FileSplit fileSplit;
private Configuration conf;
private boolean processed = false;
private Text key = new Text();
private BytesWritable value = new BytesWritable();
@Override
public void initialize(InputSplit split, TaskAttemptContext context)
throws IOException, InterruptedException {
this.fileSplit = (FileSplit) split;
this.conf = context.getConfiguration();
}
@Override
public boolean nextKeyValue() throws IOException, InterruptedException {
if (processed) {
return false;
}
// Read the entire image file
Path file = fileSplit.getPath();
FileSystem fs = file.getFileSystem(conf);
// Set key as image filename (without extension)
String filename = file.getName();
String imageId = filename.substring(0, filename.lastIndexOf('.'));
key.set(imageId);
// Read image bytes
FSDataInputStream in = null;
  try {
in = fs.open(file);
long fileLength = fileSplit.getLength();
byte[] buffer = new byte[(int) fileLength];
in.readFully(buffer);
value.set(buffer, 0, buffer.length);
} finally {
if (in != null) {
in.close();
}
}
processed = true;
return true;
}
@Override
public Text getCurrentKey() {
return key;
}
@Override
public BytesWritable getCurrentValue() {
return value;
}
@Override
public float getProgress() {
return processed ? 1.0f : 0.0f;
}
@Override
public void close() throws IOException {
// Nothing to close
}
}
