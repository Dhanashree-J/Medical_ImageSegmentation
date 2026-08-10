package com.unsam.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.DataOutputStream;
import java.io.IOException;
public class CompressedSegmentationRecordWriter extends RecordWriter<Text,
SegmentationResultWritable> {
private DataOutputStream out;
private Configuration conf;
public CompressedSegmentationRecordWriter(
java.io.OutputStream compressedOut,
Configuration conf) {
this.out = new DataOutputStream(compressedOut);
this.conf = conf;
}
@Override
public void write(Text key, SegmentationResultWritable value)
throws IOException, InterruptedException {
// Same as uncompressed, but output stream is already compressed
SegmentationRecordWriter delegate = new SegmentationRecordWriter(out, conf);
delegate.write(key, value);
}
@Override
public void close(TaskAttemptContext context) throws IOException {
if (out != null) {
out.close();
}
}
}
