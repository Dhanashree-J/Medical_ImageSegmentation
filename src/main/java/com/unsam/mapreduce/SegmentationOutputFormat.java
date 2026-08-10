package com.unsam.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;
public class SegmentationOutputFormat extends FileOutputFormat<Text,
SegmentationResultWritable> {
@Override
public RecordWriter<Text, SegmentationResultWritable> getRecordWriter(
TaskAttemptContext context) throws IOException, InterruptedException {
Configuration conf = context.getConfiguration();
boolean isCompressed = getCompressOutput(context);
Path file = getDefaultWorkFile(context, ".json");
FileSystem fs = file.getFileSystem(conf);
if (isCompressed) {
return new CompressedSegmentationRecordWriter(
new GZIPOutputStream(fs.create(file, false)),
conf
);
} else {
return new SegmentationRecordWriter(
fs.create(file, false),
conf
);
}
}
}
UnSAMSegmentati
