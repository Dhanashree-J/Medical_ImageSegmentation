package com.unsam.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.*;
public class SegmentationRecordWriter extends RecordWriter<Text,
SegmentationResultWritable> {
private DataOutputStream out;
private Configuration conf;
public SegmentationRecordWriter(DataOutputStream out, Configuration conf) {
this.out = out;
this.conf = conf;
}
@Override
public void write(Text key, SegmentationResultWritable value)
throws IOException, InterruptedException {
// Write as JSON for easy parsing
StringBuilder json = new StringBuilder();
json.append("{\n");
json.append(" \"imageId\": \"").append(value.getImageId()).append("\",\n");
json.append(" \"originalWidth\": ").append(value.getOriginalWidth()).append(",\n");
json.append(" \"originalHeight\": ").append(value.getOriginalHeight()).append(",\n");
json.append(" \"numSegments\": ").append(value.getNumSegments()).append(",\n");
// Statistics
SegmentationStatistics stats = value.getStatistics();
json.append(" \"statistics\": {\n");
json.append(" \"averageScore\": ").append(stats.getAverageScore()).append(",\n");
json.append(" \"minScore\": ").append(stats.getMinScore()).append(",\n");
json.append(" \"maxScore\": ").append(stats.getMaxScore()).append(",\n");
json.append(" \"averageMaskArea\": ").append(stats.getAverageMaskArea()).append("\n");
json.append(" },\n");
// Segments
json.append(" \"segments\": [\n");
for (int i = 0; i < value.getMasks().size(); i++) {
SegmentationMask mask = value.getMasks().get(i);
float score = value.getScores().get(i);
json.append(" {\n");
json.append(" \"id\": ").append(i).append(",\n");
json.append(" \"score\": ").append(score).append(",\n");
json.append(" \"area\": ").append(mask.getArea()).append(",\n");
json.append(" \"width\": ").append(mask.getWidth()).append(",\n");
json.append(" \"height\": ").append(mask.getHeight()).append("\n");
json.append(" }");
if (i < value.getMasks().size() - 1) {
json.append(",");
}
json.append("\n");
}
json.append(" ],\n");
// Metadata
ImageMetadata metadata = value.getMetadata();
json.append(" \"metadata\": {\n");
json.append("\"processingTimestamp\":").append(metadata.getProcessingTimestamp()).append(",
\n");
json.append(" \"colorModel\": \"").append(metadata.getColorModel()).append("\",\n");
json.append(" \"type\": ").append(metadata.getType()).append("\n");
json.append(" }\n");
json.append("}\n");
// Write JSON to output
out.write(json.toString().getBytes("UTF-8"));
// Optionally write binary mask files
if (conf.getBoolean("unsam.output.binary.masks", true)) {
writeBinaryMasks(key.toString(), value);
}
}
private void writeBinaryMasks(String imageId, SegmentationResultWritable result)
throws IOException {
// Implementation would write individual mask files
// Format: imageId_mask_N.bin where N is segment index
// This is useful for visualization and downstream processing
// For now, this is a placeholder
}
@Override
public void close(TaskAttemptContext context) throws IOException {
if (out != null) {
out.close();
}
}
}
