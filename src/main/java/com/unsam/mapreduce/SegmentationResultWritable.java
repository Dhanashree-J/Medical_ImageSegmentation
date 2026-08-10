package com.unsam.mapreduce;
import org.apache.hadoop.io.Writable;
import java.io.*;
import java.util.*;
public class SegmentationResultWritable implements Writable {
private String imageId;
private List<SegmentationMask> masks;
private List<Float> scores;
private ImageMetadata metadata;
private int originalWidth;
private int originalHeight;
private int numSegments;
private SegmentationStatistics statistics;
public SegmentationResultWritable() {
this.masks = new ArrayList<>();
this.scores = new ArrayList<>();
this.metadata = new ImageMetadata();
this.statistics = new SegmentationStatistics();
}
@Override
public void write(DataOutput out) throws IOException {
out.writeUTF(imageId);
out.writeInt(masks.size());
for (SegmentationMask mask : masks) {
mask.write(out);
}
out.writeInt(scores.size());
for (Float score : scores) {
out.writeFloat(score);
}
metadata.write(out);
out.writeInt(originalWidth);
out.writeInt(originalHeight);
out.writeInt(numSegments);
statistics.write(out);
}
@Override
public void readFields(DataInput in) throws IOException {
imageId = in.readUTF();
int numMasks = in.readInt();
masks = new ArrayList<>(numMasks);
for (int i = 0; i < numMasks; i++) {
SegmentationMask mask = new SegmentationMask();
mask.readFields(in);
masks.add(mask);
}
int numScores = in.readInt();
scores = new ArrayList<>(numScores);
for (int i = 0; i < numScores; i++) {
scores.add(in.readFloat());
}
metadata = new ImageMetadata();
metadata.readFields(in);
originalWidth = in.readInt();
originalHeight = in.readInt();
numSegments = in.readInt();
statistics = new SegmentationStatistics();
statistics.readFields(in);
}
// Getters and Setters
public String getImageId() { return imageId; }
public void setImageId(String id) { this.imageId = id; }
public List<SegmentationMask> getMasks() { return masks; }
public void setMasks(List<SegmentationMask> masks) { this.masks = masks; }
public List<Float> getScores() { return scores; }
public void setScores(List<Float> scores) { this.scores = scores; }
public ImageMetadata getMetadata() { return metadata; }
public void setMetadata(ImageMetadata metadata) { this.metadata = metadata; }
public int getOriginalWidth() { return originalWidth; }
public void setOriginalWidth(int width) { this.originalWidth = width; }
public int getOriginalHeight() { return originalHeight; }
public void setOriginalHeight(int height) { this.originalHeight = height; }
public int getNumSegments() { return numSegments; }
public void setNumSegments(int num) { this.numSegments = num; }
public SegmentationStatistics getStatistics() { return statistics; }
public void setStatistics(SegmentationStatistics stats) { this.statistics = stats; }
}
