package com.unsam.mapreduce;
import org.apache.hadoop.io.Writable;
import java.io.*;
public class SegmentationStatistics implements Writable {
private float averageScore;
private float minScore;
private float maxScore;
private long averageMaskArea;
public SegmentationStatistics() {}
@Override
public void write(DataOutput out) throws IOException {
out.writeFloat(averageScore);
out.writeFloat(minScore);
out.writeFloat(maxScore);
out.writeLong(averageMaskArea);
}
@Override
public void readFields(DataInput in) throws IOException {
averageScore = in.readFloat();
minScore = in.readFloat();
maxScore = in.readFloat();
averageMaskArea = in.readLong();
}
// Getters and Setters
public float getAverageScore() { return averageScore; }
public void setAverageScore(float avg) { this.averageScore = avg; }
public float getMinScore() { return minScore; }
public void setMinScore(float min) { this.minScore = min; }
public float getMaxScore() { return maxScore; }
public void setMaxScore(float max) { this.maxScore = max; }
public long getAverageMaskArea() { return averageMaskArea; }
public void setAverageMaskArea(long area) { this.averageMaskArea = area; }
}
