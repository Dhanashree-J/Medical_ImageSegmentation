package com.unsam.mapreduce;
import org.apache.hadoop.io.Writable;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
public class SegmentationMask implements Writable {
private boolean[][] maskArray;
private int width;
private int height;
private long area;
public SegmentationMask() {}
public SegmentationMask(boolean[][] mask) {
  this.maskArray = mask;
this.height = mask.length;
this.width = mask[0].length;
this.area = calculateArea();
}
private long calculateArea() {
long count = 0;
for (int i = 0; i < height; i++) {
for (int j = 0; j < width; j++) {
if (maskArray[i][j]) count++;
}
}
return count;
}
@Override
public void write(DataOutput out) throws IOException {
out.writeInt(height);
out.writeInt(width);
out.writeLong(area);
List<Integer> rle = runLengthEncode();
out.writeInt(rle.size());
for (int val : rle) {
out.writeInt(val);
}
}
@Override
public void readFields(DataInput in) throws IOException {
height = in.readInt();
width = in.readInt();
area = in.readLong();
int rleSize = in.readInt();
List<Integer> rle = new ArrayList<>(rleSize);
for (int i = 0; i < rleSize; i++) {
rle.add(in.readInt());
}
maskArray = runLengthDecode(rle);
}
private List<Integer> runLengthEncode() {
List<Integer> rle = new ArrayList<>();
boolean currentValue = false;
int count = 0;
for (int i = 0; i < height; i++) {
for (int j = 0; j < width; j++) {
if (maskArray[i][j] == currentValue) {
count++;
} else {
  rle.add(count);
currentValue = !currentValue;
count = 1;
}
}
}
rle.add(count);
return rle;
}
private boolean[][] runLengthDecode(List<Integer> rle) {
boolean[][] decoded = new boolean[height][width];
boolean currentValue = false;
int idx = 0;
for (int runLength : rle) {
for (int i = 0; i < runLength; i++) {
int row = idx / width;
int col = idx % width;
decoded[row][col] = currentValue;
idx++;
}
currentValue = !currentValue;
}
return decoded;
}
public boolean[][] getMaskArray() { return maskArray; }
public int getWidth() { return width; }
public int getHeight() { return height; }
public long getArea() { return area; }
}
