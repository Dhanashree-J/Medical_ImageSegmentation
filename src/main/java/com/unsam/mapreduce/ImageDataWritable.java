package com.unsam.mapreduce;
import org.apache.hadoop.io.Writable;
import java.io.*;
import java.util.*;
public class ImageDataWritable implements Writable {
private String imageId;
private float[][][] normalizedPixels;
private List<PromptPoint> promptPoints;
private ImageMetadata metadata;
private int originalWidth;
private int originalHeight;
public ImageDataWritable() {
this.promptPoints = new ArrayList<>();
this.metadata = new ImageMetadata();
}
@Override
public void write(DataOutput out) throws IOException {
out.writeUTF(imageId != null ? imageId : "");
// Handle normalizedPixels safely
if (normalizedPixels != null && normalizedPixels.length > 0) {
int height = normalizedPixels.length;
int width = normalizedPixels[0].length;
int channels = normalizedPixels[0][0].length;
out.writeInt(height);
out.writeInt(width);
out.writeInt(channels);
for (int h = 0; h < height; h++) {
for (int w = 0; w < width; w++) {
for (int c = 0; c < channels; c++) {
out.writeFloat(normalizedPixels[h][w][c]);}}}
} else {
out.writeInt(0); // height
out.writeInt(0); // width
out.writeInt(0); // channels
}
// Handle promptPoints safely
if (promptPoints != null) {
out.writeInt(promptPoints.size());
for (PromptPoint point : promptPoints) {
point.write(out);
}
} else {
out.writeInt(0);
}
// Handle metadata safely
if (metadata != null) {
metadata.write(out);
} else {
// Write empty/default metadata
out.writeUTF(""); // or other default values depending on your Metadata class
}
out.writeInt(originalWidth);
out.writeInt(originalHeight);
// ADD THIS AT THE END
if (imageBytes != null) {
out.writeInt(imageBytes.length);
out.write(imageBytes);
} else {
out.writeInt(0);
}
}
@Override
public void readFields(DataInput in) throws IOException {
this.imageId = in.readUTF();
int height = in.readInt();
int width = in.readInt();
int channels = in.readInt();
normalizedPixels = new float[height][width][channels];
for (int h = 0; h < height; h++) {
for (int w = 0; w < width; w++) {
for (int c = 0; c < channels; c++) {
normalizedPixels[h][w][c] = in.readFloat();}}}
int numPrompts = in.readInt();
promptPoints = new ArrayList<>(numPrompts);
for (int i = 0; i < numPrompts; i++) {
PromptPoint point = new PromptPoint();
point.readFields(in);
promptPoints.add(point);
}
metadata = new ImageMetadata();
metadata.readFields(in);
this.originalWidth = in.readInt();
this.originalHeight = in.readInt();
int byteLength = in.readInt();
if (byteLength > 0) {
this.imageBytes = new byte[byteLength];
in.readFully(this.imageBytes);
} else {
this.imageBytes = new byte[0];
}
}
// Getters and Setters
public String getImageId() { return imageId; }
public void setImageId(String imageId) { this.imageId = imageId; }
public float[][][] getNormalizedPixels() { return normalizedPixels; }
public void setNormalizedPixels(float[][][] pixels) { this.normalizedPixels = pixels; }
public List<PromptPoint> getPromptPoints() { return promptPoints; }
public void setPromptPoints(List<PromptPoint> points) { this.promptPoints = points; }
public ImageMetadata getMetadata() { return metadata; }
public void setMetadata(ImageMetadata metadata) { this.metadata = metadata; }
public int getOriginalWidth() { return originalWidth; }
public void setOriginalWidth(int width) { this.originalWidth = width; }
public int getOriginalHeight() { return originalHeight; }
public void setOriginalHeight(int height) { this.originalHeight = height; }
public byte[] imageBytes;
public byte[] getImageBytes() {
return imageBytes;
}
public void setImageBytes(byte[] bytes) {
this.imageBytes = bytes;
}
}
