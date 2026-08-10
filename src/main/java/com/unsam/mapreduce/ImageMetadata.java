package com.unsam.mapreduce;
import org.apache.hadoop.io.Writable;
import java.io.*;
public class ImageMetadata implements Writable {
private String imageId;
private int width;
private int height;
private String colorModel;
private int type;
private long processingTimestamp;
public ImageMetadata() {}
@Override
public void write(DataOutput out) throws IOException {
out.writeUTF(imageId != null ? imageId : "");
out.writeInt(width);
out.writeInt(height);
out.writeUTF(colorModel != null ? colorModel : "");
out.writeInt(type);
out.writeLong(processingTimestamp);
}
@Override
public void readFields(DataInput in) throws IOException {
imageId = in.readUTF();
width = in.readInt();
height = in.readInt();
colorModel = in.readUTF();
type = in.readInt();
processingTimestamp = in.readLong();
}
// Getters and Setters
public String getImageId() { return imageId; }
public void setImageId(String id) { this.imageId = id; }
public int getWidth() { return width; }
public void setWidth(int w) { this.width = w; }
public int getHeight() { return height; }
public void setHeight(int h) { this.height = h; }
public String getColorModel() { return colorModel; }
public void setColorModel(String model) { this.colorModel = model; }
public int getType() { return type; }
public void setType(int t) { this.type = t; }
public long getProcessingTimestamp() { return processingTimestamp; }
public void setProcessingTimestamp(long ts) { this.processingTimestamp = ts; }
}
