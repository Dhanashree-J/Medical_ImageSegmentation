package com.unsam.mapreduce;
import org.apache.hadoop.io.Writable;
import java.io.*;
public class PromptPoint implements Writable {
private int x;
private int y;
private int label;
public PromptPoint() {}
public PromptPoint(int x, int y, int label) {
this.x = x;
this.y = y;
this.label = label;
}
@Override
public void write(DataOutput out) throws IOException {
out.writeInt(x);
out.writeInt(y);
out.writeInt(label);
}
@Override
public void readFields(DataInput in) throws IOException {
x = in.readInt();
y = in.readInt();
label = in.readInt();
}
public int getX() { return x; }
public int getY() { return y; }
public int getLabel() { return label; }
}
