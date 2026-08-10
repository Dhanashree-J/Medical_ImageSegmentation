package com.unsam.mapreduce;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.awt.Image;
import java.util.*;
public class ImagePreprocessingMapper extends Mapper<Text, BytesWritable, Text,
ImageDataWritable> {
private static final int SAM_INPUT_SIZE = 1024;
@Override
protected void map(Text key, BytesWritable value, Context context)
throws IOException, InterruptedException {
String imageId = key.toString();
System.out.println("Processing image: " + imageId);
// Copy image bytes
byte[] imageBytes = Arrays.copyOf(value.getBytes(), value.getLength());
try {
// Step 1: Decode image
BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageBytes));
if (image == null) {
ImageDataWritable output = new ImageDataWritable(); // no-arg constructor
output.setImageId(imageId); // set the ID
output.setNormalizedPixels(null); // set pixels as null
context.write(key, output); // write to context
System.out.println("Image decode failed: " + imageId);
return;
}
// Step 2: Resize
BufferedImage resized = resizeImage(image, SAM_INPUT_SIZE, SAM_INPUT_SIZE);
// Step 3: Normalize
float[][][] pixels = normalizeImage(resized);
// Step 4: Set into writable
ImageDataWritable output = new ImageDataWritable();
output.setImageId(imageId);
output.setImageBytes(imageBytes);
output.setNormalizedPixels(pixels);
// Send to reducer
context.write(key, output);
} catch (Exception e) {
System.out.println("Mapper error for " + imageId + ": " + e.getMessage());
ImageDataWritable output = new ImageDataWritable();
output.setImageId(imageId);
output.setNormalizedPixels(null);
context.write(key, output);
}
context.getCounter("UnSAM", "IMAGES_PROCESSED").increment(1);
//-- context.getCounter("UnSAM",
"BYTES_SENT_TO_REDUCER").increment(imageBytes.length);
}
// ---------- Helpers ----------
private BufferedImage decodeImage(byte[] imageBytes) throws IOException {
ByteArrayInputStream bais = new ByteArrayInputStream(imageBytes);
return ImageIO.read(bais);
}
private BufferedImage resizeImage(BufferedImage original, int width, int height) {
Image tmp = original.getScaledInstance(width, height, Image.SCALE_SMOOTH);
BufferedImage resized = new BufferedImage(width, height,
BufferedImage.TYPE_INT_RGB);
resized.getGraphics().drawImage(tmp, 0, 0, null);
return resized;
}
private float[][][] normalizeImage(BufferedImage image) {
int width = image.getWidth();
int height = image.getHeight();
float[][][] normalized = new float[height][width][3];
for (int y = 0; y < height; y++) {
for (int x = 0; x < width; x++) {
int rgb = image.getRGB(x, y);
int r = (rgb >> 16) & 0xFF;
int g = (rgb >> 8) & 0xFF;
int b = rgb & 0xFF;
normalized[y][x][0] = r / 255.0f;
normalized[y][x][1] = g / 255.0f;
normalized[y][x][2] = b / 255.0f;
}
}
return normalized;
}
}
