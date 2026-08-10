package com.unsam.mapreduce;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import org.apache.hadoop.io.BytesWritable;
import java.nio.file.Files;
public class SAMSegmentationReducer
extends Reducer<Text, ImageDataWritable, Text, org.apache.hadoop.io.Writable> {
@Override
protected void reduce(Text key,
Iterable<ImageDataWritable> values,
Context context)
throws IOException, InterruptedException {
System.out.println("Reducer started for key: " + key.toString());
for (ImageDataWritable imageData : values) {
if (imageData == null) continue;
String imageId = imageData.getImageId();
float[][][] pixels = imageData.getNormalizedPixels();
// Proper validation
if (pixels == null || pixels.length == 0 || pixels[0].length == 0) {
System.out.println("Pixels are empty for: " + imageId);
context.write(new Text(imageId),
new Text("FAILED: Empty pixel data"));
continue;
}
// Use the exact name we found in the grep
byte[] imageBytes = imageData.getImageBytes();
try {
String inputPath = imageId + ".jpg";
String outputPath = "output_" + imageId + ".jpg";
if (imageBytes == null || imageBytes.length == 0) {
context.write(key, new Text("FAILED: Image bytes were null or empty from Mapper"));
return; // Skip to the next one
}
try (FileOutputStream fos = new FileOutputStream(inputPath)) {
fos.write(imageBytes);
fos.flush();
} // file is closed here
ProcessBuilder pb = new ProcessBuilder(
"/home/hadoop/python3.12/bin/python3",
"./sam_wrapper.py",
inputPath,
outputPath
);
pb.redirectErrorStream(true);
Process process = pb.start();
BufferedReader reader = new BufferedReader(
new InputStreamReader(process.getInputStream())
);
StringBuilder outputLog = new StringBuilder();
String line;
while ((line = reader.readLine()) != null) {
outputLog.append(line).append("\n");
}
int exitCode = process.waitFor();
if (exitCode == 0) {
File outputFile = new File(outputPath); // Ensure this matches Python's output_path
if (outputFile.exists() && outputFile.length() > 0) {
// 1. Read the local file bytes
byte[] resultBytes = java.nio.file.Files.readAllBytes(outputFile.toPath());
// 2. Write to HDFS! (This is what makes the file size > 0)
context.write(key, new BytesWritable(resultBytes));
// 3. Clean up the local temp file
outputFile.delete();
} else {
context.write(key, new Text("ERROR: Python finished but " + outputPath + " is empty or
missing."));}}
else {
context.write(new Text(imageId),
new Text("FAILED:\n" + outputLog.toString()));
context.getCounter("UnSAM", "SAM_FAILURE").increment(1);
}
} catch (Exception e) {
StringWriter sw = new StringWriter();
e.printStackTrace(new PrintWriter(sw));
context.write(key, new Text("JAVA_CRASH: " + sw.toString().substring(0, Math.min(200,
sw.toString().length()))));
}
}
}
}
