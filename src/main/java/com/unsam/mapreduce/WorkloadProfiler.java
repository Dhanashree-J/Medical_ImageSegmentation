package com.unsam.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.*;
public class WorkloadProfiler {
private Configuration conf;
private static final int MIN_SAMPLE_SIZE = 100;
private static final float SAMPLE_PERCENTAGE = 0.10f;
public WorkloadProfiler(Configuration conf) {
this.conf = conf;
}
public ProfileResults profileWorkload(String inputPath, String modelType)
throws IOException {
System.out.println("\n=== Starting Workload Profiling ===");
System.out.println("Input path: " + inputPath);
System.out.println("Model type: " + modelType);
ProfileResults results = new ProfileResults();
results.setModelType(modelType);
List<Path> allFiles = listInputFiles(inputPath);
int totalFiles = allFiles.size();
results.setDatasetSize(totalFiles);
System.out.println("Total images in dataset: " + totalFiles);
int sampleSize = Math.max(MIN_SAMPLE_SIZE, (int)(totalFiles *
SAMPLE_PERCENTAGE));
sampleSize = Math.min(sampleSize, totalFiles);
List<Path> sampleFiles = sampleFiles(allFiles, sampleSize);
System.out.println("Profiling sample size: " + sampleSize + " images");
ImageCharacteristics imgChar = profileImageCharacteristics(sampleFiles);
results.setAverageImageSizeMB(imgChar.avgSizeMB);
results.setAverageWidth(imgChar.avgWidth);
results.setAverageHeight(imgChar.avgHeight);
ProcessingMetrics procMetrics = profileProcessingTime(sampleFiles, modelType);
results.setAverageMapTimeMs(procMetrics.avgMapTimeMs);
results.setAverageReduceTimeMs(procMetrics.avgReduceTimeMs);
results.setAverageMemoryUsageMB(procMetrics.avgMemoryMB);
calculateRecommendations(results);
printProfileSummary(results);
System.out.println("=== Profiling Complete ===\n");
return results;
}
private List<Path> listInputFiles(String inputPath) throws IOException {
List<Path> files = new ArrayList<>();
FileSystem fs = FileSystem.get(conf);
Path input = new Path(inputPath);
RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, true);
while (iterator.hasNext()) {
LocatedFileStatus status = iterator.next();
if (status.isFile() && !status.getPath().getName().startsWith("_")) {
files.add(status.getPath());
}
}
return files;
}
private List<Path> sampleFiles(List<Path> allFiles, int sampleSize) {
List<Path> sampled = new ArrayList<>();
Random random = new Random(42);
if (sampleSize >= allFiles.size()) {
return new ArrayList<>(allFiles);
}
List<Path> pool = new ArrayList<>(allFiles);
for (int i = 0; i < sampleSize; i++) {
int idx = random.nextInt(pool.size());
sampled.add(pool.remove(idx));
}
return sampled;
}
private ImageCharacteristics profileImageCharacteristics(List<Path> files)
throws IOException {
FileSystem fs = FileSystem.get(conf);
ImageCharacteristics characteristics = new ImageCharacteristics();
long totalSizeBytes = 0;
int totalWidth = 0;
int totalHeight = 0;
int count = 0;
System.out.println("Analyzing image characteristics...");
for (Path file : files) {
try {
FileStatus status = fs.getFileStatus(file);
totalSizeBytes += status.getLen();
FSDataInputStream in = fs.open(file);
byte[] buffer = new byte[(int)status.getLen()];
in.readFully(buffer);
in.close();
ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
BufferedImage img = ImageIO.read(bais);
if (img != null) {
totalWidth += img.getWidth();
totalHeight += img.getHeight();
count++;
}
} catch (Exception e) {
System.err.println("Error reading " + file + ": " + e.getMessage());
}
}
if (count > 0) {
characteristics.avgSizeMB = (int)((totalSizeBytes / count) / (1024 * 1024));
characteristics.avgWidth = totalWidth / count;
characteristics.avgHeight = totalHeight / count;
}
System.out.println("Average image size: " + characteristics.avgSizeMB + " MB");
System.out.println("Average dimensions: " + characteristics.avgWidth +"x" + characteristics.avgHeight);
return characteristics;
}
private ProcessingMetrics profileProcessingTime(List<Path> files, String modelType)
throws IOException {
ProcessingMetrics metrics = new ProcessingMetrics();
System.out.println("Profiling processing time...");
FileSystem fs = FileSystem.get(conf);
long totalMapTime = 0;
long totalReduceTime = 0;
long totalMemory = 0;
int count = 0;
int profilingSample = Math.min(50, files.size());
for (int i = 0; i < profilingSample; i++) {
Path file = files.get(i);
try {
long mapStart = System.currentTimeMillis();
FSDataInputStream in = fs.open(file);
FileStatus status = fs.getFileStatus(file);
byte[] buffer = new byte[(int)status.getLen()];
in.readFully(buffer);
in.close();
ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
BufferedImage img = ImageIO.read(bais);
if (img != null) {
long mapTime = System.currentTimeMillis() - mapStart;
totalMapTime += mapTime;
long reduceTime = estimateReduceTime(modelType, 1024, 1024);
totalReduceTime += reduceTime;
long memory = estimateMemoryUsage(modelType, 1024, 1024);
totalMemory += memory;
count++;
}
} catch (Exception e) {
System.err.println("Error profiling " + file + ": " + e.getMessage());
}
if ((i + 1) % 10 == 0) {
System.out.println("Profiled " + (i + 1) + "/" + profilingSample + " images");
}
}
if (count > 0) {
metrics.avgMapTimeMs = totalMapTime / count;
metrics.avgReduceTimeMs = totalReduceTime / count;
metrics.avgMemoryMB = (int)(totalMemory / count / (1024 * 1024));
}
System.out.println("Average map time: " + metrics.avgMapTimeMs + " ms");
System.out.println("Average reduce time: " + metrics.avgReduceTimeMs + " ms");
System.out.println("Average memory usage: " + metrics.avgMemoryMB + " MB");
return metrics;
}
private long estimateReduceTime(String modelType, int width, int height) {
long baseTime;
switch (modelType) {
case "vit_b":
baseTime = 2300;
break;
case "vit_l":
baseTime = 3800;
break;
case "vit_h":
baseTime = 6500;
break;
default:
baseTime = 2300;
}
long overhead = 500;
return baseTime + overhead;
}
private long estimateMemoryUsage(String modelType, int width, int height) {
long modelSize;
f switch (modelType) {
case "vit_b":
modelSize = 350L * 1024 * 1024;
break;
case "vit_l":
modelSize = 1200L * 1024 * 1024;
break;
case "vit_h":
modelSize = 2400L * 1024 * 1024;
break;
default:
modelSize = 350L * 1024 * 1024;
}
long imageMemory = width * height * 3L * 4;
long activationMemory = imageMemory * 2;
long overhead = 512L * 1024 * 1024;
return modelSize + imageMemory + activationMemory + overhead;
}
private void calculateRecommendations(ProfileResults results) {
int avgMemoryMB = results.getAverageMemoryUsageMB();
int batchSize;
if (avgMemoryMB < 2000) {
batchSize = 2;
} else if (avgMemoryMB < 4000) {
batchSize = 4;
} else {
batchSize = 8;
}
results.setRecommendedBatchSize(batchSize);
long totalImages = results.getDatasetSize();
long avgTimePerImage = results.getAverageMapTimeMs() +
results.getAverageReduceTimeMs();
int assumedParallelism = 6;
long estimatedTotalTimeMs = (totalImages * avgTimePerImage) / assumedParallelism;
results.setEstimatedTotalTimeMinutes((int)(estimatedTotalTimeMs / 60000));
}
private void printProfileSummary(ProfileResults results) {
System.out.println("\n=== Profile Summary ===");
System.out.println("Dataset size: " + results.getDatasetSize() + " images");
System.out.println("Model type: " + results.getModelType());
System.out.println("Average image size: " + results.getAverageImageSizeMB() + " MB");
System.out.println("Average dimensions: " + results.getAverageWidth() +"x" + results.getAverageHeight());
System.out.println("Average map time: " + results.getAverageMapTimeMs() + " ms");
System.out.println("Average reduce time: " + results.getAverageReduceTimeMs() + " ms");
System.out.println("Average memory: " + results.getAverageMemoryUsageMB() + " MB");
System.out.println("Recommended batch size: " + results.getRecommendedBatchSize());
System.out.println("Estimated total time: " +
results.getEstimatedTotalTimeMinutes() + " minutes");
System.out.println("=======================\n");
}
private static class ImageCharacteristics {
int avgSizeMB;
int avgWidth;
int avgHeight;
}
private static class ProcessingMetrics {
long avgMapTimeMs;
long avgReduceTimeMs;
int avgMemoryMB;
}
}
