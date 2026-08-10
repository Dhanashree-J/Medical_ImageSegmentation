package com.unsam.mapreduce;
public class ProfileResults {
private String modelType;
private int datasetSize;
private int averageImageSizeMB;
private int averageWidth;
private int averageHeight;
private long averageMapTimeMs;
private long averageReduceTimeMs;
private int averageMemoryUsageMB;
private int recommendedBatchSize;
private int estimatedTotalTimeMinutes;
public String getModelType() { return modelType; }
public void setModelType(String type) { this.modelType = type; }
public int getDatasetSize() { return datasetSize; }
public void setDatasetSize(int size) { this.datasetSize = size; }
public int getAverageImageSizeMB() { return averageImageSizeMB; }
public void setAverageImageSizeMB(int size) { this.averageImageSizeMB = size; }
public int getAverageWidth() { return averageWidth; }
public void setAverageWidth(int width) { this.averageWidth = width; }
public int getAverageHeight() { return averageHeight; }
public void setAverageHeight(int height) { this.averageHeight = height; }
public long getAverageMapTimeMs() { return averageMapTimeMs; }
public void setAverageMapTimeMs(long time) { this.averageMapTimeMs = time; }
public long getAverageReduceTimeMs() { return averageReduceTimeMs; }
public void setAverageReduceTimeMs(long time) { this.averageReduceTimeMs = time; }
public int getAverageMemoryUsageMB() { return averageMemoryUsageMB; }
public void setAverageMemoryUsageMB(int memory) { this.averageMemoryUsageMB =
memory; }
public int getRecommendedBatchSize() { return recommendedBatchSize; }
public void setRecommendedBatchSize(int size) { this.recommendedBatchSize = size; }
public int getEstimatedTotalTimeMinutes() { return estimatedTotalTimeMinutes; }
public void setEstimatedTotalTimeMinutes(int time) { this.estimatedTotalTimeMinutes =
time; }
}
