package com.unsam.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import java.util.*;
public class ConfigurationOptimizer {
private ProfileResults profileResults;
private ClusterTopology clusterTopology;
private static final Map<String, Integer> MODEL_SIZES = new HashMap<String, Integer>()
{{
put("vit_b", 350);
put("vit_l", 1200);
put("vit_h", 2400);
}};
private static final int MEMORY_OVERHEAD = 2048;
private static final int OS_RESERVED_MEMORY = 4096;
private static final float MEMORY_SAFETY_MARGIN = 0.9f;
public ConfigurationOptimizer(ProfileResults profile) {
this.profileResults = profile;
this.clusterTopology = new ClusterTopology();
}
public Configuration optimizeConfiguration(Configuration conf) throws Exception {
System.out.println("\n=== Starting Configuration Optimization ===");
clusterTopology.queryCluster(conf);
System.out.println("Detected cluster topology:");
clusterTopology.printTopology();
MemoryAllocation memAlloc = calculateMemoryAllocation();
ParallelismConfig parallelism = calculateParallelism(memAlloc);
IOConfig ioConfig = calculateIOConfig(parallelism);
applyOptimizations(conf, memAlloc, parallelism, ioConfig);
System.out.println("=== Configuration Optimization Complete ===\n");
return conf;
}
private MemoryAllocation calculateMemoryAllocation() {
MemoryAllocation alloc = new MemoryAllocation();
String modelType = profileResults.getModelType();
int modelSize = MODEL_SIZES.getOrDefault(modelType,2048);
int avgImageSize = profileResults.getAverageImageSizeMB();
alloc.mapMemoryMB = Math.max(avgImageSize * 3, 1024);
int batchSize = profileResults.getRecommendedBatchSize();
int calculatedReduceMem = modelSize + (batchSize * avgImageSize * 3) +
MEMORY_OVERHEAD;
// 2. FORCE a minimum of 4096 for SAM models
alloc.reduceMemoryMB = Math.max(calculatedReduceMem, 4096);
// 3. Update Heaps (0.8 is fine, but 0.75 is safer for Python overhead)
alloc.mapHeapMB = (int)(alloc.mapMemoryMB * 0.8);
alloc.reduceHeapMB = (int)(alloc.reduceMemoryMB * 0.75);
System.out.println("Memory Allocation:");
System.out.println(" Map Container: " + alloc.mapMemoryMB + " MB");
System.out.println(" Map Heap: " + alloc.mapHeapMB + " MB");
System.out.println(" Reduce Container: " + alloc.reduceMemoryMB + " MB");
System.out.println(" Reduce Heap: " + alloc.reduceHeapMB + " MB");
return alloc;
}
private ParallelismConfig calculateParallelism(MemoryAllocation memAlloc) {
ParallelismConfig config = new ParallelismConfig();
List<NodeInfo> nodes = clusterTopology.getDataNodes();
Map<String, Integer> mapperCapacity = new HashMap<>();
Map<String, Integer> reducerCapacity = new HashMap<>();
int totalMapperSlots = 0;
int totalReducerSlots = 0;
for (NodeInfo node : nodes) {
int availableMemory = (int)((node.memoryMB - OS_RESERVED_MEMORY) *
MEMORY_SAFETY_MARGIN);
int vcores = node.vcores;
int maxMappers = Math.min(
availableMemory / memAlloc.mapMemoryMB,
vcores
);
maxMappers = Math.max(1, maxMappers);
int maxReducers = Math.min(
availableMemory / memAlloc.reduceMemoryMB,
vcores
);
maxReducers = Math.max(1, maxReducers);
mapperCapacity.put(node.hostname, maxMappers);
reducerCapacity.put(node.hostname, maxReducers);
totalMapperSlots += maxMappers;
totalReducerSlots += maxReducers;
System.out.println(String.format(
"Node %s (%d MB): %d mappers, %d reducers",
node.hostname, node.memoryMB, maxMappers, maxReducers
));
}
config.mapperDistribution = applyHeterogeneityWeights(mapperCapacity);
config.reducerDistribution = applyHeterogeneityWeights(reducerCapacity);
config.totalMappers = totalMapperSlots;
config.totalReducers = Math.min(totalReducerSlots, profileResults.getDatasetSize());
System.out.println("Parallelism Configuration:");
System.out.println(" Total Mappers: " + config.totalMappers);
System.out.println(" Total Reducers: " + config.totalReducers);
return config;
}
private Map<String, Float> applyHeterogeneityWeights(Map<String, Integer> capacity) {
Map<String, Float> weights = new HashMap<>();
List<Integer> capacities = new ArrayList<>(capacity.values());
Collections.sort(capacities);
int median = capacities.get(capacities.size() / 2);
for (Map.Entry<String, Integer> entry : capacity.entrySet()) {
String node = entry.getKey();
int cap = entry.getValue();
float weight = cap > median ? (float)cap / median : 1.0f;
weights.put(node, weight);
}
return weights;
}
private IOConfig calculateIOConfig(ParallelismConfig parallelism) {
IOConfig config = new IOConfig();
// FIXED: Ensure ioSortMB is at least 100 MB (minimum safe value)
int avgImageSize = profileResults.getAverageImageSizeMB();
int calculatedSortMB = (int)(Math.max(avgImageSize * 0.4, 100));
config.ioSortMB = Math.min(calculatedSortMB, 512);
config.ioSortFactor = Math.max(10, Math.min(100, parallelism.totalReducers));
config.shuffleParallelCopies = Math.min(20, Math.max(1, parallelism.totalReducers / 2));
System.out.println("I/O Configuration:");
System.out.println(" Sort Buffer: " + config.ioSortMB + " MB");
System.out.println(" Sort Factor: " + config.ioSortFactor);
System.out.println(" Shuffle Parallel Copies: " + config.shuffleParallelCopies);
return config;
}
private void applyOptimizations(
Configuration conf,
MemoryAllocation memAlloc,
ParallelismConfig parallelism,
IOConfig ioConfig) {
conf.setInt("mapreduce.map.memory.mb", 4096);
conf.setInt("mapreduce.reduce.memory.mb", memAlloc.reduceMemoryMB);
conf.set("mapreduce.map.java.opts",
String.format("-Xmx%dm -XX:+UseG1GC -XX:MaxGCPauseMillis=200",
memAlloc.mapHeapMB));
conf.set("mapreduce.reduce.java.opts",
String.format("-Xmx%dm -XX:+UseG1GC -XX:MaxGCPauseMillis=200",3072));
conf.setInt("mapreduce.job.maps", parallelism.totalMappers);
conf.setInt("mapreduce.job.reduces", parallelism.totalReducers);
conf.setInt("unsam.optimal.reducers", parallelism.totalReducers);
// CRITICAL FIX: Set io.sort.mb to a safe value (minimum 100 MB)
conf.setInt("mapreduce.task.io.sort.mb", ioConfig.ioSortMB);
conf.setInt("mapreduce.task.io.sort.factor", ioConfig.ioSortFactor);
conf.setInt("mapreduce.reduce.shuffle.parallelcopies", ioConfig.shuffleParallelCopies);
conf.setBoolean("mapreduce.map.output.compress", true);
conf.set("mapreduce.map.output.compress.codec",
"org.apache.hadoop.io.compress.SnappyCodec");
conf.setBoolean("mapreduce.map.speculative", true);
conf.setBoolean("mapreduce.reduce.speculative", true);
conf.setFloat("mapreduce.job.speculative.slowtaskthreshold", 0.75f);
conf.setInt("mapreduce.job.jvm.numtasks", -1);
conf.setLong("mapreduce.task.timeout", 1800000);
conf.setFloat("mapreduce.reduce.shuffle.input.buffer.percent", 0.7f);
conf.setFloat("mapreduce.reduce.shuffle.merge.percent", 0.66f);
}
private static class MemoryAllocation {
int mapMemoryMB;
int reduceMemoryMB;
int mapHeapMB;
int reduceHeapMB;
}
private static class ParallelismConfig {
int totalMappers;
int totalReducers;
Map<String, Float> mapperDistribution;
Map<String, Float> reducerDistribution;
}
private static class IOConfig {
int ioSortMB;
int ioSortFactor;
int shuffleParallelCopies;
}
}
class ClusterTopology {
private List<NodeInfo> dataNodes;
public ClusterTopology() {
this.dataNodes = new ArrayList<>();
}
public void queryCluster(Configuration conf) throws Exception {
YarnClient yarnClient = YarnClient.createYarnClient();
yarnClient.init(conf);
yarnClient.start();
try {
List<NodeReport> nodeReports = yarnClient.getNodeReports();
for (NodeReport report : nodeReports) {
NodeInfo node = new NodeInfo();
node.hostname = report.getNodeId().getHost();
node.memoryMB = report.getCapability().getMemory();
node.vcores = report.getCapability().getVirtualCores();
node.state = report.getNodeState().toString();
if ("RUNNING".equals(node.state)) {
dataNodes.add(node);
}
}
} finally {
yarnClient.stop();
}
}
public List<NodeInfo> getDataNodes() {
return dataNodes;
}
public void printTopology() {
System.out.println("Cluster Nodes:");
for (NodeInfo node : dataNodes) {
System.out.println(String.format(
" %s: %d MB, %d vcores, %s",
node.hostname, node.memoryMB, node.vcores, node.state
));
}
}
}
class NodeInfo {
String hostname;
int memoryMB;
int vcores;
String state;
}
