package com.unsam.mapreduce;
public class TestArgs {
public static void main(String[] args) {
System.out.println("=== Simple Test Args ===");
System.out.println("Number of arguments: " + args.length);
for (int i = 0; i < args.length; i++) {
System.out.println("arg[" + i + "] = '" + args[i] + "'");
}
}
}
