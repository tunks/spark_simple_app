/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.att.simple;

/**
 *
 * @author ebrimatunkara
 */
/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

public class SimpleApp {
  static final ClassLoader loader = SimpleApp.class.getClassLoader();
  public static void main(String[] args) {
    String logFile = "sample.txt"; 
    SparkConf conf = new SparkConf()
                         .setMaster("local")
                          .setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter((String s) -> s.contains("a")).count();

    long numBs = logData.filter((String s) -> s.contains("b")).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
  }
}
