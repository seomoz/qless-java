package com.moz.qless;

import java.util.ArrayList;
import java.util.List;

import com.moz.qless.client.ClientHelper;

public class IntegrationTestJob {
  public static List<String> runningHistory = new ArrayList<>();
  private static final String DELIMETER = ".";

  public static void process(final Job job) {
    final String result = job.getKlassName() + IntegrationTestJob.DELIMETER
        + ClientHelper.DEFAULT_JOB_METHOD;
    IntegrationTestJob.runningHistory.add(result);
    System.out.println(result);
  }

  public static void test(final Job job) {
    final String result = job.getKlassName() + IntegrationTestJob.DELIMETER
        + job.getQueueName();

    IntegrationTestJob.runningHistory.add(result);
    System.out.println(result);
  }

  public static void testA(final Job job) {
    final String result = job.getKlassName()
        + IntegrationTestJob.DELIMETER + job.getQueueName();

    IntegrationTestJob.runningHistory.add(result);
    System.out.println(result);
  }

  public static void testB(final Job job) {
    final String result = job.getKlassName()
        + IntegrationTestJob.DELIMETER + job.getQueueName();

    IntegrationTestJob.runningHistory.add(result);
    System.out.println(result);
  }

  public static void testC(final Job job) {
    final String result = job.getKlassName()
        + IntegrationTestJob.DELIMETER + job.getQueueName();

    IntegrationTestJob.runningHistory.add(result);
    System.out.println(result);
  }

  public static void testMessagelessException(final Job job) {
    throw new RuntimeException();
  }
}
