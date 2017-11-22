package com.moz.qless;

import java.util.ArrayList;
import java.util.List;

import com.moz.qless.client.ClientHelper;

public class IntegrationTestJob {
  public static List<String> runningHistory = new ArrayList<>();
  private static final String DELIMETER = ".";

  public static void process(final Job job) {
    final String result = job.getKlassName() + DELIMETER
        + ClientHelper.DEFAULT_JOB_METHOD;
    runningHistory.add(result);
    System.out.println(result);
  }

  public static void test(final Job job) {
    final String result = job.getKlassName() + DELIMETER + job.getQueueName();

    runningHistory.add(result);
    System.out.println(result);
  }

  public static void testA(final Job job) {
    final String result = job.getKlassName()
        + DELIMETER + job.getQueueName();

    runningHistory.add(result);
    System.out.println(result);
  }

  public static void testB(final Job job) {
    final String result = job.getKlassName() + DELIMETER + job.getQueueName();

    runningHistory.add(result);
    System.out.println(result);
  }

  public static void testC(final Job job) {
    final String result = job.getKlassName() + DELIMETER + job.getQueueName();

    runningHistory.add(result);
    System.out.println(result);
  }

  public static void testMessagelessException(final Job job) {
    throw new RuntimeException();
  }

  public static void testAlwaysFails(final Job job) {
    throw new UnsupportedOperationException("This always fails");
  }
}
