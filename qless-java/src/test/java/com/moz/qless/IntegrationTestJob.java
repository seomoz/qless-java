package com.moz.qless;

import java.util.ArrayList;
import java.util.List;

public class IntegrationTestJob {
  public static List<String> runningHistory = new ArrayList<String>();
  private static final String DELIMETER = ".";

  public static String test(final Job job) {
    final String result = job.getKlassName() + IntegrationTestJob.DELIMETER
        + job.getQueueName();
    IntegrationTestJob.runningHistory.add(result);

    return result;
  }

  public static void testA(final Job job) {
    IntegrationTestJob.runningHistory.add(job.getKlassName()
        + IntegrationTestJob.DELIMETER + job.getQueueName());
    System.out.println(job.getKlassName() + IntegrationTestJob.DELIMETER
        + job.getQueueName());
  }

  public static void testB(final Job job) {
    IntegrationTestJob.runningHistory.add(job.getKlassName()
        + IntegrationTestJob.DELIMETER + job.getQueueName());
    System.out.println(job.getKlassName() + IntegrationTestJob.DELIMETER
        + job.getQueueName());
  }

  public static void testC(final Job job) {
    IntegrationTestJob.runningHistory.add(job.getKlassName() + "." + job.getQueueName());
    System.out.println(job.getKlassName() + IntegrationTestJob.DELIMETER
        + job.getQueueName());
  }
}
