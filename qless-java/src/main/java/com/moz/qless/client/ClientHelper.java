package com.moz.qless.client;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class ClientHelper {
  public static final String EMPTY_RESULT = "{}";
  public static final String ZERO_VALUE = "0";

  public static final String DEFAULT_BACKLOG = ClientHelper.ZERO_VALUE;
  public static final String DEFAULT_DELAY = ClientHelper.ZERO_VALUE;
  public static final String DEFAULT_HOSTNAME = "localhost";
  public static final String DEFAULT_OFFSET = ClientHelper.ZERO_VALUE;
  public static final String DEFAULT_PRIORITY = ClientHelper.ZERO_VALUE;
  public static final String DEFAULT_RETRIES = "5";
  public static final int DEFAULT_HEARTBEAT = 60;
  public static final String DEFAULT_APPLICATION = "qless";
  public static final int DEFAULT_GRACE_PERIOD = 10;
  public static final int DEFAULT_JOBS_HISTORY = 604800;
  public static final int DEFAULT_STATS_HISTORY = 30;
  public static final int DEFAULT_HISTOGRAM_HISTORY = 7;
  public static final int DEFAULT_JOBS_HISTORY_COUNT = 50000;

  public static final String DEFAULT_JOB_METHOD = "process";

  public static String generateJid() {
    return UUID.randomUUID().toString().replaceAll("-", "");
  }

  public static String getCurrentSeconds() {
    return String.valueOf(System.currentTimeMillis() / 1000);
  }

  public static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (final UnknownHostException e) {
      return ClientHelper.DEFAULT_HOSTNAME;
    }
  }

  public static String getPid() {
    final String name = ManagementFactory.getRuntimeMXBean().getName();
    return name.split("@")[0];
  }
}
