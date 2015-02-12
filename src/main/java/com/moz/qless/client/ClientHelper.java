package com.moz.qless.client;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class ClientHelper {
  public static final String EMPTY_RESULT = "{}";

  public static final String DEFAULT_HOSTNAME = "localhost";
  public static final String DEFAULT_APPLICATION = "qless";
  public static final Integer DEFAULT_BACKLOG = 0;
  public static final Integer DEFAULT_DELAY = 0;
  public static final Integer DEFAULT_OFFSET = 0;
  public static final Integer DEFAULT_PRIORITY = 0;
  public static final Integer DEFAULT_RETRIES = 5;
  public static final Integer DEFAULT_INTERVAL = 60;
  public static final Integer DEFAULT_HEARTBEAT = 60;
  public static final Integer DEFAULT_GRACE_PERIOD = 10;
  public static final Integer DEFAULT_JOBS_HISTORY = 604800;
  public static final Integer DEFAULT_STATS_HISTORY = 30;
  public static final Integer DEFAULT_HISTOGRAM_HISTORY = 7;
  public static final Integer DEFAULT_JOBS_HISTORY_COUNT = 50000;

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
