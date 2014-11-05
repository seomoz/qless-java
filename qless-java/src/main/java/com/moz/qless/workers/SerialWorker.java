package com.moz.qless.workers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import com.moz.qless.Client;
import com.moz.qless.Job;
import com.moz.qless.QlessException;
import com.moz.qless.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerialWorker {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SerialWorker.class);

  private final Client client;
  private final List<Queue> queues =
      new ArrayList<>();

  private String workerName;
  private String currentJid;
  private final int intervalInSeconds;

  private Iterator<Queue> queueIter;
  private boolean shutDown = false;

  public SerialWorker(
      final List<String> queueNameList,
      final Client client,
      final String workerName, final int intervalInSeconds) {

    Preconditions.checkArgument(
        (null != queueNameList) && (!queueNameList.isEmpty()),
        "empty queue list provided");
    Preconditions.checkArgument(
        null != client,
        "null client provided");
    Preconditions.checkArgument(
        intervalInSeconds > 0,
        "only positive value for intervalInSeconds allowed");

    this.client = client;
    this.intervalInSeconds = intervalInSeconds;

    if (Strings.isNullOrEmpty(workerName)) {
      this.setWorkerName(this.client.workerName());
    } else {
      this.setWorkerName(workerName
          + "-"
          + this.client.workerName());
    }

    for (final String queueName : queueNameList) {
      this.queues.add(this.client.getQueue(queueName));
    }

    if (this.queues.isEmpty()) {
      throw new QlessException("empty queue list for the worker");
    } else {
      this.queueIter = this.queues.iterator();
      LOGGER.info("initialize worker");
    }
  }

  public void shutDown() {
    this.shutDown = true;
    LOGGER.info("shutdown worker");
  }

  private Job getJob() throws IOException {
    if ((null == this.queues) || (this.queues.isEmpty())) {
      return null;
    }

    final int queueSize = this.queues.size();
    int count = 0;

    while (!this.shutDown && (count < queueSize)) {
      if (!this.queueIter.hasNext()) {
        this.queueIter = this.queues.iterator();
      }

      final Queue queue = this.queueIter.next();
      final Job job = queue.pop();
      if (null != job) {
        return job;
      }

      ++count;
    }

    return null;
  }

  public void run() throws InterruptedException, IOException {
    while (!this.shutDown) {
      final Job job = this.getJob();

      if (null == job) {
        LOGGER.info("empty job list and sleep {} seconds",
            this.intervalInSeconds);

        Thread.sleep(this.intervalInSeconds * 1000);
        continue;
      }

      this.setCurrentJid(job.getJid());

      LOGGER.info("working on {} : {}",
          job.getKlassName(),
          this.getCurrentJid());

      job.process();
    }
  }

  public String getCurrentJid() {
    return this.currentJid;
  }

  public void setCurrentJid(final String currentJid) {
    this.currentJid = currentJid;
  }

  public String getWorkerName() {
    return this.workerName;
  }

  private void setWorkerName(final String workerName) {
    this.workerName = workerName;
  }
}
