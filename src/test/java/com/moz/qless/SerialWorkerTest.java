package com.moz.qless;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.Test;

public class SerialWorkerTest extends IntegrationTest {
  private static final String DEFAULT_QUEUE_NAME = "test";
  private static final String DEFAULT_JOB_NAME = IntegrationTestJob.class.getName();

  private Thread getWorkerThread(final SerialWorker worker,
      final int intervalInSeconds) {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(intervalInSeconds * 1000);
          worker.shutDown();
        } catch (final InterruptedException v) {
          System.out.println(v);
        }
      }
    });
  }

  @Test
  public void workerMeta() throws IOException, InterruptedException {
    final Queue queue = this.client.getQueue(DEFAULT_QUEUE_NAME);
    final String jid = queue.put(jobSpec());

    final SerialWorker worker = new SerialWorker(
        Arrays.asList(DEFAULT_QUEUE_NAME),
        this.client, null, 10);
    IntegrationTestJob.RUNNING_HISTORY.clear();

    final Thread signal = this.getWorkerThread(worker, 2);

    signal.start();
    worker.run();

    assertThat(worker.getCurrentJid(),
        equalTo(jid));
    assertThat(worker.getWorkerName(),
        equalTo(this.client.workerName()));
  }

  @Test
  public void singleQueue() throws IOException, InterruptedException {
    final Queue queue = this.client.getQueue(DEFAULT_QUEUE_NAME);
    queue.put(jobSpec(DEFAULT_JOB_NAME));

    final SerialWorker worker = new SerialWorker(
        Arrays.asList(DEFAULT_QUEUE_NAME),
        this.client, null, 10);
    IntegrationTestJob.RUNNING_HISTORY.clear();

    final Thread signal = this.getWorkerThread(worker, 2);

    signal.start();
    worker.run();

    assertThat(IntegrationTestJob.RUNNING_HISTORY.get(0),
        equalTo(DEFAULT_JOB_NAME + ".test"));
  }

  @Test
  public void notExisitQueue() throws IOException, InterruptedException {
    final Queue queue = this.client.getQueue("foo");
    queue.put(jobSpec(DEFAULT_JOB_NAME));

    final SerialWorker worker = new SerialWorker(
        Arrays.asList("foo"),
        this.client, null, 10);
    IntegrationTestJob.RUNNING_HISTORY.clear();

    final Thread signal = this.getWorkerThread(worker, 2);

    signal.start();
    worker.run();

    assertThat(IntegrationTestJob.RUNNING_HISTORY.get(0),
        equalTo(DEFAULT_JOB_NAME + ".process"));
  }

  @Test
  public void multiQueuesRotation() throws IOException, InterruptedException {
    final Queue queueA = this.client.getQueue("testA");
    final Queue queueB = this.client.getQueue("testB");
    final Queue queueC = this.client.getQueue("testC");

    queueA.put(jobSpec(DEFAULT_JOB_NAME));
    queueA.put(jobSpec(DEFAULT_JOB_NAME));
    queueB.put(jobSpec(DEFAULT_JOB_NAME));
    queueC.put(jobSpec(DEFAULT_JOB_NAME));
    queueC.put(jobSpec(DEFAULT_JOB_NAME));

    final SerialWorker worker = new SerialWorker(
        Arrays.asList("testA", "testB", "testC"),
        this.client, null, 10);
    IntegrationTestJob.RUNNING_HISTORY.clear();

    final Thread signal = this.getWorkerThread(worker, 3);

    signal.start();
    worker.run();

    assertThat(IntegrationTestJob.RUNNING_HISTORY,
        contains(
            "com.moz.qless.IntegrationTestJob.testA",
            "com.moz.qless.IntegrationTestJob.testB",
            "com.moz.qless.IntegrationTestJob.testC",
            "com.moz.qless.IntegrationTestJob.testA",
            "com.moz.qless.IntegrationTestJob.testC"));
  }
}
