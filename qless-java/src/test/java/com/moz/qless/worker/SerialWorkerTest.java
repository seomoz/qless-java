package com.moz.qless.worker;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.moz.qless.Client;
import com.moz.qless.client.ClientHelper;
import com.moz.qless.ClientCreation;
import com.moz.qless.IntegrationTestJob;
import com.moz.qless.Queue;
import com.moz.qless.workers.SerialWorker;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

public class SerialWorkerTest {
  private static final String DEFAULT_QUEUE_NAME = "test";
  private static final String DEFAULT_JOB_NAME = "com.moz.qless.IntegrationTestJob";
  private final JedisPool jedisPool = new JedisPool(ClientHelper.DEFAULT_HOSTNAME);
  private Client client;

  @Before
  public void before() throws IOException {
    this.client = ClientCreation.create(this.jedisPool);
  }

  @Test
  public void workerMeta() throws IOException, InterruptedException {
    final Queue queue = this.client.getQueues().get(SerialWorkerTest.DEFAULT_QUEUE_NAME);
    final String jid = queue.put(SerialWorkerTest.DEFAULT_JOB_NAME, null, null);

    final SerialWorker worker = new SerialWorker(
        Arrays.asList(new String[] {SerialWorkerTest.DEFAULT_QUEUE_NAME}),
        this.client, null, 10);
    IntegrationTestJob.runningHistory.clear();

    final Thread signal = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(2000);
          worker.shutDown();
        } catch (final InterruptedException v) {
          System.out.println(v);
        }
      }
    };

    signal.start();
    worker.run();

    assertThat(worker.getCurrentJid(),
        equalTo(jid));
    assertThat(worker.getWorkerName(),
        equalTo(this.client.workerName()));
  }

  @Test
  public void singleQueue() throws IOException, InterruptedException {
    final Queue queue = this.client.getQueues().get(SerialWorkerTest.DEFAULT_QUEUE_NAME);
    queue.put(SerialWorkerTest.DEFAULT_JOB_NAME, null, null);

    final SerialWorker worker = new SerialWorker(
        Arrays.asList(new String[] {SerialWorkerTest.DEFAULT_QUEUE_NAME}),
        this.client, null, 10);
    IntegrationTestJob.runningHistory.clear();

    final Thread signal = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(2000);
          worker.shutDown();
        } catch (final InterruptedException v) {
          System.out.println(v);
        }
      }
    };

    signal.start();
    worker.run();

    assertThat(IntegrationTestJob.runningHistory.get(0),
        equalTo(SerialWorkerTest.DEFAULT_JOB_NAME + ".test"));
  }

  @Test
  public void notExisitQueue() throws IOException, InterruptedException {
    final Queue queue = this.client.getQueues().get("foo");
    queue.put(SerialWorkerTest.DEFAULT_JOB_NAME, null, null);

    final SerialWorker worker = new SerialWorker(
        Arrays.asList(new String[] {"foo"}),
        this.client, null, 10);
    IntegrationTestJob.runningHistory.clear();

    final Thread signal = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(2000);
          worker.shutDown();
        } catch (final InterruptedException v) {
          System.out.println(v);
        }
      }
    };

    signal.start();
    worker.run();

    assertThat(IntegrationTestJob.runningHistory.get(0),
        equalTo(SerialWorkerTest.DEFAULT_JOB_NAME + ".process"));
  }

  @Test
  public void multiQueuesRotation() throws IOException, InterruptedException {
    final Queue queueA = this.client.getQueues().get("testA");
    final Queue queueB = this.client.getQueues().get("testB");
    final Queue queueC = this.client.getQueues().get("testC");

    queueA.put(SerialWorkerTest.DEFAULT_JOB_NAME, null, null);
    queueA.put(SerialWorkerTest.DEFAULT_JOB_NAME, null, null);
    queueB.put(SerialWorkerTest.DEFAULT_JOB_NAME, null, null);
    queueC.put(SerialWorkerTest.DEFAULT_JOB_NAME, null, null);
    queueC.put(SerialWorkerTest.DEFAULT_JOB_NAME, null, null);

    final SerialWorker worker = new SerialWorker(
        Arrays.asList(new String[] {"testA", "testB", "testC"}),
        this.client, null, 10);
    IntegrationTestJob.runningHistory.clear();

    final Thread signal = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(5000);
          worker.shutDown();
        } catch (final InterruptedException v) {
          System.out.println(v);
        }
      }
    };

    signal.start();
    worker.run();

    assertThat(IntegrationTestJob.runningHistory,
        contains(
            "com.moz.qless.IntegrationTestJob.testA",
            "com.moz.qless.IntegrationTestJob.testB",
            "com.moz.qless.IntegrationTestJob.testC",
            "com.moz.qless.IntegrationTestJob.testA",
            "com.moz.qless.IntegrationTestJob.testC"));
  }
}
