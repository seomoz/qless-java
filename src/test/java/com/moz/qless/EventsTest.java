package com.moz.qless;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.qless.event.QlessEventListener;
import com.moz.qless.lua.LuaConfigParameter;

public class EventsTest extends IntegrationTest {
  private static final long SLEEP_MS = 500;
  private static final Logger LOGGER = LoggerFactory.getLogger(EventsTest.class);
  private Job untracked, tracked;

  @Before
  public void before() throws IOException {
    this.untracked = this.client
      .getJobs()
      .get(this.queue.put(jobSpec("untracked")));

    this.tracked = this.client
      .getJobs()
      .get(this.queue.put(jobSpec("untracked")));

    this.tracked.track();
  }

  @Test
  public void offList() throws Exception {
    final EventCapture eventCapture = new EventCapture();

    this.client
      .getEvents()
      .on("none")
      .fire(eventCapture);

    Thread.sleep(SLEEP_MS);

    assertThat(eventCapture.jidsForEvent("none"),
      is(empty()));
  }

  @Test
  public void canceled() throws Exception {
    final EventCapture eventCapture = new EventCapture();

    this.client
      .getEvents()
      .on(JobStatus.CANCELED.toString())
      .fire(eventCapture);

    this.tracked.cancel();
    this.untracked.cancel();

    Thread.sleep(SLEEP_MS);

    assertThat(eventCapture.jidsForEvent(JobStatus.CANCELED.toString()),
      hasSize(1));
    assertThat(eventCapture.jidsForEvent(JobStatus.CANCELED.toString()),
      contains(this.tracked.getJid()));
  }

  @Test
  public void completion() throws Exception {
    final EventCapture eventCapture = new EventCapture();

    this.client
      .getEvents()
      .on(JobStatus.COMPLETED.toString())
      .fire(eventCapture);

    for (final Job job : this.queue.pop(10)) {
      job.complete();
    }

    Thread.sleep(SLEEP_MS);

    assertThat(eventCapture.jidsForEvent(JobStatus.COMPLETED.toString()),
      hasSize(1));
    assertThat(eventCapture.jidsForEvent(JobStatus.COMPLETED.toString()),
      contains(this.tracked.getJid()));
  }

  @Test
  public void failed() throws Exception {
    final EventCapture eventCapture = new EventCapture();

    this.client
      .getEvents()
      .on(JobStatus.FAILED.toString())
      .fire(eventCapture);

    for (final Job job : this.queue.pop(10)) {
      job.fail("foo", "bar");
    }

    Thread.sleep(SLEEP_MS);

    assertThat(eventCapture.jidsForEvent(JobStatus.FAILED.toString()),
      hasSize(1));
    assertThat(eventCapture.jidsForEvent(JobStatus.FAILED.toString()),
      contains(this.tracked.getJid()));
  }

  @Test
  public void pop() throws Exception {
    final EventCapture eventCapture = new EventCapture();

    this.client
      .getEvents()
      .on(JobStatus.POPPED.toString())
      .fire(eventCapture);

    this.queue.pop(10);

    Thread.sleep(SLEEP_MS);

    assertThat(eventCapture.jidsForEvent(JobStatus.POPPED.toString()),
      hasSize(1));
    assertThat(eventCapture.jidsForEvent(JobStatus.POPPED.toString()),
      contains(this.tracked.getJid()));
  }

  @Test
  public void put() throws Exception {
    final EventCapture eventCapture = new EventCapture();

    this.client
      .getEvents()
      .on(JobStatus.PUT.toString())
      .fire(eventCapture);

    this.tracked.requeue("other");
    this.untracked.requeue("other");

    Thread.sleep(SLEEP_MS);

    assertThat(eventCapture.jidsForEvent(JobStatus.PUT.toString()),
      hasSize(1));
    assertThat(eventCapture.jidsForEvent(JobStatus.PUT.toString()),
      contains(this.tracked.getJid()));
  }

  @Test
  public void stalled() throws Exception {
    this.client
      .getConfig()
      .put(LuaConfigParameter.GRACE_PERIOD.toString(), 0);
    this.client
      .getConfig()
      .put(LuaConfigParameter.HEARTBEAT.toString(), 0);

    final EventCapture eventCapture = new EventCapture();

    this.client
      .getEvents()
      .on(JobStatus.STALLED.toString())
      .fire(eventCapture);

    this.queue.pop(2);
    this.queue.pop(2);

    Thread.sleep(SLEEP_MS);

    assertThat(eventCapture.jidsForEvent(JobStatus.STALLED.toString()),
      hasSize(1));
    assertThat(eventCapture.jidsForEvent(JobStatus.STALLED.toString()),
      contains(this.tracked.getJid()));
  }


  private class EventCapture implements QlessEventListener {
    Multimap<String, Object> eventsByChannel = ArrayListMultimap.create();

    @Override
    public void fire(final String channel, final Object event) {
      LOGGER.debug("fire {}", event);
      this.eventsByChannel.put(channel, event);
    }

    public List<String> jidsForEvent(final String channel) {
      final Collection<Object> objects =
        this.eventsByChannel.get(channel);
      final List<String> results =
        new ArrayList<>();

      for (final Object object : objects) {
        results.add((String) object);
      }

      return results;
    }
  }

}
