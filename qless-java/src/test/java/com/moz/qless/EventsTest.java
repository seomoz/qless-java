package com.moz.qless;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import com.moz.qless.client.ClientHelper;
import com.moz.qless.event.QlessEventListener;
import com.moz.qless.lua.LuaConfigParameter;
import com.moz.qless.lua.LuaJobStatus;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPool;

public class EventsTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventsTest.class);

  private final JedisPool jedisPool = new JedisPool(ClientHelper.DEFAULT_HOSTNAME);
  private Client client;
  private Queue queue;
  private static final String DEFAULT_NAME = "foo";
  private Job untracked, tracked;

  @Before
  public void before() throws IOException {
    this.client = ClientCreation.create(this.jedisPool);
    this.queue = new Queue(this.client, EventsTest.DEFAULT_NAME);

    this.untracked = this.client
        .getJobs()
        .get(this.queue
            .newJobPutter()
            .build()
            .put("untracked"));

    this.tracked = this.client
        .getJobs()
        .get(this.queue
            .newJobPutter()
            .build()
            .put("untracked"));

    this.tracked.track();
  }

  @Test
  public void offList() throws Exception {
      final EventCapture eventCapture = new EventCapture();

      this.client
        .getEvents()
        .on("none")
        .fire(eventCapture);

      Thread.sleep(100);

      assertThat(eventCapture.jidsForEvent("none"),
          is(empty()));
  }

  @Test
  public void canceled() throws Exception {
      final EventCapture eventCapture = new EventCapture();

      this.client
        .getEvents()
        .on(LuaJobStatus.CANCELED.toString())
        .fire(eventCapture);

      this.tracked.cancel();
      this.untracked.cancel();

      Thread.sleep(100);

      assertThat(eventCapture.jidsForEvent(LuaJobStatus.CANCELED.toString()),
          hasSize(1));
      assertThat(eventCapture.jidsForEvent(LuaJobStatus.CANCELED.toString()),
          contains(this.tracked.getJid()));
  }

  @Test
  public void completion() throws Exception {
      final EventCapture eventCapture = new EventCapture();

      this.client
        .getEvents()
        .on(LuaJobStatus.COMPLETED.toString())
        .fire(eventCapture);

      for (final Job job : this.queue.pop(10)) {
          job.complete();
      }

      Thread.sleep(100);

      assertThat(eventCapture.jidsForEvent(LuaJobStatus.COMPLETED.toString()),
          hasSize(1));
      assertThat(eventCapture.jidsForEvent(LuaJobStatus.COMPLETED.toString()),
          contains(this.tracked.getJid()));
  }

  @Test
  public void failed() throws Exception {
      final EventCapture eventCapture = new EventCapture();

      this.client
        .getEvents()
        .on(LuaJobStatus.FAILED.toString())
        .fire(eventCapture);

      for (final Job job : this.queue.pop(10)) {
          job.fail("foo", "bar");
      }

      Thread.sleep(100);

      assertThat(eventCapture.jidsForEvent(LuaJobStatus.FAILED.toString()),
          hasSize(1));
      assertThat(eventCapture.jidsForEvent(LuaJobStatus.FAILED.toString()),
          contains(this.tracked.getJid()));
  }

  @Test
  public void pop() throws Exception {
      final EventCapture eventCapture = new EventCapture();

      this.client
        .getEvents()
        .on(LuaJobStatus.POPPED.toString())
        .fire(eventCapture);

      this.queue.pop(10);

      Thread.sleep(100);

      assertThat(eventCapture.jidsForEvent(LuaJobStatus.POPPED.toString()),
          hasSize(1));
      assertThat(eventCapture.jidsForEvent(LuaJobStatus.POPPED.toString()),
          contains(this.tracked.getJid()));
  }

  @Test
  public void put() throws Exception {
      final EventCapture eventCapture = new EventCapture();

      this.client
        .getEvents()
        .on(LuaJobStatus.PUT.toString())
        .fire(eventCapture);

      this.tracked.requeue("other");
      this.untracked.requeue("other");

      Thread.sleep(100);

      assertThat(eventCapture.jidsForEvent(LuaJobStatus.PUT.toString()),
          hasSize(1));
      assertThat(eventCapture.jidsForEvent(LuaJobStatus.PUT.toString()),
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
        .on(LuaJobStatus.STALLED.toString())
        .fire(eventCapture);

      this.queue.pop(2);
      this.queue.pop(2);

      Thread.sleep(100);

      assertThat(eventCapture.jidsForEvent(LuaJobStatus.STALLED.toString()),
          hasSize(1));
      assertThat(eventCapture.jidsForEvent(LuaJobStatus.STALLED.toString()),
          contains(this.tracked.getJid()));
  }


  private class EventCapture implements QlessEventListener {
      Multimap<String, Object> eventsByChannel = ArrayListMultimap.create();

      @Override
      public void fire(final String channel, final Object event) {
          com.moz.qless.EventsTest.LOGGER.debug("fire {}", event);
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
  };

}
