package com.moz.qless;

import java.io.IOException;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.moz.qless.event.EventListener;
import com.moz.qless.event.EventThread;
import com.moz.qless.event.QlessEventListener;


public class Events implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Events.class);
  public static final String[] CHANNELS = {
    "canceled",
    "completed",
    "failed",
    "popped",
    "put",
    "stalled",
    "track",
    "untrack"
  };

  private final JedisPool jedisPool;
  private final Jedis jedis;
  private final EventListener listener = new EventListener(this);
  private final EventThread listenerThread;
  private final Multimap<String, QlessEventListener> listenersByChannel =
      ArrayListMultimap.create();

  Events(final JedisPool jedisPool) {
    this.jedisPool = jedisPool;
    this.jedis = jedisPool.getResource();

    this.listenerThread = new EventThread(this);
    this.listenerThread.start();
  }

  @Override
  public void close() throws IOException {
    this.listener.unsubscribe();
    try {
      this.listenerThread.join();
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
    }
    jedis.close();
  }

  public void subscribe(final String... channels) {
    this.jedis.subscribe(this.listener, channels);
  }

  public Builder on(final String... events) {
    return new Builder(events);
  }

  public Multimap<String, QlessEventListener> getListenersByChannel() {
    return this.listenersByChannel;
  }

  public class Builder {
    private final String[] channels;

    Builder(final String... channels) {
      this.channels = channels;
    }

    public void fire(final QlessEventListener listener) {
      for (final String channel : this.channels) {
        Events.this.listenersByChannel.put(channel, listener);
      }
    }
  }

}
