package com.moz.qless;

import java.io.IOException;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import com.moz.qless.event.EventListener;
import com.moz.qless.event.EventThread;
import com.moz.qless.event.QlessEventListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


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
    "untrack" };

  private final JedisPool jedisPool;
  public JedisPool getJedisPool() {
    return this.jedisPool;
  }

  private final Jedis jedis;

  public Jedis getJedis() {
    return this.jedis;
  }

  private final EventListener listener = new EventListener(this);
  public EventListener getListener() {
    return this.listener;
  }

  private final EventThread listenerThread;

  private final Multimap<String, QlessEventListener> listenersByChannel =
      ArrayListMultimap.create();

  public Multimap<String, QlessEventListener> getListenersByChannel() {
    return this.listenersByChannel;
  }

  Events(final JedisPool jedisPool) {
    this.jedisPool = jedisPool;
    this.jedis = jedisPool.getResource();

    this.listenerThread = new EventThread(this);
    this.listenerThread.setDaemon(true);
    this.listenerThread.start();
  }

  @Override
  public void close() throws IOException {
    this.listenerThread.cleanup();
  }

  public Builder on(final String... events) {
    return new Builder(events);
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
