package com.moz.qless.event;

import java.util.ArrayList;
import java.util.List;

import com.moz.qless.Events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventThread extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventThread.class);
  private final Events events;

  public EventThread(final Events events) {
    this.events = events;
  }

  @Override
  public void run() {
    EventThread.LOGGER.debug("Run loop starting");
    final List<String> channelList = new ArrayList<>();

    for (final String channel : Events.CHANNELS) {
      channelList.add("ql:" + channel);
    }

    final String[] channels = new String[channelList.size()];
    channelList.toArray(channels);

    if (null != this.events.getJedis()) {
      this.events.getJedis().subscribe(this.events.getListener(), channels);
    }

    EventThread.LOGGER.debug("Run loop ending");
    this.cleanup();
  }

  public void cleanup() {
    if (null != this.events.getJedis()) {
      this.events.getJedisPool().returnResource(this.events.getJedis());
    }
  }
}
