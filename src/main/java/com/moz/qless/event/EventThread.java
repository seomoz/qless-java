package com.moz.qless.event;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.qless.Events;

public class EventThread extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventThread.class);
  private final Events events;

  public EventThread(final Events events) {
    this.events = events;
  }

  @Override
  public void run() {
    LOGGER.debug("Run loop starting");
    final List<String> channelList = new ArrayList<>();

    for (final String channel : Events.CHANNELS) {
      channelList.add("ql:" + channel);
    }

    final String[] channels = new String[channelList.size()];
    channelList.toArray(channels);

    this.events.subscribe(channels);

    LOGGER.debug("Run loop ending");
  }
}
