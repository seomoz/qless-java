package com.moz.qless.event;

import java.io.IOException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPubSub;

import com.moz.qless.Events;

public class EventListener extends JedisPubSub {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventListener.class);
  private final Events events;

  public EventListener(final Events events) {
    this.events = events;
  }

  @Override
  public void onMessage(final String channel, final String message) {
    LOGGER.debug("onMessage channel={} message={}",
        channel, message);

    final String modifiedChannel = channel.substring(3);
    final Collection<QlessEventListener> listeners =
        this.events.getListenersByChannel().get(modifiedChannel);

    for (final QlessEventListener listener : listeners) {
      try {
        listener.fire(modifiedChannel, message);
      } catch (final IOException e) {
        LOGGER.error("Error handling qless event {}",
            message, e);
      }
    }
  }

  @Override
  public void onPMessage(final String pattern, final String channel,
      final String message) {
    LOGGER.debug("onPMessage pattern={} channel={} message={}",
        channel, message);
  }

  @Override
  public void onSubscribe(final String channel, final int subscribedChannels) {
    LOGGER.debug("onSubscribe channel={} subscribedChannels={}",
        channel, subscribedChannels);
  }

  @Override
  public void onUnsubscribe(final String channel, final int subscribedChannels) {
    LOGGER.debug("onUnsubscribe channel={} subscribedChannels={}",
        channel, subscribedChannels);
  }

  @Override
  public void onPUnsubscribe(final String pattern, final int subscribedChannels) {
    LOGGER.debug("onPUnsubscribe pattern={} subscribedChannels={}",
        pattern, subscribedChannels);
  }

  @Override
  public void onPSubscribe(final String pattern, final int subscribedChannels) {
    LOGGER.debug("onPSubscribe pattern={} subscribedChannels={}",
        pattern, subscribedChannels);
  }
}

