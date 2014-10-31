package com.moz.qless.event;

import java.io.IOException;

public interface QlessEventListener {
  void fire(String channel, Object event) throws IOException;
}
