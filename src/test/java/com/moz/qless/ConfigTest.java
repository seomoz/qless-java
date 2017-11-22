package com.moz.qless;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.hamcrest.Matchers;
import org.junit.Test;

import com.moz.qless.client.ClientHelper;
import com.moz.qless.lua.LuaConfigParameter;

public class ConfigTest extends IntegrationTest {
  private final String testKey = "foo";
  private final String testKeyValue = "1";

  @Test
  public void getAll() throws IOException {
    final Map<String, Object> config = this.client.getConfig().getMap();

    assertThat((int) config.get(LuaConfigParameter.HEARTBEAT.toString()),
        equalTo(ClientHelper.DEFAULT_HEARTBEAT));
    assertThat((String) config.get(LuaConfigParameter.APPLICATION.toString()),
        equalTo(ClientHelper.DEFAULT_APPLICATION));
    assertThat((int) config.get(LuaConfigParameter.GRACE_PERIOD.toString()),
        equalTo(ClientHelper.DEFAULT_GRACE_PERIOD));
    assertThat((int) config.get(LuaConfigParameter.JOBS_HISTORY.toString()),
        equalTo(ClientHelper.DEFAULT_JOBS_HISTORY));
    assertThat((int) config.get(LuaConfigParameter.STATS_HISTORY.toString()),
        equalTo(ClientHelper.DEFAULT_STATS_HISTORY));
    assertThat((int) config.get(LuaConfigParameter.HISTOGRAM_HISTORY.toString()),
        equalTo(ClientHelper.DEFAULT_HISTOGRAM_HISTORY));
    assertThat((int) config.get(LuaConfigParameter.JOBS_HISTORY_COUNT.toString()),
        equalTo(ClientHelper.DEFAULT_JOBS_HISTORY_COUNT));
  }

  @Test
  public void setGetUnset() throws IOException {
    final String itemName = "testing";
    this.client.getConfig().put(itemName, this.testKey);

    assertThat((String) this.client.getConfig().get(itemName),
        equalTo(this.testKey));
    assertThat((String) this.client.getConfig().getMap().get(itemName),
        equalTo(this.testKey));

    this.client.getConfig().remove(itemName);
    assertThat(this.client.getConfig().get(itemName),
        nullValue());
  }

  @Test
  public void clear() throws IOException {
    final Map<String, Object> originalConfig = this.client.getConfig().getMap();
    for (final String key : originalConfig.keySet()) {
      this.client.getConfig().put(key, this.testKeyValue);
    }

    this.client.getConfig().clear();
    assertThat(this.client.getConfig().getMap(),
        equalTo(originalConfig));
  }

  @Test
  public void len() throws IOException {
    assertThat(this.client.getConfig().getMap().keySet(),
        hasSize(7));
  }

  @Test
  public void contains() throws IOException {
    assertThat(this.client.getConfig().getMap().keySet(),
        not(Matchers.contains(this.testKey)));

    this.client.getConfig().put(this.testKey, this.testKeyValue);
    assertThat(this.client.getConfig().getMap().keySet(),
        not(Matchers.contains(this.testKey)));
  }

  @Test
  public void get() throws IOException {
    this.client.getConfig().put(this.testKey, this.testKeyValue);
    assertThat((String) this.client.getConfig().get(this.testKey),
        equalTo(this.testKeyValue));
  }

  @Test
  public void pop() throws IOException {
    this.client.getConfig().put(this.testKey, this.testKeyValue);

    assertThat((String) this.client.getConfig().pop(this.testKey),
        equalTo(this.testKeyValue));
    assertThat(this.client.getConfig().getMap().keySet(),
        not(Matchers.contains(this.testKey)));
  }

  @Test
  public void update() throws IOException {
    this.client.getConfig().put(this.testKey, this.testKeyValue);
    assertThat((String) this.client.getConfig().pop(this.testKey),
        equalTo(this.testKeyValue));

    this.client.getConfig().put(this.testKey, "2");
    assertThat((String) this.client.getConfig().pop(this.testKey),
        equalTo("2"));
  }
}
