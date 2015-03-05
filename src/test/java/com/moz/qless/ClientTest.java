package com.moz.qless;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.moz.qless.lua.LuaJobStatus;

import org.junit.Ignore;
import org.junit.Test;

public class ClientTest extends IntegrationTest {
  @Test
  public void track() throws IOException {
    final String jid = this.queue.put(jobSpec());

    this.client.track(jid);

    assertThat(this.client.getJobs().tracked().get(0).getJid(),
        equalTo(jid));
  }

  @Test
  public void unTrack() throws IOException {
    final String jid = this.queue.put(jobSpec());

    this.client.track(jid);
    assertThat(this.client.getJobs().tracked().get(0).getJid(),
        equalTo(jid));

    this.client.untrack(jid);
    assertThat(this.client.getJobs().tracked(),
        nullValue());
  }

  @Test
  @Ignore
  /*
   * This test pending on a qless-core bug filed at:
   * https://github.com/seomoz/qless-core/issues/55
   */
  public void tags() throws IOException {
    assertThat(this.client.tags(), nullValue());

    final String jid = this.queue.put(jobSpec());

    this.client.getJobs().get(jid).tag("tag1", "tag2");
    assertThat(this.client.tags(),
        containsInAnyOrder("tag1", "tag2"));
  }

  @Test
  public void unfail() throws IOException {
    final String jid = this.queue.put(jobSpec());

    this.queue.pop().fail(ClientTest.DEFAULT_NAME, ClientTest.DEFAULT_NAME);

    assertThat(this.client.getJobs().get(jid).getState(),
        equalTo(LuaJobStatus.FAILED.toString()));

    this.client.unfail(ClientTest.DEFAULT_NAME, ClientTest.DEFAULT_NAME);

    assertThat(this.client.getJobs().get(jid).getState(),
        equalTo(LuaJobStatus.WAITING.toString()));
  }

  @Test
  public void redisUrlBasic() throws IOException {
    new Client("redis://localhost:6379/");
  }
}
