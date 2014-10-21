package com.moz.qless.utils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.google.common.io.Resources;
import com.moz.qless.Client;
import com.moz.qless.Job;

import org.codehaus.jackson.map.InjectableValues;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;
import org.easymock.EasyMock;
import org.junit.Test;

public class JsonUtilsTest {

  @Test
  public void parseJob() throws IOException {
    final String json = Resources.toString(
        Resources.getResource(JsonUtilsTest.class, "job.json"), Charset.defaultCharset());

    final InjectableValues injectables = new InjectableValues.Std().addValue("client",
        EasyMock.createNiceMock(Client.class));
    final Job job = JsonUtils.parse(json, Job.class, injectables);

    assertThat(job.getJid(),
        equalTo("8fbb94d60c3249f9a466c74ef57740fc"));
    assertThat(job.getRetries(),
        equalTo(3));
    assertThat(job.getData(),
        notNullValue());
    assertThat(job.getData().keySet(),
        is(empty()));
    assertThat(job.getFailure(),
        notNullValue());
    assertThat(job.getFailure().keySet(),
        is(empty()));
    assertThat(job.getExpires(),
        is((long) 0));
    assertThat(job.getRemaining(),
        is(5));
    assertThat(job.getDependencies(),
        notNullValue());
    assertThat(job.getDependencies(),
        hasSize(0));
    assertThat(job.getSpawnedFromJid(),
        equalTo("false"));
    assertThat(job.getKlassName(),
        equalTo("foo"));
    assertThat(job.getTracked(),
        equalTo(false));
    assertThat(job.getTags(),
        notNullValue());
    assertThat(job.getTags(),
        hasSize(0));
    assertThat(job.getQueueName(),
        equalTo("foo"));
    assertThat(job.getState(),
        equalTo("waiting"));
    assertThat(job.getHistory(),
        notNullValue());
    assertThat(job.getHistory(),
        hasSize(1));
    assertThat(job.getHistory().get(0).when(),
        notNullValue());
    assertThat(job.getHistory().get(0).when().longValue(),
        equalTo((long) 152681526));
    assertThat(job.getHistory().get(0).queueName(),
        equalTo("foo"));
    assertThat(job.getHistory().get(0).what().toString(),
        equalTo("put"));
    assertThat(job.getDependencies(),
        notNullValue());
    assertThat(job.getDependencies(),
        hasSize(0));
    assertThat(job.getPriority(),
        equalTo(1));
    assertThat(job.getWorker(),
        equalTo("johnzhu_mac-7288"));
  }

  @Test
  public void parseJobs() throws IOException {
    final String json = Resources
        .toString(Resources.getResource(JsonUtilsTest.class, "jobs.json"),
            Charset.defaultCharset());
    final InjectableValues injectables = new InjectableValues.Std().addValue("client",
        EasyMock.createNiceMock(Client.class));

    final JavaType javaType = new ObjectMapper().getTypeFactory()
        .constructCollectionType(ArrayList.class, Job.class);
    final List<Job> jobs = JsonUtils.parse(json, javaType, injectables);

    assertThat(jobs,
        notNullValue());
    assertThat(jobs,
        hasSize(2));

    final List<String> jobIds = new ArrayList<>();
    jobIds.add(jobs.get(0).getJid());
    jobIds.add(jobs.get(1).getJid());

    assertThat(jobIds,
        containsInAnyOrder("8fbb94d60c3249f9a466c74ef57740f1",
                           "8fbb94d60c3249f9a466c74ef57740f2"));
  }

}
