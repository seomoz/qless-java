package com.moz.qless.utils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.google.common.io.Resources;
import com.moz.qless.Client;
import com.moz.qless.Job;

import org.codehaus.jackson.map.InjectableValues;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class JsonUtilsTest {

  @Test
  public void parseJob() throws IOException {
    final String json = Resources.toString(
        Resources.getResource(JsonUtilsTest.class, "job.json"), Charset.defaultCharset());

    final InjectableValues injectables = new InjectableValues.Std().addValue("client",
        EasyMock.createNiceMock(Client.class));
    final Job job = JsonUtils.parse(json, Job.class, injectables);

    Assert.assertEquals("8fbb94d60c3249f9a466c74ef57740fc", job.getJid());
    Assert.assertEquals(3, job.getRetries());
    Assert.assertNotNull(job.getData());
    Assert.assertEquals(true, job.getData().isEmpty());
    Assert.assertNotNull(job.getFailure());
    Assert.assertEquals(true, job.getFailure().isEmpty());
    Assert.assertEquals(0, job.getExpires());
    Assert.assertEquals(5, job.getRemaining());
    Assert.assertNotNull(job.getDependencies());
    Assert.assertEquals(0, job.getDependencies().size());
    Assert.assertEquals("false", job.getSpawnedFromJid());
    Assert.assertEquals("foo", job.getKlassName());
    Assert.assertEquals(false, job.getTracked());
    Assert.assertNotNull(job.getTags());
    Assert.assertEquals(0, job.getTags().size());
    Assert.assertEquals("foo", job.getQueueName());
    Assert.assertEquals("waiting", job.getState());
    Assert.assertNotNull(job.getHistory());
    Assert.assertEquals(1, job.getHistory().size());
    Assert.assertNotNull(job.getHistory().get(0).when());
    Assert.assertEquals(152681526, job.getHistory().get(0).when().longValue());
    Assert.assertEquals("foo", job.getHistory().get(0).queueName());
    Assert.assertEquals("put", job.getHistory().get(0).what());
    Assert.assertNotNull(job.getDependencies());
    Assert.assertEquals(0, job.getDependencies().size());
    Assert.assertEquals(1, job.getPriority());
    Assert.assertEquals("johnzhu_mac-7288", job.getWorker());
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

    Assert.assertNotNull(jobs);
    Assert.assertEquals(2, jobs.size());
    Assert.assertEquals("8fbb94d60c3249f9a466c74ef57740f1", jobs.get(0).getJid());
    Assert.assertEquals("8fbb94d60c3249f9a466c74ef57740f2", jobs.get(1).getJid());
  }

}
