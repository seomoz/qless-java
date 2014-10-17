package com.moz.qless.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.moz.qless.Client;
import com.moz.qless.Job;
import com.moz.qless.lua.LuaCommand;
import com.moz.qless.RecurringJob;
import com.moz.qless.utils.JsonUtils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.InjectableValues;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;

public class Jobs {
  private final Client client;

  public Jobs(final Client client) {
    this.client = client;
  }

  public List<String> complete() throws IOException {
    return this.complete(0, 25);
  }

  /**
   * Return the paginated job objects of complete jobs
   */
  @SuppressWarnings("unchecked")
  public List<String> complete(final int offset, final int count) throws IOException {
    final Object result = this.client.call(
        LuaCommand.JOBS.toString(),
        LuaCommand.COMPLETE.toString(),
        Integer.toString(offset),
        Integer.toString(count));

    return (List<String>) result;
  }

  public String failed() throws IOException {
    final Object result = this.client.call(
        LuaCommand.FAILED.toString());

    return result.toString();
  }

  public List<String> failed(final String group) throws IOException {
    return this.failed(group, 0, 25);
  }

  /**
   * If no group is provided, this returns a JSON blob of the counts of the various types
   * of failures known. If a group is provided, returns paginated job objects affected by
   * that kind of failure.
   */
  public List<String> failed(final String group, final int start, final int limit)
      throws IOException {
    final Object result = this.client.call(
	    LuaCommand.FAILED.toString(),
	    group,
      Integer.toString(start),
      Integer.toString(limit));

    final ObjectMapper mapper = new ObjectMapper();
    final JsonNode resultObj = mapper.readTree(result.toString());

    final JavaType javaType = new ObjectMapper().getTypeFactory()
        .constructCollectionType(ArrayList.class, String.class);
    return JsonUtils.parse(resultObj.get("jobs").toString(), javaType);
  }

  /**
   * Return the list of job object for the given jids
   */
  public List<Job> get(final List<String> jids) throws IOException {
    final List<Job> jobList = new ArrayList<>();
    for (final String jid : jids) {
      jobList.add(this.get(jid));
    }

    return jobList;
  }

  /**
   * Return job object for the given jid
   */
  public Job get(final String jid) throws IOException {
    Class<? extends Job> klass = Job.class;
    Object result = this.client.call(
	    LuaCommand.GET.toString(),
	    jid);

    if (null == result) {
      result = this.client.call(
	      LuaCommand.RECUR_GET.toString(),
	      jid);
      if (null == result) {
        return null;
      }
      klass = RecurringJob.class;
    }

    final String json = result.toString();
    final InjectableValues inject = new InjectableValues.Std().addValue(
	    "client",
      this.client);
    return JsonUtils.parse(json, klass, inject);
  }

  public List<String> tagged(final String tag) throws IOException {
    return this.tagged(tag, 0, 25);
  }

  /**
   * Return the paginated job objects tagged with a tag
   */
  public List<String> tagged(final String tag, final int offset, final int count)
      throws IOException {
    final Object result = this.client.call(
	    LuaCommand.TAG.toString(),
      LuaCommand.GET.toString(),
      tag,
      Integer.toString(offset),
      Integer.toString(count));

    final ObjectMapper mapper = new ObjectMapper();
    final JsonNode resultObj = mapper.readTree(result.toString());

    final JavaType javaType = new ObjectMapper().getTypeFactory()
        .constructCollectionType(ArrayList.class, String.class);
    return JsonUtils.parse(resultObj.get("jobs").toString(), javaType);
  }

  /**
   * Return the list of job objects that are being tracked
   */
  public List<Job> tracked() throws IOException {
    final Object result = this.client.call(
        LuaCommand.TRACK.toString());

    final ObjectMapper mapper = new ObjectMapper();
    final JsonNode resultObj = mapper.readTree(result.toString());

    final InjectableValues injectables = new InjectableValues.Std().addValue("client",
        this.client);
    final JavaType javaType = new ObjectMapper().getTypeFactory()
        .constructCollectionType(ArrayList.class, Job.class);
    return JsonUtils.parse(resultObj.get("jobs").toString(), javaType, injectables);
  }

}
