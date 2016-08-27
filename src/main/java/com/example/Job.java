package com.example;

import org.apache.mesos.Protos;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by sandesh on 8/17/16.
 */
public class Job
{
  private double cpus;
  private double mem;
  private String command;
  private boolean submitted;
  private List<String> uris = new ArrayList<>();

  public double getCpus()
  {
    return cpus;
  }

  public void setCpus(double cpus)
  {
    this.cpus = cpus;
  }

  public double getMem()
  {
    return mem;
  }

  public void setMem(double mem)
  {
    this.mem = mem;
  }

  public String getCommand()
  {
    return command;
  }

  public void setCommand(String command)
  {
    this.command = command;
  }

  public boolean isSubmitted()
  {
    return submitted;
  }

  public void setSubmitted(boolean submitted)
  {
    this.submitted = submitted;
  }

  private Job() {
    submitted = false;
  }

  public Protos.TaskInfo makeTask(Protos.SlaveID targetSlave)
  {
    UUID uuid = UUID.randomUUID();
    Protos.TaskID id = Protos.TaskID.newBuilder()
        .setValue(uuid.toString())
        .build();

    Protos.CommandInfo.Builder commandInfoBuilder = Protos.CommandInfo.newBuilder().setValue(command);

    for (int i=0; i < uris.size(); ++i) {

      commandInfoBuilder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(uris.get(i)).build());
    }

    Protos.TaskInfo task = Protos.TaskInfo.newBuilder().setName("task " + id)
        .setCommand(commandInfoBuilder)
            .setTaskId(id)
        .addResources(Protos.Resource.newBuilder()
            .setName("cpus")
            .setType(Protos.Value.Type.SCALAR)
            .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpus))
        )
        .addResources(Protos.Resource.newBuilder()
            .setName("mem")
            .setType(Protos.Value.Type.SCALAR)
            .setScalar(Protos.Value.Scalar.newBuilder().setValue(mem))
        )
        .setSlaveId(targetSlave)
        .build();

    return task;
  }

  public static Job fromJSON(JSONObject jsonObject) throws JSONException
  {
    Job job = new Job();
    job.cpus = jsonObject.getDouble("cpus");
    job.mem = jsonObject.getDouble("mem");
    job.command = jsonObject.getString("command");

    JSONArray jsonArray = jsonObject.getJSONArray("uris");

    for (int i=0; i < jsonArray.length(); ++i) {
      job.uris.add(jsonArray.getString(i));
    }

    return job;
  }
}
