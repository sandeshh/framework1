package com.example;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Created by sandesh on 8/17/16.
 */
public class MyScheduler implements Scheduler
{
  List<Job> jobs = new ArrayList<Job>();

  public MyScheduler(List<Job> jobs)
  {
    this.jobs = jobs;
  }

  public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo)
  {
    System.out.println("Registerd with the framework id " + frameworkID);
  }

  public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo)
  {

  }

  public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID)
  {

  }

  public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus)
  {
    System.out.println("Got status update " + taskStatus);
  }

  public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes)
  {

  }

  public void disconnected(SchedulerDriver schedulerDriver)
  {

  }

  public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID)
  {

  }

  public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i)
  {

  }

  public void error(SchedulerDriver schedulerDriver, String s)
  {

  }

  public void resourceOffers(SchedulerDriver driver, java.util.List<Protos.Offer> offers) {
    synchronized (jobs) {
      List<Job> pendingJobs = new ArrayList<>();
      for (Job j : jobs) {
        if (!j.isSubmitted()) {
          pendingJobs.add(j);
        }
      }
      for (Protos.Offer o : offers) {
        if (pendingJobs.isEmpty()) {
          driver.declineOffer(o.getId());
          break;
        }
        Job j = pendingJobs.remove(0);
        Protos.TaskInfo ti = j.makeTask(o.getSlaveId());
        driver.launchTasks(
            Collections.singletonList(o.getId()),
            Collections.singletonList(ti)
        );
        j.setSubmitted(true);
      }
    }
  }

}
