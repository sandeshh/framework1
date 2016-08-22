package com.example;

import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.VMLeaseObject;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Hello world!
 *
 */
public class App 
{
    MesosSchedulerDriver driver;
    private final BlockingQueue<VirtualMachineLease> leasesQueue;


    public App(String[] args) throws IOException
    {
        leasesQueue = new LinkedBlockingQueue<>();

        List<Job> jobs = new ArrayList<>();
        byte[] data = Files.readAllBytes(Paths.get(args[2]));
        JSONObject config = new JSONObject(new String(data, "UTF-8"));
        JSONArray jobsArray = config.getJSONArray("jobs");

        for (int i = 0; i < jobsArray.length(); i++) {
            jobs.add(Job.fromJSON(jobsArray.getJSONObject(i)));
        }

        System.out.println(jobs);

        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
            .setUser("")
            .setName("Useless remote BASH")
            .build();

        Scheduler mySched = new MyScheduler(jobs);
        driver = new MesosSchedulerDriver(
            mySched,
            frameworkInfo,
            args[1]
	          );

        driver.start();
        driver.join();
    }

    public class MyScheduler implements Scheduler
    {
        List<Job> jobs = new ArrayList<Job>();
        private final TaskScheduler scheduler;
        private final Map<String, String> launchedTasks;
        private final AtomicReference<MesosSchedulerDriver> ref = new AtomicReference<>();

        public MyScheduler(List<Job> jobs)
        {
            this.jobs = jobs;
            this.scheduler = new TaskScheduler.Builder().withLeaseOfferExpirySecs(1000000000).withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                @Override
                public void call(VirtualMachineLease lease) {
                    System.out.println("Declining offer on " + lease.hostname());
                    ref.get().declineOffer(lease.getOffer().getId());
                }
            }).build();

            launchedTasks = new HashMap<>();
            ref.set(driver);
        }

        public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo)
        {
            System.out.println("Registerd with the framework id " + frameworkID);
            scheduler.expireAllLeases();
        }

        public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo)
        {
            scheduler.expireAllLeases();
        }

        public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID)
        {
            scheduler.expireLease(offerID.getValue());
        }

        public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus)
        {
            System.out.println("Got status update " + taskStatus);

            switch (taskStatus.getState()) {
                case TASK_FAILED:
                case TASK_LOST:
                case TASK_FINISHED:
                    //   scheduler.getTaskUnAssigner().call(taskStatus.getTaskId().getValue(), launchedTasks.get(taskStatus.getTaskId().getValue()));
                    break;
            }
        }

        public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes)
        {

        }

        public void disconnected(SchedulerDriver schedulerDriver)
        {

        }

        public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID)
        {
            scheduler.expireAllLeasesByVMId(slaveID.getValue());
        }

        public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i)
        {

        }

        public void error(SchedulerDriver schedulerDriver, String s)
        {

        }

        public void resourceOffers(SchedulerDriver driver, java.util.List<Protos.Offer> offers) {

            for(Protos.Offer offer: offers) {
                System.out.println("Adding offer " + offer.getId() + " from host " + offer.getHostname());
                leasesQueue.offer(new VMLeaseObject(offer));
            }

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
}
