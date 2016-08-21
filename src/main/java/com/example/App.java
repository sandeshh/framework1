package com.example;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(String[] args) throws IOException
    {
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
        SchedulerDriver driver = new MesosSchedulerDriver(
            mySched,
            frameworkInfo,
            args[1]
	          );

        driver.start();
        driver.join();
    }
}
