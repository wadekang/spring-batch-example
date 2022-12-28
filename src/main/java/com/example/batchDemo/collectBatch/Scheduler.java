package com.example.batchDemo.collectBatch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class Scheduler {

    private final JobLauncher jobLauncher;
    private final CollectJobConfig collectJobConfig;

    @Scheduled(initialDelay = 1000, fixedDelay = 1000 * 10)
    public void updateCollectCount() {
        try {
            log.info("Start updateCollectCount");
            JobExecution execution = jobLauncher.run(collectJobConfig.CollectJob(), simpleJobParam());
            log.info("Job finished with status: {}", execution.getStatus());
            log.info("Current Thread: {}", Thread.currentThread().getName());
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private JobParameters simpleJobParam() {
        Map<String, JobParameter> confMap = new HashMap<>();
        confMap.put("time", new JobParameter(System.currentTimeMillis()));
        return new JobParameters(confMap);
    }
}
