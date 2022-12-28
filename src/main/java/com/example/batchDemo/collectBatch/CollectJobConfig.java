package com.example.batchDemo.collectBatch;

import com.example.batchDemo.elasticsearch.ElasticsearchQuery;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class CollectJobConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final ElasticsearchQuery elasticsearchQuery;

    @Bean
    public Job CollectJob() {
        log.info("Start CollectJob!");

        return jobBuilderFactory.get("collectJob")
                .start(collectBatchStep())
                .build();
    }

    @Bean
    public Step collectBatchStep() {
        return stepBuilderFactory.get("collectBatchStep")
                .tasklet((contribution, chunkContext) -> {
                    log.info("Batch Step!");

                    oneDayCollect();
                    threeDayCollect();
                    sevenDayCollect();

                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    private void oneDayCollect() {
        log.info("oneDayCollect!");

        elasticsearchQuery.oneDayCollectUpsert();
    }

    private void threeDayCollect() {
        log.info("threeDayCollect!");
    }

    private void sevenDayCollect() {
        log.info("sevenDayCollect!");
    }
}
