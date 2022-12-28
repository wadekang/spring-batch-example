package com.example.batchDemo.elasticsearch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class ElasticsearchQuery {

    private final RestClient restClient;
    private static final String AGGREGATION_KEY = "byAssetsId";

    public void oneDayCollectUpsert() {
        Map<Integer, Integer> esResponse = searchCollectingAssetListES(1);
        if (esResponse == null || esResponse.isEmpty()) {
            log.info("No Data");
            return;
        }

        esResponse.forEach((key, value) -> {
            log.info("key: {}, value: {}", key, value);
        });
    }

    private Map<Integer, Integer> searchCollectingAssetListES(int optionDate) {
        Map<Integer, Integer> bucketMap = new HashMap<>();

        try {
            String base = "dsp_base_*_*_";
            List<String> endpoints = new ArrayList<>();

            LocalDateTime dateTime = LocalDateTime.now(ZoneId.of("UTC"));
            for (int i = 0; i < optionDate + 1; i++) {
                String endpoint = base + dateTime.format(DateTimeFormatter.BASIC_ISO_DATE);
                endpoint = URLEncoder.encode(endpoint, "UTF-8");
                endpoints.add(endpoint);

                dateTime = dateTime.minus(1, ChronoUnit.DAYS);
            }
            dateTime = dateTime.plus(1, ChronoUnit.DAYS);

            Request request = new Request("POST", "/" + String.join(",", endpoints) + "/_search?");

            String queryString = getESQueryString(dateTime);

            request.addParameter("pretty", "false");
            request.setEntity(new NStringEntity(queryString, ContentType.APPLICATION_JSON));

            try {
                Response response = restClient.performRequest(request);

                JSONObject reJsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                if(reJsonObject.isNull("aggregations")) return null;

                JSONArray buckets = reJsonObject.getJSONObject("aggregations").getJSONObject(AGGREGATION_KEY).getJSONArray("buckets");

                buckets.forEach(item -> {
                    JSONObject buck = (JSONObject) item;
                    bucketMap.put(buck.getInt("key"), buck.getInt("doc_count"));
                });
            } catch (Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }

        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

        return bucketMap;
    }

    private String getESQueryString(LocalDateTime dateTime) {
        long epochMilli = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();

        String query = "\"query\":{\"bool\":{\"must\":[{\"range\":{\"collectorReceiptTime\":{\"gte\":" + epochMilli + "}}}]}},";
        String aggregations = "\"aggregations\":{\"" + AGGREGATION_KEY + "\":{\"terms\":{\"field\":\"assetsId\",\"size\":10000}}}";

        return "{\"size\":0," + query + aggregations + "}";
    }
}
