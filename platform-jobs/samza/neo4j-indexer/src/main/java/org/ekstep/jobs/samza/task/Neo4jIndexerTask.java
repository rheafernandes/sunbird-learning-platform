package org.ekstep.jobs.samza.task;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.ekstep.jobs.samza.service.ISamzaService;
import org.ekstep.jobs.samza.service.Neo4jIndexerService;
import org.ekstep.jobs.samza.service.task.JobMetrics;
import org.ekstep.jobs.samza.util.JobLogger;
import org.ekstep.jobs.samza.util.SamzaCommonParams;
import org.ekstep.learning.util.ControllerUtil;

import java.util.HashMap;
import java.util.Map;

public class Neo4jIndexerTask implements StreamTask, InitableTask, WindowableTask {

    private final JobLogger logger = new JobLogger(Neo4jIndexerTask.class);
    private JobMetrics metrics;
    private ISamzaService service;
    private ControllerUtil controllerUtil;

    public Neo4jIndexerTask(Config config, TaskContext taskContext) throws Exception{
        init(config,taskContext);
    }


    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
        try {
            metrics = new JobMetrics(taskContext,config.get("output.metrics.job.name"),config.get("output.metrics.topic.name"));
            service = new Neo4jIndexerService();
            controllerUtil = new ControllerUtil();
            service.initialize(config);
            logger.info("Task initialized");
        } catch(Exception ex) {
            logger.error("Task initialization failed", ex);
            throw ex;
        }
    }

    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        Map<String, Object> outgoingMap = getMessage(incomingMessageEnvelope);
        try {
            if (outgoingMap.containsKey(SamzaCommonParams.edata.name())) {
                Map<String, Object> edata = (Map<String, Object>) outgoingMap.getOrDefault(SamzaCommonParams.edata.name(), new HashMap<String, Object>());
                if (MapUtils.isNotEmpty(edata) && StringUtils.equalsIgnoreCase("definition_update", edata.getOrDefault("action", "").toString())) {
                    logger.info("definition_update event received for objectType: " + edata.getOrDefault("objectType", "").toString());
                    String graphId = edata.getOrDefault("graphId", "").toString();
                    String objectType = edata.getOrDefault("objectType", "").toString();
                    controllerUtil.updateDefinitionCache(graphId, objectType);
                }
            } else {
                service.processMessage(outgoingMap, metrics, messageCollector);
            }
        } catch (Exception e) {
            metrics.incErrorCounter();
            logger.error("Error while processing message:", outgoingMap, e);
        }
    }
    private Map<String, Object> getMessage(IncomingMessageEnvelope envelope) {
        try {
            return (Map<String, Object>) envelope.getMessage();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Invalid message:" + envelope.getMessage(), e);
            return new HashMap<String, Object>();
        }
    }

    @Override
    public void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        Map<String, Object> event = metrics.collect();
        messageCollector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", metrics.getTopic()), event));
        metrics.clear();
    }
}
