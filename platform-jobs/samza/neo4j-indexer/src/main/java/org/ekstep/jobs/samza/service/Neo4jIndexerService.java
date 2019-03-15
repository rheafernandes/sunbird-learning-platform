package org.ekstep.jobs.samza.service;

import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.ekstep.common.dto.Response;
import org.ekstep.common.mgr.ConvertToGraphNode;
import org.ekstep.graph.dac.model.Node;
import org.ekstep.graph.model.node.DefinitionDTO;
import org.ekstep.jobs.samza.service.task.JobMetrics;
import org.ekstep.jobs.samza.service.util.INeo4jIndexer;
import org.ekstep.jobs.samza.util.JSONUtils;
import org.ekstep.jobs.samza.util.JobLogger;
import org.ekstep.learning.router.LearningRequestRouterPool;

import java.util.Map;

public class Neo4jIndexerService implements ISamzaService, INeo4jIndexer {

    private SystemStream systemStream;
    private final JobLogger logger = new JobLogger(Neo4jIndexerService.class);


    @Override
    public void initialize(Config config) throws Exception {
        JSONUtils.loadProperties(config);
        logger.info("Service config initialized");
        LearningRequestRouterPool.init();
        logger.info("Learning actors initialized");
        systemStream = new SystemStream("kafka", config.get("output.failed.events.topic.name"));
    }

    @Override
    public void processMessage(Map<String, Object> message, JobMetrics metrics, MessageCollector collector) throws Exception {
        System.out.println("Printing the message received: " + message);
        System.out.println("These are metrics"  + metrics);
        try{
            switch ((String) message.get("operationType")) {
                case "CREATE" : {
                    Map contentMap = INeo4jIndexer.prepareContentMap(message);
                    DefinitionDTO definitionDTO = INeo4jIndexer.getDefinition((String)message.get("graphId"), (String)message.get("objectType"));
                    if( !contentMap.isEmpty() && definitionDTO != null) {
                    Node node = INeo4jIndexer.getNode(message, contentMap, definitionDTO);
                    if(node != null) {
                        Response response = INeo4jIndexer.getResponse(node);
                        System.out.println(response.getResponseCode());
                        System.out.println(response.getResult());
                    }
                    }
                    break;
                }
                case "UPDATE" : {
                    System.out.println("Let's see what to do");
                    break;
                }
            }

        } catch (Exception e){
            logger.error("Processing of message failed",e);
        }
    }



}
