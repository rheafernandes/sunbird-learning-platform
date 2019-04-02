package org.ekstep.jobs.samza.service;

import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.ekstep.common.dto.Response;
import org.ekstep.graph.dac.model.Node;
import org.ekstep.graph.model.node.DefinitionDTO;
import org.ekstep.graph.model.node.MetadataDefinition;
import org.ekstep.graph.model.node.RelationDefinition;
import org.ekstep.graph.model.node.TagDefinition;
import org.ekstep.jobs.samza.service.task.JobMetrics;
import org.ekstep.jobs.samza.service.util.INeo4jIndexer;
import org.ekstep.jobs.samza.util.JSONUtils;
import org.ekstep.learning.router.LearningRequestRouterPool;

import java.util.List;
import java.util.Map;

/* SET TYPE AND TESTING TO BE DONE */
public class Neo4jIndexerService implements ISamzaService, INeo4jIndexer {

    private SystemStream systemStream;

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
        try{
            switch ((String)message.get(Node_Type)){
                case "DATA_NODE": case "SET":{
                    Map contentMap = INeo4jIndexer.prepareContentMap(message);
                    DefinitionDTO definitionDTO = INeo4jIndexer.getDefinition((String)message.get(Graph_Id), (String)message.get(Object_Type));
                    if( contentMap != null && definitionDTO != null) {
                        Node node = null;
                        switch ((String)message.get(Operation_Type)) {
                            case "CREATE": {
                                node = INeo4jIndexer.getNode(message, contentMap, definitionDTO,null);
                                break;
                            }
                            case "UPDATE": {
                                Node existingNode = controllerUtil.getNode((String)message.get(Graph_Id),(String) message.get(NODE_UNIQUE_ID));
                                if(existingNode != null)
                                    node = INeo4jIndexer.getNode(message, contentMap, definitionDTO,existingNode);
                                break;
                            }
                            case "DELETE": {
                                Node existingNode = controllerUtil.getNode((String)message.get(Graph_Id),(String) message.get(NODE_UNIQUE_ID));
                                if(existingNode != null)
                                    node= existingNode;
                                break;
                            }
                        }
                        if(node != null) {
                            Response response = INeo4jIndexer.getResponse(node,(String) message.get(Operation_Type));
                            System.out.println(response.getResponseCode());
                            System.out.println(response.getParams().getErrmsg());
                        }
                    }else{
                        logger.info("Transaction Data isn't available.");
                    }
                    break;
                }
                case "DEFINITION_NODE":{
                    if(((String)message.get(Operation_Type)).equals("CREATE") || ((String)message.get(Operation_Type)).equals("UPDATE") ){
                        Map<String,Object> transactionDataProperties = (Map<String, Object>) ((Map) message.get(Transaction_Data)).get("properties");
                        List<MetadataDefinition> properties = INeo4jIndexer.getDefinitionProperties(transactionDataProperties);
                        List<RelationDefinition> inRelations = INeo4jIndexer.getRelationDefinition(transactionDataProperties,IN);
                        List<RelationDefinition> outRelations = INeo4jIndexer.getRelationDefinition(transactionDataProperties,OUT);
                        List<TagDefinition> systemTags = INeo4jIndexer.getSystemTags(transactionDataProperties);
                        Map<String,Object> metadata = INeo4jIndexer.prepareContentMap(message);
                        metadata.remove("versionKey");
                        DefinitionDTO definitionDTO = INeo4jIndexer.getDefinitionObject(message, properties,
                                inRelations, outRelations, systemTags, metadata);
                        if(definitionDTO != null){
                            Response response = controllerUtil.addDefinitionNode(definitionDTO);
                            System.out.println(response.getResponseCode());
                        }
                    }else if(((String)message.get(Operation_Type)).equals("DELETE")){
                        Response response = controllerUtil.deleteDefinition((String)message.get(Graph_Id), (String)message.get(Object_Type));
                        System.out.println(response.getResponseCode());
                        System.out.println(response.getParams().getErrmsg());
                    }
                    break;
                }
//                case "SET" :{
//
//                }
            }
        } catch (Exception e){
            logger.error("Processing of message failed",e);
        }
    }



}
