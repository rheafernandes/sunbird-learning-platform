package org.ekstep.jobs.samza.service.util;


import org.ekstep.common.dto.Response;
import org.ekstep.common.mgr.ConvertToGraphNode;
import org.ekstep.graph.dac.model.Node;
import org.ekstep.graph.model.node.DefinitionDTO;
import org.ekstep.learning.util.ControllerUtil;

import java.security.SecureRandom;
import java.util.Map;

public interface INeo4jIndexer {

    public final ControllerUtil controllerUtil = new ControllerUtil();

    public static Node prepareNode(){
       return null;
    }

    public static Map<String,Object> prepareContentMap(Map incomingMap){
        Map resultMap = null;
        if (incomingMap.get("transactionData") != null){
            Map <String, Object> properties = (Map<String, Object>) ((Map)incomingMap.get("transactionData")).get("properties");
            if (properties != null && !properties.isEmpty()) {
                for (String key : properties.keySet()) {
                    if(((Map<String,Object>) properties.get(key)).get("nv") != null){
                        resultMap.put(key,((Map<String,Object>) properties.get(key)).get("nv"));
                    }
                }
            }
        }
        System.out.println(resultMap);
        return resultMap;
    }

    public static DefinitionDTO getDefinition(String graphId, String objectType){
        controllerUtil.updateDefinitionCache(graphId,objectType);
        return controllerUtil.getDefinition(graphId, objectType);
    }

    public static Node getNode (Map message, Map contentMap, DefinitionDTO definitionDTO){
        try{
            Node node = ConvertToGraphNode.convertToGraphNode(contentMap, definitionDTO, null);
            node.setObjectType((String)message.get("objectType"));
            node.setGraphId((String)message.get("graphId"));
            return node;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Response getResponse(Node node ){
        return controllerUtil.createDataNode(node);
    }

}
