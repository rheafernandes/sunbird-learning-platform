package org.ekstep.jobs.samza.service.util;


import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.ekstep.common.Platform;
import org.ekstep.common.dto.Response;
import org.ekstep.common.exception.ClientException;
import org.ekstep.common.mgr.ConvertToGraphNode;
import org.ekstep.graph.dac.enums.SystemProperties;
import org.ekstep.graph.dac.model.Node;
import org.ekstep.graph.dac.model.Relation;
import org.ekstep.graph.exception.GraphEngineErrorCodes;
import org.ekstep.graph.model.node.DefinitionDTO;
import org.ekstep.graph.model.node.MetadataDefinition;
import org.ekstep.graph.model.node.RelationDefinition;
import org.ekstep.graph.model.node.TagDefinition;
import org.ekstep.graph.service.common.DACConfigurationConstants;
import org.ekstep.jobs.samza.exception.PlatformErrorCodes;
import org.ekstep.jobs.samza.exception.PlatformException;
import org.ekstep.jobs.samza.service.Neo4jIndexerService;
import org.ekstep.jobs.samza.util.JobLogger;
import org.ekstep.learning.util.ControllerUtil;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;


public interface INeo4jIndexer {
    public final String Transaction_Data = "transactionData";
    public final String New_Value = "nv";
    public final String Object_Type = "objectType";
    public final String Graph_Id = "graphId";
    public final String Operation_Type = "operationType";
    public final String Identifier = "identifier";
    public final JobLogger logger = new JobLogger(INeo4jIndexer.class);
    public final String IN = "IN";
    public final String OUT = "OUT";
    public final String NODE_UNIQUE_ID = "nodeUniqueId";
    public final String Node_Type = "nodeType";
    public  ObjectMapper mapper = new ObjectMapper();


    public final ControllerUtil controllerUtil = new ControllerUtil();


    public static Map<String, Object> prepareContentMap(Map<String,Object> incomingMap) {
        Map resultMap ;
        Map updatedMap = null;
        if (incomingMap.get(Transaction_Data) != null) {
            resultMap = new HashMap();
            Map<String, Object> properties = (Map<String, Object>) ((Map) incomingMap.get(Transaction_Data)).get("properties");
            if (properties != null && !properties.isEmpty()) {
                for (String key : properties.keySet()) {
                    if (((Map<String, Object>) properties.get(key)).get(New_Value) != null) {
                        if (key.equals(SystemProperties.IL_UNIQUE_ID.name())) {
                            resultMap.put(Identifier, ((Map<String, Object>) properties.get(key)).get(New_Value));
                        }else {
                            resultMap.put(key, ((Map<String, Object>) properties.get(key)).get(New_Value));
                        }
                    }
                }

            }
            resultMap.put("versionKey", Platform.config.getString(DACConfigurationConstants.PASSPORT_KEY_BASE_PROPERTY));
            updatedMap = removeSystemProperties(resultMap);
        }
        return updatedMap;
    }


    public static DefinitionDTO getDefinition(String graphId, String objectType) {
        controllerUtil.updateDefinitionCache(graphId, objectType);
        DefinitionDTO definitionDTO= controllerUtil.getDefinition(graphId, objectType);

        if (null == definitionDTO) {
            logger.info("Failed to fetch definition node from cache");
            throw new PlatformException(PlatformErrorCodes.ERR_DEFINITION_NOT_FOUND.name(),
                    "definition node for graphId:" + graphId + " and objectType:" + objectType
                            + " is null due to some issue");
        }
        return definitionDTO;
    }

    public static Node getNode(Map message, Map contentMap, DefinitionDTO definitionDTO, Node graphNode) {
        try {
            Node node = ConvertToGraphNode.convertToGraphNode(contentMap, definitionDTO, graphNode);
            node.setObjectType((String) message.get(Object_Type));
            node.setGraphId((String) message.get(Graph_Id));
            node.setIdentifier((String) message.get(NODE_UNIQUE_ID));
            node.setInRelations(getUpdatedRelations(message,graphNode, IN));
            node.setOutRelations(getUpdatedRelations(message,graphNode, OUT));
            return node;
        } catch (Exception e) {
            logger.error("Client Error, Enter Valid Tags",e);
            return null;
        }
    }
    public static List<Relation> getUpdatedRelations(Map incomingMap, Node existingNode, String direction){
        List<Relation> updatedRelations = null;
        if(existingNode != null){
            updatedRelations = new ArrayList<>();
            List<Relation> existingRelation = null;
            List <Relation> addedRelations = getRelations(incomingMap,direction,"addedRelations");
            List <Relation> removedRelations = getRelations(incomingMap,direction,"removedRelations");
            switch (direction){
                case IN: {
                         existingRelation = existingNode.getInRelations();
                         break;
                    }
                case OUT: {
                        existingRelation = existingNode.getOutRelations();
                        break;
                    }
            }
        if (existingRelation != null)
            updatedRelations.addAll(existingRelation);
        if (removedRelations != null && !removedRelations.isEmpty())
            updatedRelations = removeRelations(updatedRelations,removedRelations);
        if (addedRelations != null && !addedRelations.isEmpty())
            updatedRelations.addAll(addedRelations);
        }
        return updatedRelations ;
    }

    public static List<Relation> getRelations(Map incomingMap, String Direction, String type) {
        List<Relation> newRelations = new ArrayList<>();
        if (incomingMap.get(Transaction_Data) != null) {
            List<Map<String, Object>> relations = (List<Map<String, Object>>) ((Map) incomingMap.get(Transaction_Data)).get(type);
            if(relations != null && !relations.isEmpty()) {
                relations.forEach(relation -> {
                    switch (Direction){
                        case IN :{
                                if(relation.get("dir") != null && relation.get("dir").toString().equalsIgnoreCase(Direction)) {
                                    Relation newRelation = new Relation((String)relation.get("id"), (String)relation.get("rel"), (String)incomingMap.get(NODE_UNIQUE_ID));
                                    newRelation.setMetadata((Map<String,Object>) relation.get("relMetadata"));
                                    newRelations.add(newRelation);
                                }
                            break;
                        }
                        case OUT :{
                            if (relation.get("dir") != null && relation.get("dir").toString().equalsIgnoreCase(Direction)){
                                Relation newRelation = new Relation((String)incomingMap.get(NODE_UNIQUE_ID), (String)relation.get("rel"), (String)relation.get("id"));
                                newRelation.setMetadata((Map<String,Object>) relation.get("relMetadata"));
                                newRelations.add(newRelation);
                            }
                            break;
                        }
                    }

                });
            }
        }
        return newRelations;
    }

    public static List<Relation> removeRelations(List<Relation>updatedRelations, List<Relation> removedRelations){
        List<Relation> tempRelation = new ArrayList();
        for (Relation rel :removedRelations ) {
            for(Relation relation :updatedRelations) {
                if(!rel.getStartNodeId().equals(relation.getStartNodeId())
                        || !rel.getEndNodeId().equals(relation.getEndNodeId())
                        || !rel.getRelationType().equals(relation.getRelationType())){
                    tempRelation.add(relation);
                }
            }
        }
        return tempRelation;
    }



    public static Response getResponse(Node node, String operationType) {
        Response response = null;
        switch (operationType) {
            case "CREATE": {
                response = controllerUtil.createDataNode(node);
                break;
            }
            case "UPDATE": {
                response = controllerUtil.updateNode(node);
                break;
            }
            case "DELETE": {
                response = controllerUtil.deleteNode(node);
            }
        }
        return response;
    }

    public static Map removeSystemProperties(Map metadataMap) {
        if(!metadataMap.isEmpty() && metadataMap != null){
            EnumSet.allOf(SystemProperties.class)
                    .forEach(key -> {
                        if (metadataMap.containsKey(key.name())) {
                            metadataMap.remove(key.name());
                        }
                    });
        }
        return metadataMap;
    }

    public static DefinitionDTO getDefinitionObject(Map message, List<MetadataDefinition> properties,
                                                    List<RelationDefinition> inRelations,
                                                    List<RelationDefinition> outRelations,
                                                    List<TagDefinition> systemTags,
                                                    Map<String,Object> metadata) {
            DefinitionDTO definitionDTO = new DefinitionDTO();
            definitionDTO.setIdentifier((String)message.get(NODE_UNIQUE_ID));
            definitionDTO.setObjectType((String)message.get(Object_Type));
            definitionDTO.setProperties(properties);
            definitionDTO.setInRelations(inRelations);
            definitionDTO.setOutRelations(outRelations);
            definitionDTO.setSystemTags(systemTags);
            definitionDTO.setMetadata(metadata);
            return definitionDTO;
    }

    public static List<MetadataDefinition> getDefinitionProperties(Map transactionDataProps){
        List<MetadataDefinition> metadataDefinitions = new ArrayList<>();
        if(transactionDataProps != null )
        if(transactionDataProps.get(SystemProperties.IL_INDEXABLE_METADATA_KEY.name()) != null){
            metadataDefinitions = getListData(SystemProperties.IL_INDEXABLE_METADATA_KEY.name(),transactionDataProps,"METADATA");
        }
        if(transactionDataProps.get(SystemProperties.IL_NON_INDEXABLE_METADATA_KEY.name()) != null){
            metadataDefinitions.addAll(getListData(SystemProperties.IL_NON_INDEXABLE_METADATA_KEY.name(),transactionDataProps,"METADATA"));
        }
        return metadataDefinitions;
    }

    public static List<RelationDefinition> getRelationDefinition(Map transactionDataProps, String direction){
        List<RelationDefinition> relationDefinitions = new ArrayList<>();
        if(transactionDataProps.get("IL_"+ direction +"_RELATIONS_KEY") != null){
            relationDefinitions = getListData("IL_"+ direction +"_RELATIONS_KEY",transactionDataProps,"RELATION");
        }
        return relationDefinitions;
    }

    public static List<TagDefinition> getSystemTags(Map transactionDataProps){
        List<TagDefinition> tagDefinitions = new ArrayList<>();
        if(transactionDataProps.get(SystemProperties.IL_SYSTEM_TAGS_KEY.name()) != null){
            tagDefinitions = getListData(SystemProperties.IL_SYSTEM_TAGS_KEY.name(),transactionDataProps,"TAG");
        }
        return tagDefinitions;
    }

    public static List getListData(String propName, Map transactionDataProps, String propType){
        List<Map<String,Object>> propMaps = null;
        try {
            propMaps = mapper.readValue(((Map)transactionDataProps.get(propName)).get("nv").toString(),new TypeReference<List<Map<String,Object>>>(){});
        } catch (IOException e) {
            e.printStackTrace();
        }
        List returnList = null;
        if(propMaps != null && !propMaps.isEmpty()){
            switch (propType){
                case "METADATA":{
                    List<MetadataDefinition> metadataDefinitions = new ArrayList<>();
                    for (Map<String,Object> propMap :propMaps) {
                    MetadataDefinition metadataDefinition = mapper.convertValue(propMap, MetadataDefinition.class);
                    metadataDefinitions.add(metadataDefinition);
                    }
                    returnList= metadataDefinitions;
                    break;
                }
                case "RELATION":{
                    List<RelationDefinition> relationDefinitions = new ArrayList<>();
                    for (Map<String,Object> propMap :propMaps) {
                        RelationDefinition relationDefinition = mapper.convertValue(propMap, RelationDefinition.class);
                        relationDefinitions.add(relationDefinition);
                    }
                    returnList = relationDefinitions;
                    break;
                }
                case "TAG":{
                    List<TagDefinition> tagDefinitions = new ArrayList<>();
                    for (Map<String,Object> propMap :propMaps) {
                        TagDefinition tagDefinition = mapper.convertValue(propMap, TagDefinition.class);
                        tagDefinitions.add(tagDefinition);
                    }
                    returnList = tagDefinitions;
                    break;
                }
            }
        }
    return returnList;
    }

}
