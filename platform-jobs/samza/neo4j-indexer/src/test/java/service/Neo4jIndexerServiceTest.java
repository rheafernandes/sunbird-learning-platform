package service;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.ekstep.common.Platform;
import org.ekstep.graph.dac.model.Node;
import org.ekstep.graph.model.node.DefinitionDTO;
import org.ekstep.graph.model.node.MetadataDefinition;
import org.ekstep.graph.model.node.RelationDefinition;
import org.ekstep.graph.model.node.TagDefinition;
import org.ekstep.graph.service.common.DACConfigurationConstants;
import org.ekstep.jobs.samza.service.Neo4jIndexerService;
import org.ekstep.jobs.samza.service.util.INeo4jIndexer;
import org.ekstep.jobs.samza.task.Neo4jIndexerTask;
import org.ekstep.learning.util.ControllerUtil;
import org.junit.*;


import static org.junit.Assert.*;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jIndexerServiceTest {
    public Map incomingMapNoTD;
    public Map incomingMapEmptyTD;
    public Map incomingMapTD;
    public Map expectedMap;
    public Map incomingMapWithAddedINRelation;
    public Map incomingMapWithAddedOUTRelation;
    public Map incomingMapWithDeletedINRelation;
    public Map incomingMapWithDeletedOUTRelation;
    public Map incomingMapWithExceptionNode;
    public Map incomingDefinitionNodeMap;
    public Map transactionDataMap;
    public Neo4jIndexerService neo4jIndexerService;
    public Neo4jIndexerTask neo4jIndexerTask;
    public static ObjectMapper mapper = new ObjectMapper();

    public final String messageWithNoTD = "{\"ets\":1552367693312,\"channel\":\"in.ekstep\",\"mid\":\"68b42e6b-7e30-40f8-a4e4-9512883bccbc\",\"label\":\"Pythagorus Theorum\",\"nodeType\":\"DATA_NODE\",\"userId\":\"ANONYMOUS\",\"createdOn\":\"2019-03-12T10:44:53.301+0530\",\"objectType\":\"Content\",\"nodeUniqueId\":\"do_11271699613881139211\",\"requestId\":null,\"operationType\":\"CREATE\",\"nodeGraphId\":437275,\"graphId\":\"domain\"}";
    public final String messageWithEmptyTD = "{\"ets\":1552367693312,\"channel\":\"in.ekstep\", \"transactionData\":{},\"mid\":\"68b42e6b-7e30-40f8-a4e4-9512883bccbc\",\"label\":\"Pythagorus Theorum\",\"nodeType\":\"DATA_NODE\",\"userId\":\"ANONYMOUS\",\"createdOn\":\"2019-03-12T10:44:53.301+0530\",\"objectType\":\"Content\",\"nodeUniqueId\":\"do_11271699613881139211\",\"requestId\":null,\"operationType\":\"CREATE\",\"nodeGraphId\":437275,\"graphId\":\"domain\"}";
    public final String messageWithTD = "{\"ets\":1552367693312,\"channel\":\"in.ekstep\",\"transactionData\":{\"properties\":{\"ownershipType\":{\"ov\":null,\"nv\":\"[createdBy]\"},\"code\":{\"ov\":null,\"nv\":\"test.pdf.1\"},\"keywords\":{\"ov\":null,\"nv\":\"[colors,games]\"},\"channel\":{\"ov\":null,\"nv\":\"in.ekstep\"},\"language\":{\"ov\":null,\"nv\":\"[English]\"},\"mimeType\":{\"ov\":null,\"nv\":\"application/pdf\"},\"idealScreenSize\":{\"ov\":null,\"nv\":\"normal\"},\"createdOn\":{\"ov\":null,\"nv\":\"2019-03-12T10:44:52.696+0530\"},\"contentDisposition\":{\"ov\":null,\"nv\":\"inline\"},\"lastUpdatedOn\":{\"ov\":null,\"nv\":\"2019-03-12T10:44:52.696+0530\"},\"contentEncoding\":{\"ov\":null,\"nv\":\"identity\"},\"dialcodeRequired\":{\"ov\":null,\"nv\":\"No\"},\"contentType\":{\"ov\":null,\"nv\":\"Resource\"},\"audience\":{\"ov\":null,\"nv\":\"[Learner]\"},\"IL_SYS_NODE_TYPE\":{\"ov\":null,\"nv\":\"DATA_NODE\"},\"visibility\":{\"ov\":\"null\",\"nv\":\"Default\"},\"os\":{\"ov\":null,\"nv\":\"[All]\"},\"mediaType\":{\"ov\":null,\"nv\":\"content\"},\"osId\":{\"ov\":null,\"nv\":\"org.ekstep.quiz.app\"},\"versionKey\":{\"ov\":null,\"nv\":\"1552367692696\"},\"idealScreenDensity\":{\"ov\":null,\"nv\":\"hdpi\"},\"framework\":{\"ov\":null,\"nv\":\"NCF\"},\"compatibilityLevel\":{\"ov\":null,\"nv\":1.0},\"IL_FUNC_OBJECT_TYPE\":{\"ov\":null,\"nv\":\"Content\"},\"name\":{\"ov\":null,\"nv\":\"Pythagorus Theorum\"},\"IL_UNIQUE_ID\":{\"ov\":null,\"nv\":\"do_11271699613881139211\"},\"resourceType\":{\"ov\":null,\"nv\":\"Read\"},\"status\":{\"ov\":null,\"nv\":\"Draft\"}}},\"mid\":\"68b42e6b-7e30-40f8-a4e4-9512883bccbc\",\"label\":\"Pythagorus Theorum\",\"nodeType\":\"DATA_NODE\",\"userId\":\"ANONYMOUS\",\"createdOn\":\"2019-03-12T10:44:53.301+0530\",\"objectType\":\"Content\",\"nodeUniqueId\":\"do_11271699613881139211\",\"requestId\":null,\"operationType\":\"CREATE\",\"nodeGraphId\":437275,\"graphId\":\"domain\"}";
    public final String messageWithAddedINRelation ="{\"ets\":1554055552258,\"channel\":\"in.ekstep\",\"transactionData\":{\"removedTags\":[],\"addedRelations\":[{\"rel\":\"hasMember\",\"id\":\"do_11273082308291788816\",\"label\":\"Akshara Worksheet Grade 6 Item Set\",\"dir\":\"IN\",\"type\":\"ItemSet\",\"relMetadata\":{}}],\"removedRelations\":[],\"addedTags\":[],\"properties\":{}},\"mid\":\"6fb8994d-cd1a-460a-b43e-9d30e7bfe62d\",\"label\":\"G5Q5\",\"nodeType\":\"DATA_NODE\",\"userId\":\"ANONYMOUS\",\"createdOn\":\"2019-03-31T23:35:52.258+0530\",\"objectType\":\"AssessmentItem\",\"nodeUniqueId\":\"G5Q5\",\"requestId\":null,\"operationType\":\"UPDATE\",\"nodeGraphId\":693,\"graphId\":\"domain\"}";
    public final String messageWithAddedOUTRelation = "{\"ets\":1554181300238,\"channel\":\"in.ekstep\",\"transactionData\":{\"removedTags\":[],\"addedRelations\":[{\"rel\":\"hasSequenceMember\",\"id\":\"do_11272835122216140818\",\"label\":\"Test_Collection_TextBookUnit_01\",\"dir\":\"OUT\",\"type\":\"Content\",\"relMetadata\":{\"IL_SEQUENCE_INDEX\":1}}],\"removedRelations\":[],\"addedTags\":[],\"properties\":{}},\"mid\":\"31153609-e2eb-48f2-adb2-d73d27feb153\",\"label\":\"Maths-2\",\"nodeType\":\"DATA_NODE\",\"userId\":\"ANONYMOUS\",\"createdOn\":\"2019-04-02T10:31:40.238+0530\",\"objectType\":\"Content\",\"nodeUniqueId\":\"org.ekstep.mar28.textbook.test02\",\"requestId\":null,\"operationType\":\"UPDATE\",\"nodeGraphId\":437218,\"graphId\":\"domain\"}";
    public final String messageWithDeletedINRelation ="{\"ets\":1554181300236,\"channel\":null,\"transactionData\":{\"removedTags\":[],\"addedRelations\":[],\"removedRelations\":[{\"rel\":\"hasSequenceMember\",\"id\":\"org.ekstep.mar28.textbook.test02\",\"label\":\"Maths-2\",\"dir\":\"IN\",\"type\":\"Content\",\"relMetadata\":{\"IL_SEQUENCE_INDEX\":2}}],\"addedTags\":[],\"properties\":{}},\"mid\":\"702877d5-02e9-44fc-874d-ae16581ffde7\",\"label\":\"Test_Collection_TextBookUnit_02\",\"nodeType\":\"DATA_NODE\",\"userId\":\"ANONYMOUS\",\"createdOn\":\"2019-04-02T10:31:40.236+0530\",\"objectType\":\"Content\",\"nodeUniqueId\":\"do_11272835122225152019\",\"requestId\":null,\"operationType\":\"UPDATE\",\"nodeGraphId\":437226,\"graphId\":\"domain\"}";
    public final String messageWithDeletedOUTRelation ="{\"ets\":1554181300236,\"channel\":\"in.ekstep\",\"transactionData\":{\"removedTags\":[],\"addedRelations\":[],\"removedRelations\":[{\"rel\":\"hasSequenceMember\",\"id\":\"do_11272835122216140818\",\"label\":\"Test_Collection_TextBookUnit_01\",\"dir\":\"OUT\",\"type\":\"Content\",\"relMetadata\":{\"IL_SEQUENCE_INDEX\":1}}],\"addedTags\":[],\"properties\":{}},\"mid\":\"1ff3cd28-ffea-4080-bd73-ab8d931281c1\",\"label\":\"Maths-2\",\"nodeType\":\"DATA_NODE\",\"userId\":\"ANONYMOUS\",\"createdOn\":\"2019-04-02T10:31:40.236+0530\",\"objectType\":\"Content\",\"nodeUniqueId\":\"org.ekstep.mar28.textbook.test02\",\"requestId\":null,\"operationType\":\"UPDATE\",\"nodeGraphId\":437218,\"graphId\":\"domain\"}";
    public final String messageWithExceptionNode = "{\"ets\":1554188093007,\"channel\":\"in.ekstep\",\"transactionData\":{\"properties\":{\"ownershipType\":{\"ov\":null,\"nv\":[\"createdBy\"]},\"code\":{\"ov\":null,\"nv\":\"test.pdf.1\"},\"tags\":{\"ov\":null,\"nv\":\"ok\"},\"channel\":{\"ov\":null,\"nv\":\"in.ekstep\"},\"language\":{\"ov\":null,\"nv\":[\"English\"]},\"mimeType\":{\"ov\":null,\"nv\":\"application/pdf\"},\"idealScreenSize\":{\"ov\":null,\"nv\":\"normal\"},\"createdOn\":{\"ov\":null,\"nv\":\"2019-04-02T12:24:52.101+0530\"},\"contentDisposition\":{\"ov\":null,\"nv\":\"inline\"},\"lastUpdatedOn\":{\"ov\":null,\"nv\":\"2019-04-02T12:24:52.101+0530\"},\"contentEncoding\":{\"ov\":null,\"nv\":\"identity\"},\"dialcodeRequired\":{\"ov\":null,\"nv\":\"No\"},\"contentType\":{\"ov\":null,\"nv\":\"Resource\"},\"lastStatusChangedOn\":{\"ov\":null,\"nv\":\"2019-04-02T12:24:52.101+0530\"},\"audience\":{\"ov\":null,\"nv\":[\"Learner\"]},\"IL_SYS_NODE_TYPE\":{\"ov\":null,\"nv\":\"DATA_NODE\"},\"visibility\":{\"ov\":null,\"nv\":\"Default\"},\"os\":{\"ov\":null,\"nv\":[\"All\"]},\"mediaType\":{\"ov\":null,\"nv\":\"content\"},\"osId\":{\"ov\":null,\"nv\":\"org.ekstep.quiz.app\"},\"versionKey\":{\"ov\":null,\"nv\":\"1554188092101\"},\"idealScreenDensity\":{\"ov\":null,\"nv\":\"hdpi\"},\"framework\":{\"ov\":null,\"nv\":\"NCF\"},\"compatibilityLevel\":{\"ov\":null,\"nv\":1.0},\"IL_FUNC_OBJECT_TYPE\":{\"ov\":null,\"nv\":\"Content\"},\"name\":{\"ov\":null,\"nv\":\"Ad Calculus\"},\"IL_UNIQUE_ID\":{\"ov\":null,\"nv\":\"do_11273190885071257611\"},\"resourceType\":{\"ov\":null,\"nv\":\"Read\"},\"status\":{\"ov\":null,\"nv\":\"Draft\"}}},\"mid\":\"700e7bb0-e004-4e97-8a03-bb54217d8b20\",\"label\":\"Ad Calculus\",\"nodeType\":\"DATA_NODE\",\"userId\":\"ANONYMOUS\",\"createdOn\":\"2019-04-02T12:24:52.983+0530\",\"objectType\":\"Content\",\"nodeUniqueId\":\"do_11273190885071257611\",\"requestId\":null,\"operationType\":\"CREATE\",\"nodeGraphId\":437261,\"graphId\":\"domain\"}";
    public final String messageWithDefinitionNode = " {\"ets\":1554180672406,\"channel\":\"in.ekstep\",\"transactionData\":{\"properties\":{\"IL_INDEXABLE_METADATA_KEY\":{\"ov\":null,\"nv\":\"[{\\\"required\\\":true,\\\"dataType\\\":\\\"Text\\\",\\\"propertyName\\\":\\\"name\\\",\\\"title\\\":\\\"Name\\\",\\\"description\\\":\\\"Name of the category\\\",\\\"category\\\":\\\"General\\\",\\\"displayProperty\\\":\\\"Editable\\\",\\\"range\\\":null,\\\"defaultValue\\\":\\\"\\\",\\\"renderingHints\\\":\\\"{'inputType': 'text', 'order': 1}\\\",\\\"indexed\\\":true,\\\"draft\\\":false,\\\"rangeValidation\\\":true},{\\\"required\\\":false,\\\"dataType\\\":\\\"Text\\\",\\\"propertyName\\\":\\\"description\\\",\\\"title\\\":\\\"Name\\\",\\\"description\\\":\\\"description of the category\\\",\\\"category\\\":\\\"General\\\",\\\"displayProperty\\\":\\\"Editable\\\",\\\"range\\\":null,\\\"defaultValue\\\":\\\"\\\",\\\"renderingHints\\\":\\\"{'inputType': 'text', 'order': 3}\\\",\\\"indexed\\\":true,\\\"draft\\\":false,\\\"rangeValidation\\\":true},{\\\"required\\\":true,\\\"dataType\\\":\\\"Text\\\",\\\"propertyName\\\":\\\"code\\\",\\\"title\\\":\\\"Code\\\",\\\"description\\\":\\\"Unique code for the category\\\",\\\"category\\\":\\\"General\\\",\\\"displayProperty\\\":\\\"Editable\\\",\\\"range\\\":null,\\\"defaultValue\\\":\\\"\\\",\\\"renderingHints\\\":\\\"{'inputType': 'text', 'order': 4}\\\",\\\"indexed\\\":true,\\\"draft\\\":false,\\\"rangeValidation\\\":true},{\\\"required\\\":false,\\\"dataType\\\":\\\"JSON\\\",\\\"propertyName\\\":\\\"translations\\\",\\\"title\\\":\\\"Translations\\\",\\\"description\\\":\\\"Translations for the category\\\",\\\"category\\\":\\\"General\\\",\\\"displayProperty\\\":\\\"Editable\\\",\\\"range\\\":null,\\\"defaultValue\\\":\\\"\\\",\\\"renderingHints\\\":\\\"{'inputType': 'object', 'order': 5}\\\",\\\"indexed\\\":true,\\\"draft\\\":false,\\\"rangeValidation\\\":true},{\\\"required\\\":false,\\\"dataType\\\":\\\"json\\\",\\\"propertyName\\\":\\\"defaultTerm\\\",\\\"title\\\":\\\"defaultTerm\\\",\\\"description\\\":\\\"defaultTerm associated with the category\\\",\\\"category\\\":\\\"General\\\",\\\"displayProperty\\\":\\\"Editable\\\",\\\"range\\\":null,\\\"defaultValue\\\":\\\"\\\",\\\"renderingHints\\\":\\\"{'inputType': 'text', 'order': 6}\\\",\\\"indexed\\\":true,\\\"draft\\\":false,\\\"rangeValidation\\\":true},{\\\"required\\\":false,\\\"dataType\\\":\\\"Select\\\",\\\"propertyName\\\":\\\"status\\\",\\\"title\\\":\\\"Status\\\",\\\"description\\\":\\\"Status of the category\\\",\\\"category\\\":\\\"general\\\",\\\"displayProperty\\\":\\\"Editable\\\",\\\"range\\\":[\\\"Draft\\\",\\\"Live\\\",\\\"Retired\\\"],\\\"defaultValue\\\":\\\"Live\\\",\\\"renderingHints\\\":\\\"{'inputType': 'select', 'order': 7}\\\",\\\"indexed\\\":true,\\\"draft\\\":false,\\\"rangeValidation\\\":true}]\"},\"IL_SYS_NODE_TYPE\":{\"ov\":null,\"nv\":\"DEFINITION_NODE\"},\"IL_SYSTEM_TAGS_KEY\":{\"ov\":null,\"nv\":\"[{\\\"name\\\":\\\"num\\\",\\\"description\\\":\\\"wtf\\\"}]\"},\"IL_REQUIRED_PROPERTIES\":{\"ov\":null,\"nv\":[\"name\",\"code\"]},\"channel\":{\"ov\":null,\"nv\":\"in.ekstep\"},\"createdOn\":{\"ov\":null,\"nv\":\"2019-04-02T10:21:11.642+0530\"},\"ttl\":{\"ov\":null,\"nv\":11},\"versionKey\":{\"ov\":null,\"nv\":\"1554180671642\"},\"IL_FUNC_OBJECT_TYPE\":{\"ov\":null,\"nv\":\"Test-10\"},\"IL_NON_INDEXABLE_METADATA_KEY\":{\"ov\":null,\"nv\":\"[{\\\"required\\\":false,\\\"dataType\\\":\\\"Text\\\",\\\"propertyName\\\":\\\"createdBy\\\",\\\"title\\\":\\\"Created By\\\",\\\"description\\\":\\\"\\\",\\\"category\\\":\\\"audit\\\",\\\"displayProperty\\\":\\\"Editable\\\",\\\"range\\\":[],\\\"defaultValue\\\":\\\"\\\",\\\"renderingHints\\\":\\\"{ 'inputType': 'text',  'order': 8}\\\",\\\"indexed\\\":false,\\\"draft\\\":false,\\\"rangeValidation\\\":true},{\\\"required\\\":false,\\\"dataType\\\":\\\"Date\\\",\\\"propertyName\\\":\\\"createdOn\\\",\\\"title\\\":\\\"Created On\\\",\\\"description\\\":\\\"\\\",\\\"category\\\":\\\"audit\\\",\\\"displayProperty\\\":\\\"Readonly\\\",\\\"range\\\":[],\\\"defaultValue\\\":\\\"\\\",\\\"renderingHints\\\":\\\"{ inputType': 'date', 'order': 9 }\\\",\\\"indexed\\\":false,\\\"draft\\\":false,\\\"rangeValidation\\\":true},{\\\"required\\\":false,\\\"dataType\\\":\\\"Text\\\",\\\"propertyName\\\":\\\"lastUpdatedBy\\\",\\\"title\\\":\\\"Last Updated By\\\",\\\"description\\\":\\\"\\\",\\\"category\\\":\\\"audit\\\",\\\"displayProperty\\\":\\\"Editable\\\",\\\"range\\\":[],\\\"defaultValue\\\":\\\"\\\",\\\"renderingHints\\\":\\\"{ 'inputType': 'text',  'order': 10 }\\\",\\\"indexed\\\":false,\\\"draft\\\":false,\\\"rangeValidation\\\":true},{\\\"required\\\":false,\\\"dataType\\\":\\\"Date\\\",\\\"propertyName\\\":\\\"lastUpdatedOn\\\",\\\"title\\\":\\\"Last Updated On\\\",\\\"description\\\":\\\"\\\",\\\"category\\\":\\\"audit\\\",\\\"displayProperty\\\":\\\"Readonly\\\",\\\"range\\\":[],\\\"defaultValue\\\":\\\"\\\",\\\"renderingHints\\\":\\\"{ inputType': 'date', 'order': 11 }\\\",\\\"indexed\\\":false,\\\"draft\\\":false,\\\"rangeValidation\\\":true}]\"},\"IL_IN_RELATIONS_KEY\":{\"ov\":null,\"nv\":\"[{\\\"relationName\\\":\\\"hasSequenceMember\\\",\\\"objectTypes\\\":[\\\"Framework\\\"],\\\"title\\\":\\\"frameworks\\\",\\\"description\\\":\\\"framework instance which has sequence relationship with this categoryInstance\\\",\\\"required\\\":false,\\\"renderingHints\\\":\\\"{ 'order': 14 }\\\"},{\\\"relationName\\\":\\\"hasSequenceMember\\\",\\\"objectTypes\\\":[\\\"Channel\\\"],\\\"title\\\":\\\"channels\\\",\\\"description\\\":\\\"channel instance which has sequence relationship with this categoryInstance\\\",\\\"required\\\":false,\\\"renderingHints\\\":\\\"{ 'order': 15 }\\\"}]\"},\"limit\":{\"ov\":null,\"nv\":50},\"lastUpdatedOn\":{\"ov\":null,\"nv\":\"2019-04-02T10:21:11.642+0530\"},\"fields\":{\"ov\":null,\"nv\":[\"identifier\",\"name\",\"code\",\"description\",\"status\",\"translations\"]},\"IL_UNIQUE_ID\":{\"ov\":null,\"nv\":\"DEFINITION_NODE_Test-10\"},\"IL_OUT_RELATIONS_KEY\":{\"ov\":null,\"nv\":\"[{\\\"relationName\\\":\\\"hasSequenceMember\\\",\\\"objectTypes\\\":[\\\"Term\\\"],\\\"title\\\":\\\"terms\\\",\\\"description\\\":\\\"term instance which has sequence relationship with this categoryInstance\\\",\\\"required\\\":false,\\\"renderingHints\\\":\\\"{ 'order': 16 }\\\"},{\\\"relationName\\\":\\\"hasSequenceMember\\\",\\\"objectTypes\\\":[\\\"Domain\\\"],\\\"title\\\":\\\"domains\\\",\\\"description\\\":\\\"Domain which has sequence relationship with this categoryInstance\\\",\\\"required\\\":false,\\\"renderingHints\\\":\\\"{ 'order': 17 }\\\"}]\"}}},\"mid\":\"976ad528-e0e4-4e2c-ab99-4dcc47076390\",\"label\":\"\",\"nodeType\":\"DEFINITION_NODE\",\"userId\":\"ANONYMOUS\",\"createdOn\":\"2019-04-02T10:21:12.386+0530\",\"objectType\":\"Test-10\",\"nodeUniqueId\":\"DEFINITION_NODE_Test-10\",\"requestId\":null,\"operationType\":\"CREATE\",\"nodeGraphId\":437319,\"graphId\":\"domain\"}";
    Object expectedMessage = "{\"ownershipType\":\"[createdBy]\",\"code\":\"test.pdf.1\",\"keywords\":\"[colors,games]\",\"channel\":\"in.ekstep\",\"language\":\"[English]\",\"mimeType\":\"application/pdf\",\"idealScreenSize\":\"normal\",\"createdOn\":\"2019-03-12T10:44:52.696+0530\",\"contentDisposition\":\"inline\",\"lastUpdatedOn\":\"2019-03-12T10:44:52.696+0530\",\"contentEncoding\":\"identity\",\"dialcodeRequired\":\"No\",\"contentType\":\"Resource\",\"audience\":\"[Learner]\",\"visibility\":\"Default\",\"os\":\"[All]\",\"mediaType\":\"content\",\"osId\":\"org.ekstep.quiz.app\",\"versionKey\":1552367692696,\"idealScreenDensity\":\"hdpi\",\"framework\":\"NCF\",\"compatibilityLevel\":1.0,\"name\":\"Pythagorus Theorum\",\"identifier\":\"do_11271699613881139211\",\"resourceType\":\"Read\",\"status\":\"Draft\"}";

    ControllerUtil controllerUtil = new ControllerUtil();

    @Before
    public void setup() throws Exception{
        controllerUtil = new ControllerUtil();
        incomingMapTD = getMap(messageWithTD);
        incomingMapNoTD = getMap(messageWithNoTD);
        incomingMapEmptyTD = getMap(messageWithEmptyTD);
        expectedMap= getMap((String)expectedMessage);
        incomingMapWithAddedINRelation = getMap(messageWithAddedINRelation);
        incomingMapWithAddedOUTRelation = getMap(messageWithAddedOUTRelation);
        incomingMapWithDeletedINRelation = getMap(messageWithDeletedINRelation);
        incomingMapWithDeletedOUTRelation = getMap(messageWithDeletedOUTRelation);
        incomingMapWithExceptionNode = getMap(messageWithExceptionNode);
        incomingDefinitionNodeMap = getMap(messageWithDefinitionNode);
        transactionDataMap =  (Map<String, Object>) ((Map) incomingDefinitionNodeMap.get("transactionData")).get("properties");
    }

    @After
    public void teardown() throws Exception {
        incomingMapEmptyTD = null;
        incomingMapNoTD = null;
        incomingMapTD = null;
        controllerUtil = null;
    }

    @Test
    public void messageNullOrInvalidTest() throws Exception {
        Map expectedMap= null;
        assertEquals(expectedMap,INeo4jIndexer.prepareContentMap(incomingMapNoTD));
    }

    @Test
    public void messageWithNoTransactionDataTest() {
        Map expectedMap = new HashMap();
        expectedMap.put("versionKey", Platform.config.getString(DACConfigurationConstants.PASSPORT_KEY_BASE_PROPERTY));
        assertEquals(expectedMap,INeo4jIndexer.prepareContentMap(incomingMapEmptyTD));
        assertEquals(expectedMap.size(),INeo4jIndexer.prepareContentMap(incomingMapEmptyTD).size());
    }

    @Test
    public void messageWithTransactionDataTest(){
        expectedMap.put("versionKey", Platform.config.getString(DACConfigurationConstants.PASSPORT_KEY_BASE_PROPERTY));
        Map resultMap = INeo4jIndexer.prepareContentMap(this.incomingMapTD);
        assertEquals(expectedMap,resultMap);
    }

    @Test
    public void getNodeWithAddedOUTRelationsNoTDTest(){
        DefinitionDTO definitionDTO= controllerUtil.getDefinition((String)incomingMapWithAddedOUTRelation.get("graphId"),(String)incomingMapWithAddedOUTRelation.get("objectType"));
        Map expectedMap = new HashMap();
        Node existingNode = controllerUtil.getNode("domain", (String) incomingMapWithAddedINRelation.get("nodeUniqueId"));
        assertEquals(incomingMapWithAddedOUTRelation.get("operationType"),"UPDATE");
        expectedMap.put("versionKey", Platform.config.getString(DACConfigurationConstants.PASSPORT_KEY_BASE_PROPERTY));
        Node resultNode = INeo4jIndexer.getNode(incomingMapWithAddedOUTRelation,expectedMap,definitionDTO,existingNode);
        assertEquals(resultNode.getObjectType(),incomingMapWithAddedOUTRelation.get("objectType"));
        assertEquals(expectedMap.size(),resultNode.getMetadata().size());
        assertEquals(resultNode.getInRelations(), existingNode.getInRelations());
        assertEquals(resultNode.getOutRelations().size(), existingNode.getOutRelations().size()+1);
    }
    @Test
    public void getNodeWithAddedINRelationsNoTDTest(){
        DefinitionDTO definitionDTO= controllerUtil.getDefinition((String)incomingMapWithAddedINRelation.get("graphId"),(String)incomingMapWithAddedINRelation.get("objectType"));
        Map expectedMap = new HashMap();
        Node existingNode = controllerUtil.getNode("domain", (String) incomingMapWithAddedOUTRelation.get("nodeUniqueId"));
        assertEquals(incomingMapWithAddedINRelation.get("operationType"),"UPDATE");
        expectedMap.put("versionKey", Platform.config.getString(DACConfigurationConstants.PASSPORT_KEY_BASE_PROPERTY));
        Node resultNode = INeo4jIndexer.getNode(incomingMapWithAddedINRelation,expectedMap,definitionDTO,existingNode);
        assertEquals(resultNode.getObjectType(),incomingMapWithAddedINRelation.get("objectType"));
        assertEquals(expectedMap.size(),resultNode.getMetadata().size());
        assertEquals(resultNode.getInRelations().size(), existingNode.getInRelations().size()+1);
        assertEquals(resultNode.getOutRelations(), existingNode.getOutRelations());
    }

    @Test
    public void getNodeWithRemovedINRelationsNoTDTest(){
        DefinitionDTO definitionDTO= controllerUtil.getDefinition((String)incomingMapWithDeletedINRelation.get("graphId"),(String)incomingMapWithDeletedINRelation.get("objectType"));
        Map expectedMap = new HashMap();
        Node existingNode = controllerUtil.getNode("domain", (String) incomingMapWithDeletedINRelation.get("nodeUniqueId"));
        assertEquals(incomingMapWithDeletedINRelation.get("operationType"),"UPDATE");
        expectedMap.put("versionKey", Platform.config.getString(DACConfigurationConstants.PASSPORT_KEY_BASE_PROPERTY));
        Node resultNode = INeo4jIndexer.getNode(incomingMapWithDeletedINRelation,expectedMap,definitionDTO,existingNode);
        assertEquals(resultNode.getObjectType(),incomingMapWithDeletedINRelation.get("objectType"));
        assertEquals(expectedMap.size(),resultNode.getMetadata().size());
        assertEquals(resultNode.getInRelations().size(), existingNode.getInRelations().size()-1);
        assertEquals(resultNode.getOutRelations(), existingNode.getOutRelations());

    }

    @Test
    public void getNodeWithRemovedOUTRelationsNoTDTest(){
        DefinitionDTO definitionDTO= controllerUtil.getDefinition((String)incomingMapWithDeletedOUTRelation.get("graphId"),(String)incomingMapWithDeletedINRelation.get("objectType"));
        Map expectedMap = new HashMap();
        assertEquals(incomingMapWithDeletedOUTRelation.get("operationType"),"UPDATE");
        Node existingNode = controllerUtil.getNode("domain", (String) incomingMapWithDeletedOUTRelation.get("nodeUniqueId"));
        expectedMap.put("versionKey", Platform.config.getString(DACConfigurationConstants.PASSPORT_KEY_BASE_PROPERTY));
        Node resultNode = INeo4jIndexer.getNode(incomingMapWithDeletedOUTRelation,expectedMap,definitionDTO,existingNode);
        assertEquals(resultNode.getObjectType(),incomingMapWithDeletedOUTRelation.get("objectType"));
        assertEquals(expectedMap.size(),resultNode.getMetadata().size());
        assertEquals(resultNode.getInRelations(), existingNode.getInRelations());
        assertEquals(resultNode.getOutRelations().size(), existingNode.getOutRelations().size()-1);
    }

    @Test
    public void getNodeWithException() {
        DefinitionDTO definitionDTO= controllerUtil.getDefinition((String)incomingMapWithDeletedOUTRelation.get("graphId"),(String)incomingMapWithDeletedINRelation.get("objectType"));
        assertEquals(incomingMapWithExceptionNode.get("operationType"),"CREATE");
        Map expectedMap = INeo4jIndexer.prepareContentMap(incomingMapWithExceptionNode);
        Node resultNode = INeo4jIndexer.getNode(incomingMapWithExceptionNode,expectedMap,definitionDTO,null);
        assertEquals(resultNode,null);

    }

    @Test
    public void getNodeNoExistingNodeTest(){
        DefinitionDTO definitionDTO= controllerUtil.getDefinition((String)incomingMapTD.get("graphId"),(String)incomingMapTD.get("objectType"));
        Node resultNode = INeo4jIndexer.getNode(incomingMapTD, expectedMap, definitionDTO, null );
        assertEquals(resultNode.getIdentifier(),incomingMapTD.get("nodeUniqueId"));
        expectedMap.remove("identifier");
        assertEquals(resultNode.getMetadata(),expectedMap);
        assertEquals(resultNode.getObjectType(),incomingMapTD.get("objectType"));
    }

    @Test
    public void getNodeWithExistingNodeTest(){


    }

    @Test
    public void getDefinitionNode(){
        List<MetadataDefinition> properties = INeo4jIndexer.getDefinitionProperties(transactionDataMap);
        List<RelationDefinition> inRelations = INeo4jIndexer.getRelationDefinition(transactionDataMap,"IN");
        List<RelationDefinition> outRelations = INeo4jIndexer.getRelationDefinition(transactionDataMap,"OUT");
        List<TagDefinition> systemTags = INeo4jIndexer.getSystemTags(transactionDataMap);
        Map<String,Object> metadata = INeo4jIndexer.prepareContentMap(incomingDefinitionNodeMap);
        metadata.remove("versionKey");
        DefinitionDTO definitionDTO = INeo4jIndexer.getDefinitionObject(incomingDefinitionNodeMap,properties,inRelations,outRelations,systemTags,metadata);
        assertEquals(definitionDTO.getMetadata(),metadata);
    }

    public static Map<String, Object> getMap(String message) {
        Map map = null;
        try {
            map = mapper.readValue(message, new TypeReference<Map<String,Object>>(){});
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }
}
