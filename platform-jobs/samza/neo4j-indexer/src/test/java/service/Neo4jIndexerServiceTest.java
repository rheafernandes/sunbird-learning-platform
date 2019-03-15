package service;

import org.ekstep.jobs.samza.service.util.INeo4jIndexer;
import org.junit.*;

import static org.junit.Assert.*;
import java.util.Map;

public class Neo4jIndexerServiceTest {
    public Map incomingMapNoTD;
    public Map incomingMapEmptyTD;
    public Map incomingMapTD;
    public final Object messageWithNoTD = "{ets=1552367693312,channel=in.ekstep,mid=68b42e6b-7e30-40f8-a4e4-9512883bccbc,label=Pythagorus Theorum,nodeType=DATA_NODE,userId=ANONYMOUS,createdOn=2019-03-12T10:44:53.301+0530,objectType=Content,nodeUniqueId=do_11271699613881139211,requestId=null,operationType=CREATE,nodeGraphId=437275,graphId=domain}";
    public final Object messageWithEmptyTD = "{ets=1552367693312,channel=in.ekstep,transactionData=,mid=68b42e6b-7e30-40f8-a4e4-9512883bccbc,label=Pythagorus Theorum,nodeType=DATA_NODE,userId=ANONYMOUS,createdOn=2019-03-12T10:44:53.301+0530,objectType=Content,nodeUniqueId=do_11271699613881139211,requestId=null,operationType=CREATE,nodeGraphId=437275,graphId=domain}";
    public final Object messageWithTD = "{ets=1552367693312,channel=in.ekstep,transactionData={properties={ownershipType={ov=null,nv=[createdBy]},code={ov=null,nv=test.pdf.1},keywords={ov=null,nv=[colors,games]},channel={ov=null,nv=in.ekstep},language={ov=null,nv=[English]},mimeType={ov=null,nv=application/pdf},idealScreenSize={ov=null,nv=normal},createdOn={ov=null,nv=2019-03-12T10:44:52.696+0530},contentDisposition={ov=null,nv=inline},lastUpdatedOn={ov=null,nv=2019-03-12T10:44:52.696+0530},contentEncoding={ov=null,nv=identity},dialcodeRequired={ov=null,nv=No},contentType={ov=null,nv=Resource},audience={ov=null,nv=[Learner]},IL_SYS_NODE_TYPE={ov=null,nv=DATA_NODE},visibility={ov=null,nv=Default},os={ov=null,nv=[All]},mediaType={ov=null,nv=content},osId={ov=null,nv=org.ekstep.quiz.app},versionKey={ov=null,nv=1552367692696},idealScreenDensity={ov=null,nv=hdpi},framework={ov=null,nv=NCF},compatibilityLevel={ov=null,nv=1.0},IL_FUNC_OBJECT_TYPE={ov=null,nv=Content},name={ov=null,nv=Pythagorus Theorum},IL_UNIQUE_ID={ov=null,nv=do_11271699613881139211},resourceType={ov=null,nv=Read},status={ov=null,nv=Draft}}mid=68b42e6b-7e30-40f8-a4e4-9512883bccbc,label=Pythagorus Theorum,nodeType=DATA_NODE,userId=ANONYMOUS,createdOn=2019-03-12T10:44:53.301+0530,objectType=Content,nodeUniqueId=do_11271699613881139211,requestId=null,operationType=CREATE,nodeGraphId=437275,graphId=domain}";


    @Before
    public void setup() throws Exception {
        incomingMapTD = (Map<String, Object>) messageWithTD;
        incomingMapNoTD = (Map<String, Object>) messageWithNoTD;
        incomingMapEmptyTD = (Map<String, Object>) messageWithEmptyTD;
    }

    @After
    public void teardown() throws Exception {
        incomingMapEmptyTD = null;
        incomingMapNoTD = null;
        incomingMapTD = null;
    }

    @Test
    public void messageNullOrInvalidTest() {
        Map expectedMap = null;
        Map resultMap = INeo4jIndexer.prepareContentMap(this.incomingMapNoTD);
        assertEquals(expectedMap,resultMap);
    }

    @Test
    public void messageWithNoTransactionDataTest() {
        Map expectedMap = null;
        Map resultMap = INeo4jIndexer.prepareContentMap(this.incomingMapEmptyTD);
        assertEquals(expectedMap,resultMap);
    }

    @Test
    public void messageWithTransactionData(){
        Object expectedMessage = "{ownershipType=[createdBy],code=test.pdf.1,keywords=[colors,games],channel=in.ekstep,language=[English],mimeType=application/pdf,idealScreenSize=normal,createdOn=2019-03-12T10:44:52.696+0530,contentDisposition=inline,lastUpdatedOn=2019-03-12T10:44:52.696+0530,contentEncoding=identity,dialcodeRequired=No,contentType=Resource,audience[Learner],IL_SYS_NODE_TYPE=DATA_NODE,visibility=Default,os[All],mediaType=content,osId=org.ekstep.quiz.app,versionKey=1552367692696,idealScreenDensity=hdpi,framework=NCF,compatibilityLevel=1.0,IL_FUNC_OBJECT_TYPE=Content,name=Pythagorus Theorum,IL_UNIQUE_ID=do_11271699613881139211,resourceType=Read,status=Draft}";
        Map expectedMap = (Map<String, Object>) expectedMessage;
        System.out.println(expectedMap.get("keywords"));
        Map resultMap = INeo4jIndexer.prepareContentMap(this.incomingMapEmptyTD);
        assertEquals(expectedMap,resultMap);
    }
}
