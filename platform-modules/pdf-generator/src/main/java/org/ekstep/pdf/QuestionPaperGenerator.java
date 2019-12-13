package org.ekstep.pdf;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itextpdf.text.Document;
import com.itextpdf.text.pdf.PdfWriter;
import com.itextpdf.tool.xml.XMLWorkerHelper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.ekstep.assessment.store.AssessmentStore;
import org.ekstep.common.dto.Response;
import org.ekstep.graph.dac.model.Node;
import org.ekstep.graph.dac.model.Relation;
import org.ekstep.learning.util.ControllerUtil;
import org.ekstep.question.IQuestionHandler;
import org.ekstep.question.QuestionHandlerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QuestionPaperGenerator {
    private static ControllerUtil controllerUtil = new ControllerUtil();
    private static AssessmentStore assessmentStore = new AssessmentStore();
    private static final String ASSESSMENT_OBJECT_TYPE = "AssessmentItem";
    private static final String TAXONOMY_ID = "domain";
    private static final String NODE_LIST = "node_list";
    private static final String BODY = "body";
    private static final List<String> externalPropsToFetch = Arrays.asList(BODY);
    private static final String TYPE = "type";

    private static final ObjectMapper mapper = new ObjectMapper();

    public static File generatePdf(Node node) {
        List<String> questionIds = fetchChildIds(node);
        if(CollectionUtils.isNotEmpty(questionIds)) {
            Map<String, Object> assessmentData = getAssessmentDataMap(questionIds);
            Map<String, Object> htmlData = populateAssessmentData(assessmentData);
            String htmlString = generateHtmlString(null, htmlData);
            if(StringUtils.isNotEmpty(htmlString) && StringUtils.isNotBlank(htmlString)) {
                return convertHtmlFileToPdf(htmlString);
            } else
                return null;
        }
        return null;
    }

    private static List<String> fetchChildIds(Node node) {
        return node.getOutRelations().stream()
                .filter(relation -> StringUtils.equalsIgnoreCase(ASSESSMENT_OBJECT_TYPE, relation.getEndNodeObjectType()))
                .map(Relation::getEndNodeId)
                .collect(Collectors.toList());
    }

    private static Map<String, Object> getAssessmentDataMap(List<String> identifiers) {
        Map<String, Object> nodeMap = getMetadataFromNeo4j(identifiers);
        Map<String, Object> cassandraMap = getExternalPropsData(identifiers);
        if(MapUtils.isNotEmpty(nodeMap) && MapUtils.isNotEmpty(nodeMap)) {
            Map<String, Object> assessmentMap = new HashMap<>(cassandraMap);
            assessmentMap.forEach((key, value) -> nodeMap.merge(key, value, (v1, v2) -> ((Map<String, Object>) cassandraMap.get(key)).put(TYPE, v2)));
            return assessmentMap;
        }
        return new HashMap<>();
    }

    private static Map<String, Object> getMetadataFromNeo4j(List<String> identifiers) {
        Response response = controllerUtil.getDataNodes(TAXONOMY_ID, identifiers);
        List<Node> nodes = (List<Node>) response.get(NODE_LIST);
        if (CollectionUtils.isNotEmpty(nodes)) {
            return nodes.stream().collect(Collectors.toMap(node -> node.getIdentifier(), node -> (String) ((Node) node).getMetadata().get(TYPE)));
        }
        return new HashMap<>();
    }

    private static Map<String, Object> getExternalPropsData(List<String> identifiers) {
        return assessmentStore.getItems(identifiers, externalPropsToFetch);
    }


    /**
     * This will return a Map with id and Map<String, Object> containing data required to be populated in html template</String,>
     *
     * @param assessmentMap
     * @return
     */
    private static Map<String, Object> populateAssessmentData(Map<String, Object> assessmentMap) {
        Map<String, Object> assessmentHtmlMap = new HashMap<>();
        assessmentMap.forEach((key, value) -> populateData(assessmentHtmlMap, key, value));
        return assessmentHtmlMap;
    }

    private static void populateData(Map<String, Object> assessmentHtmlMap, String key, Object value) {
        Map<String, Object> valueMap = (Map<String, Object>) value;
        IQuestionHandler handler = QuestionHandlerFactory.getHandler((String) (valueMap.get(TYPE)));
        try {
            Map<String, Object> bodyMap = mapper.readValue((String) valueMap.get(BODY), new TypeReference<Map<String, String>>() {});
            Map<String, Object> htmlDataMap = new HashMap<String, Object>() {{
                put("questions", handler.populateQuestions(bodyMap));
                put("answers", handler.populateAnswers(bodyMap));
                put("options", handler.populateOptions(bodyMap));
            }};
            assessmentHtmlMap.put(key, htmlDataMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static String generateHtmlString(File htmlTemp, Map<String, Object> assessmentMap) {
        String htmlTemplate = "      <div class=\"questions\">\n" +
                "            q_placeholder\n" +
                "      </div>\n" +
                "      <div class=\"answers\" style=\"page-break-before: always\">\n" +
                "            a_placeholder\n" +
                "      </div>\n";
        StringBuilder questionString = new StringBuilder();
        assessmentMap.entrySet().forEach(question -> {
            questionString.append("<div class=\"question\">\n")
                    .append(((Map<String, String>) ((Map<String, Object>) question.getValue()).get("question")).get("text"))
                    .append("<img src=\"" + ((Map<String, Object>) ((Map<String, Object>) question.getValue()).get("question")).get("image") + "\" alt = \"\"/>\n")
                    .append("<div class=\"row\">\n");
            ((List<Map<String, Object>>) ((Map<String, Object>) question.getValue()).get("options")).forEach(option -> {
                questionString.append("<div class=\"column\">\n").append(option.get("text")).append("<img src=\"" + option.get("image") + "\" alt = \"\"/>\n").append("</div>\n");
            });
            questionString.append("</div>\n");
            questionString.append("</div>\n");
        });
        htmlTemplate = htmlTemplate.replace("q_placeholder", questionString.toString());
        StringBuilder answersString = new StringBuilder();
        assessmentMap.keySet().forEach(key -> {
            answersString.append("<div class=\"answer\">")
                    .append(((Map<String, String>) ((Map<String, Object>) assessmentMap.get(key)).get("question")).get("text"))
                    .append("<img src=\"" + ((Map<String, Object>) ((Map<String, Object>) assessmentMap.get(key)).get("question")).get("image") + "\" alt = \"\"/>\n")
                    .append("<p>" + ((Map<String, Object>) assessmentMap.get(key)).get("text") + "</p>\n")
                    .append("<img src=\"" + ((Map<String, Object>) assessmentMap.get(key)).get("image") + " \" alt=\"\"/>\n")
                    .append("</div>\n");
        });
        htmlTemplate = htmlTemplate.replace("a_placeholder", answersString.toString());
        return htmlTemplate;
    }

    private static File convertHtmlFileToPdf(String htmlString) {
        File file = new File("/data/tmp/assessments/" + getPdfFileName());
        try {
            OutputStream os = new FileOutputStream(file);
            Document document = new Document();
            PdfWriter writer = PdfWriter.getInstance(document, os);
            document.open();
            InputStream is = new ByteArrayInputStream(htmlString.getBytes());
            XMLWorkerHelper.getInstance().parseXHtml(writer, document, is);
            document.close();
            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return file;
    }

    private static String getPdfFileName() {
        return "pdf_" + System.currentTimeMillis();
    }
}
