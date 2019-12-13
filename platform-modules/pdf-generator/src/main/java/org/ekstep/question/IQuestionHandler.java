package org.ekstep.question;

import org.ekstep.graph.dac.model.Node;

import java.io.File;
import java.util.List;
import java.util.Map;

public interface IQuestionHandler {

    Map<String, Object> populateQuestions(Map<String, Object> bodyMap);

    Map<String, Object> populateOptions(Map<String, Object> bodyMap);

    Map<String, Object> populateAnswers(Map<String, Object> bodyMap);

}
