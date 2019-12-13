package org.ekstep.question;

import org.ekstep.question.IQuestionHandler;
import org.ekstep.question.impl.McqQuestionHandler;

public class QuestionHandlerFactory {

    private static IQuestionHandler mcqQuestionHandler = new McqQuestionHandler();

    public static IQuestionHandler getHandler(String questionType) {
        IQuestionHandler manager = null;
        switch (questionType) {
            case "mcq": manager = mcqQuestionHandler;
                break;
            case "sa":
                break;
            case "vsa":
                break;
            case "la":
                break;
            default: break;
        }
        return manager;
    }
}


