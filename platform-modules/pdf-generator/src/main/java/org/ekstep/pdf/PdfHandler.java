package org.ekstep.pdf;

import org.ekstep.common.exception.ServerException;
import org.ekstep.common.util.S3PropertyReader;
import org.ekstep.graph.dac.model.Node;
import org.ekstep.learning.common.enums.ContentErrorCodes;
import org.ekstep.learning.util.CloudStore;
import org.ekstep.telemetry.logger.TelemetryManager;

import java.io.File;

public class PdfHandler {
    private static final String CONTENT_FOLDER = "cloud_storage.content.folder";
    private static final String QUESTION_PAPER_FOLDER = "cloud_storage.question_paper.folder";
    /**
     * This method will generate a pdf and upload it based on the object type and upload the file
     * @param node
     * @return
     */
    public static String generateAndUploadPdf(Node node, String objectType) {
        switch (objectType) {
            case "Content": {
                File pdfFile = QuestionPaperGenerator.generatePdf(node);
                uploadFileToCloud(pdfFile, QUESTION_PAPER_FOLDER);
            }
        }
        //Generate PDF
        //Upload Pdf
        return null;
    }

    private static String uploadFileToCloud(File pdfFile, String objectFolderName) {
        try {
            String folder = S3PropertyReader.getProperty(CONTENT_FOLDER);
            folder = folder + "/" + S3PropertyReader.getProperty(objectFolderName);
            String[] urlArray = CloudStore.uploadFile(folder, pdfFile, true);
            return urlArray[1];
        } catch (Exception e) {
            TelemetryManager.error("Error while uploading the file.", e);
            throw new ServerException(ContentErrorCodes.ERR_CONTENT_UPLOAD_FILE.name(),
                    "Error while uploading the File.", e);
        }
    }
}
