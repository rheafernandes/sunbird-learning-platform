package org.ekstep.content.mimetype.mgr.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.ekstep.common.dto.Response;
import org.ekstep.common.exception.ClientException;
import org.ekstep.common.exception.ServerException;
import org.ekstep.content.common.ContentOperations;
import org.ekstep.content.mimetype.mgr.IMimeTypeManager;
import org.ekstep.content.pipeline.initializer.InitializePipeline;
import org.ekstep.content.util.AsyncContentOperationUtil;
import org.ekstep.content.validator.ContentValidator;
import org.ekstep.graph.dac.model.Node;
import org.ekstep.learning.common.enums.ContentAPIParams;
import org.ekstep.telemetry.logger.TelemetryManager;


/**
 * The Class DocumentMimeTypeManager is a implementation of IMimeTypeManager for
 * Mime-Type as <code>application/pdf</code>, <code>application/msword</code>
 * for Content creation.
 * 
 * @author Rashmi
 * 
 * @see IMimeTypeManager
 */
public class DocumentMimeTypeManager extends BaseMimeTypeManager implements IMimeTypeManager {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.ekstep.taxonomy.mgr.IMimeTypeManager#upload(org.ekstep.graph.dac.model.
	 * Node, java.io.File, java.lang.String)
	 */
	@Override
	public Response upload(String contentId, Node node, File uploadedFile, boolean isAsync) {
		TelemetryManager.log("Uploaded File: " + uploadedFile.getName());
		TelemetryManager.log("Calling Upload Content For Node ID: " + node.getIdentifier());
		File file = null;
		String tempFilePath = "";
		try {
			String mimeType = (String)node.getMetadata().get("mimeType");
			if(StringUtils.equalsIgnoreCase(mimeType, "application/epub") && StringUtils.endsWith(uploadedFile.getName(), ".epub")) {
				tempFilePath = getTempDirectoryPath(contentId) + "index.epub";
				file = new File(tempFilePath);
				try {
					FileUtils.moveFile(uploadedFile,file);
				} catch (IOException e) {
					e.printStackTrace();
				}
				return uploadContentArtifact(contentId, node, file);
			}
			return uploadContentArtifact(contentId, node, uploadedFile);
		} finally {
			deleteTempDirectory(contentId);
		}
	}
	
	@Override
	public Response upload(String contentId, Node node, String fileUrl) {
		node.getMetadata().put(ContentAPIParams.artifactUrl.name(), fileUrl);
		return updateContentNode(contentId, node, fileUrl);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.ekstep.taxonomy.mgr.IMimeTypeManager#publish(org.ekstep.graph.dac.model
	 * .Node)
	 */
	@Override
	public Response publish(String contentId, Node node, boolean isAsync) {
		Response response = new Response();
		TelemetryManager.log("Preparing the Parameter Map for Initializing the Pipeline for Node Id: " + contentId);
		InitializePipeline pipeline = new InitializePipeline(getBasePath(contentId), contentId);
		Map<String, Object> parameterMap = new HashMap<String, Object>();
		parameterMap.put(ContentAPIParams.node.name(), node);
		parameterMap.put(ContentAPIParams.ecmlType.name(), false);

		parameterMap.put(ContentAPIParams.isPublishOperation.name(), true);

		TelemetryManager.log("Calling the 'Review' Initializer for Node Id: " + contentId);
		response = pipeline.init(ContentAPIParams.review.name(), parameterMap);
		TelemetryManager.log("Review Operation Finished Successfully for Node ID: " + contentId);

		if (BooleanUtils.isTrue(isAsync)) {
			AsyncContentOperationUtil.makeAsyncOperation(ContentOperations.PUBLISH, contentId, parameterMap);
			TelemetryManager.log("Publish Operation Started Successfully in 'Async Mode' for Node Id: " + contentId);

			response.put(ContentAPIParams.publishStatus.name(),
					"Publish Operation for Content Id '" + contentId + "' Started Successfully!");
		} else {
			TelemetryManager.log("Publish Operation Started Successfully in 'Sync Mode' for Node Id: " + contentId);
			response = pipeline.init(ContentAPIParams.publish.name(), parameterMap);
		}
		return response;
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see org.ekstep.taxonomy.mgr.IMimeTypeManager#review(org.ekstep.graph.dac.model.
	 *      Node, java.io.File, java.lang.String)
	 */
	@Override
	public Response review(String contentId, Node node, boolean isAsync) {

		TelemetryManager.log("Preparing the Parameter Map for Initializing the Pipeline For Node ID: " + contentId);
		InitializePipeline pipeline = new InitializePipeline(getBasePath(contentId), contentId);
		Map<String, Object> parameterMap = new HashMap<String, Object>();
		parameterMap.put(ContentAPIParams.node.name(), node);
		parameterMap.put(ContentAPIParams.ecmlType.name(), true);

		TelemetryManager.log("Calling the 'Review' Initializer for Node ID: " + contentId);
		return pipeline.init(ContentAPIParams.review.name(), parameterMap);
	}

	/**
	 * The method uploadContentArtifact uploads the content artifacts to s3 and
	 * set the s3 url as artifactUrl
	 * 
	 * @param node
	 * @param uploadedFile
	 * @return
	 */
	public Response uploadContentArtifact(String contentId, Node node, File uploadedFile) {
		try {
			Response response = new Response();
			TelemetryManager.log("Verifying the MimeTypes.");
			String mimeType = (String) node.getMetadata().get("mimeType");
			ContentValidator validator = new ContentValidator();
			if(validator.exceptionChecks(mimeType, uploadedFile)){
				
				TelemetryManager.log("Calling Upload Content Node For Node ID: " + contentId);
				String[] urlArray = uploadArtifactToAWS(uploadedFile, contentId);
	
				node.getMetadata().put(ContentAPIParams.s3Key.name(), urlArray[0]);
				node.getMetadata().put(ContentAPIParams.artifactUrl.name(), urlArray[1]);
				node.getMetadata().put(ContentAPIParams.size.name(), getCloudStoredFileSize(urlArray[0]));
	
				TelemetryManager.log("Calling 'updateContentNode' for Node ID: " + contentId);
				response = updateContentNode(contentId, node, urlArray[1]);
				if (!checkError(response)) {
					return response;
				}
			}
		} catch (ClientException e) {
			throw e;
		} catch (ServerException e) {
			throw e;
		} catch (Exception e) {
			throw new ServerException(ContentAPIParams.SERVER_ERROR.name(),
					"Error! Something went Wrong While Uploading the file. | [Node Id: " + node.getIdentifier() + "]");
		}
		return null;
	}
}
