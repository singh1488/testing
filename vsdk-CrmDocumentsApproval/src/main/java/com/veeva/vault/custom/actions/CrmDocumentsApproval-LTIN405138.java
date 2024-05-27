package com.veeva.vault.custom.actions;

import java.util.List;
import java.util.Optional;

import com.veeva.vault.sdk.api.action.DocumentAction;
import com.veeva.vault.sdk.api.action.DocumentActionContext;
import com.veeva.vault.sdk.api.action.DocumentActionInfo;
import com.veeva.vault.sdk.api.core.LogService;
import com.veeva.vault.sdk.api.core.RollbackException;
import com.veeva.vault.sdk.api.core.ServiceLocator;
import com.veeva.vault.sdk.api.core.ValueType;
import com.veeva.vault.sdk.api.core.VaultCollections;
import com.veeva.vault.sdk.api.document.DocumentVersion;
import com.veeva.vault.sdk.api.query.QueryCountRequest;
import com.veeva.vault.sdk.api.query.QueryExecutionRequest;
import com.veeva.vault.sdk.api.query.QueryExecutionResult;
import com.veeva.vault.sdk.api.query.QueryService;

/**
 * This class defines that Document User Action to modify CRM documents(where
 * Document type = Email Template,Email Fragment,Template Fragment, Multichannel
 * Presentation,Multichannel slide) from Staged to Approved state when the
 * Related Approved Material is in Approve for Distribution status.
 */

@DocumentActionInfo(label = "CRM Documents Approval Actions")
public class CrmDocumentsApproval implements DocumentAction {

	private static final String FIELD_ID = "id";
	private static final String FIELD_DOC_MAJOR_VERSION = "major_version_number__v";
	private static final String FIELD_DOC_MINOR_VERSION = "minor_version_number__v";
	private static final String FIELD_DOCUMENT_TYPE = "type__v";
	@SuppressWarnings("unused")
	private static final String FIELD_DOCUMENT_STATUS = "status__v";

	private static final String FIELD_BINDER_ID = "parent_binder_id__sys";
	private static final String FIELD_BINDER_MAJOR_VERSION = "parent_binder_major_version__sys";
	private static final String FIELD_BINDER_MINOR_VERSION = "parent_binder_minor_version__sys";

	private static final String FIELD_REL_TARGET_ID = "target_doc_id__v";
	private static final String FIELD_REL_TARGET_MAJOR_VERSION = "target_major_version__v";
	private static final String FIELD_REL_TARGET_MINOR_VERSION = "target_minor_version__v";

	private static final String MESSAGE = "Document cannot be moved to Approved status as Related Approved Material is not in Approve for Distribution status";
	private static final String ERROR = "OPERATION_NOT_ALLOWED";
	private static final String RELATIONSHIP_APPROVED_MAT = "related_approved_material__c";

	private static final String CHECK_CONDITION = "Approved for Distribution";

	private static final String TYPE_SLIDE = "slide__v";
	private static final String TYPE_EMAIl_TEMPLATE = "email_template__v";
	private static final String TYPE_EMAIL_FRAGMENT = "email_fragment__v";
	private static final String TYPE_TEMPLATE_FRAGMENT = "template_fragment__v";
	private static final String TYPE_MULTI_PRESENTATION = "engage_presentation__v";

	@Override
	public boolean isExecutable(DocumentActionContext documentActionContext) {
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(DocumentActionContext documentActionContext) {
		LogService logService = ServiceLocator.locate(LogService.class);
		if (logService.isInfoEnabled()) {
			logService.info(
					"-----------------------------------Start Execution------------------------------------------");
		}

		DocumentVersion docVersion = documentActionContext.getDocumentVersions().get(0);
		String documentType = docVersion.getValue(FIELD_DOCUMENT_TYPE, ValueType.PICKLIST_VALUES).get(0);
		String documentId = docVersion.getValue(FIELD_ID, ValueType.STRING);
		if (logService.isInfoEnabled()) {
			logService.info("documentType: {}; documentId: {}", documentType, documentId);
		}

		if (TYPE_SLIDE.equals(documentType) || TYPE_MULTI_PRESENTATION.equals(documentType)
				|| TYPE_EMAIl_TEMPLATE.equals(documentType) || TYPE_EMAIL_FRAGMENT.equals(documentType)
				|| TYPE_TEMPLATE_FRAGMENT.equals(documentType)) {

			QueryService queryService = ServiceLocator.locate(QueryService.class);
			List<String> slideList = VaultCollections.newList();
			if (TYPE_SLIDE.equals(documentType)) {
				slideList = getSlideLinkDocId(documentId, queryService, logService);
			}

			boolean hasRelMatInApprovedForDistribution = !slideList.isEmpty()
					? hasRelMatInApprovedForDistribution(slideList.get(0), slideList.get(1), slideList.get(2),
							queryService, logService)
					: hasRelMatInApprovedForDistribution(documentId,
							docVersion.getValue(FIELD_DOC_MAJOR_VERSION, ValueType.NUMBER).toString(),
							docVersion.getValue(FIELD_DOC_MINOR_VERSION, ValueType.NUMBER).toString(), queryService,
							logService);
			if (hasRelMatInApprovedForDistribution) {
				if (logService.isInfoEnabled()) {
					logService.info("Inside Check");
				}
				throw new RollbackException(ERROR, MESSAGE);
			}
		}
	}

	/**
	 * TODO javadoc
	 * 
	 * @param docId
	 * @param queryService
	 * @param logService
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "static-access" })
	private List<String> getSlideLinkDocId(String docId, QueryService queryService, LogService logService) {

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ").append(FIELD_BINDER_ID).append(",").append(FIELD_BINDER_MAJOR_VERSION).append(",")
				.append(FIELD_BINDER_MINOR_VERSION).append(" FROM binder_node__sys WHERE content_id__sys=")
				.append(docId);
		String query = sb.toString();

		if (logService.isInfoEnabled()) {
			logService.info("Get binder query: {}", query);
		}

		List<String> result = VaultCollections.newList();
		QueryExecutionRequest queryExecutionRequest = queryService.newQueryExecutionRequestBuilder()
				.withQueryString(query).build();
		queryService.query(queryExecutionRequest).onSuccess(queryExecutionResponse -> {
			Optional<QueryExecutionResult> r = queryExecutionResponse.streamResults().findFirst();
			if (r.isPresent()) {
				String binderId = r.get().getValue(FIELD_BINDER_ID, ValueType.NUMBER.STRING).toString();
				String binderMajVer = r.get().getValue(FIELD_BINDER_MAJOR_VERSION, ValueType.NUMBER).toString();
				String binderMinVer = r.get().getValue(FIELD_BINDER_MINOR_VERSION, ValueType.NUMBER).toString();

				if (logService.isInfoEnabled()) {
					logService.info("Binder: {}_{}_{}", binderId, binderMajVer, binderMinVer);
				}
				result.add(binderId);
				result.add(binderMajVer);
				result.add(binderMinVer);
			} else if (logService.isWarnEnabled()) {
				logService.warn("No binder found for document {}", docId);
			}
		}).onError(queryOperationError -> {
			if (logService.isErrorEnabled()) {
				logService.error("Error {}", queryOperationError.getMessage());
			}
			throw new RollbackException(ERROR, queryOperationError.getMessage());
		}).execute();
		return result;
	}

	/**
	 * 
	 * @param docId
	 * @param majorVersion
	 * @param minorVersion
	 * @param queryService
	 * @param logService
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private boolean hasRelMatInApprovedForDistribution(String docId, String majorVersion, String minorVersion,
			QueryService queryService, LogService logService) {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ").append(FIELD_REL_TARGET_ID).append(",").append(FIELD_REL_TARGET_MAJOR_VERSION).append(",")
				.append(FIELD_REL_TARGET_MINOR_VERSION).append(" FROM relationships WHERE relationship_type__v='")
				.append(RELATIONSHIP_APPROVED_MAT).append("' AND source__vr.version_id=").append("'").append(docId)
				.append("_").append(majorVersion).append("_").append(minorVersion).append("'")
				.append(" AND target__vr.status__v!='").append(CHECK_CONDITION).append("'");
		String query = sb.toString();
		if (logService.isInfoEnabled()) {
			logService.info("Get relationship query: {}", query);
		}
		List<String> result = VaultCollections.newList();
		boolean approvedForDistributionDocFound = false;
		QueryCountRequest queryCountRequest = queryService.newQueryCountRequestBuilder().withQueryString(query).build();
		queryService.count(queryCountRequest).onSuccess(queryCountResponse -> {
			if (logService.isInfoEnabled()) {
				logService.info("queryCountResponse");
			}
			if (0 < queryCountResponse.getTotalCount()) {
				result.add(Boolean.TRUE.toString());
			}
		}).onError(queryOperationError -> {
			if (logService.isErrorEnabled()) {
				logService.error("Error {}", queryOperationError.getMessage());
			}
			throw new RollbackException(ERROR, queryOperationError.getMessage());
		}).execute();

		if (!result.isEmpty()) {
			approvedForDistributionDocFound = Boolean.TRUE.toString().equals(result.get(0));
		} else if (logService.isWarnEnabled()) {
			logService.warn("No {} relationship found for document {}_{}_{}", RELATIONSHIP_APPROVED_MAT, docId,
					majorVersion, minorVersion);
		}
		return approvedForDistributionDocFound;
	}
}