package com.veeva.vault.custom.usd;

import java.util.List;

import com.veeva.vault.sdk.api.query.QueryExecutionRequest;
import com.veeva.vault.sdk.api.query.QueryService;
import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.document.*;

@UserDefinedServiceInfo
public class SupportingDocumentClassImpl implements SupportingDocumentClass {

	@SuppressWarnings("unchecked")
	@Override
	public List<DocumentVersion> gettargetversionIds(String version_id) {

		DocumentService docService = ServiceLocator.locate((DocumentService.class));
		LogService logService = ServiceLocator.locate(LogService.class);
		QueryService queryService = ServiceLocator.locate(QueryService.class);
		List<DocumentVersion> suppDocVersion = VaultCollections.newList();

		logService.info(
				"========================================In gettargetversionIds=======================================");

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT relationship_type__v,target__vr.version_id FROM relationships where source__vr.version_id = '"
				+ version_id + "'AND relationship_type__v= 'supporting_document__c'");
		String queryString = sb.toString();

		logService.info("queryString =" + queryString);

		QueryExecutionRequest queryExecutionRequest = queryService.newQueryExecutionRequestBuilder()
				.withQueryString(queryString).build();
		queryService.query(queryExecutionRequest).onSuccess(queryExecutionResponse -> {
			queryExecutionResponse.streamResults().forEach(r -> {
				DocumentVersion supportingDocVersion = docService
						.newVersionWithId(r.getValue("target__vr.version_id", ValueType.STRING));
				suppDocVersion.add(supportingDocVersion);

			});
			// in this java code snippet I am getting the error "Local variable
			// j defined in an enclosing scope must be final or effectively
			// final", how to solve this?
		}).onError(queryOperationError -> {
			if (logService.isErrorEnabled()) {
				logService.error("Error {}", queryOperationError.getMessage());
			}
			throw new RollbackException("OPERATION_NOT_ALLOWED", queryOperationError.getMessage());
		}).execute();

		logService.info(
				"=================================== Exiting the gettargetversionIds ==================================");

		return suppDocVersion;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<String> getsupportingDocs(List<String> docIds) {

		LogService logService = ServiceLocator.locate(LogService.class);
		QueryService queryService = ServiceLocator.locate(QueryService.class);
		List<String> supportingDocTypeList = VaultCollections.newList();

		logService.info(
				"+++++++++++++++++++++++++++++++++++ In getsupportingDocs +++++++++++++++++++++++++++++++++++++++++++");

		StringBuilder sb = new StringBuilder(
				"SELECT supporting_document_type__c, global_id__sys,major_version_number__v,minor_version_number__v FROM documents WHERE version_id contains (");
		for (int i = 0; i < docIds.size(); i++) {
			sb.append("'");
			sb.append(docIds.get(i));
			sb.append("',");
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(")");

		String queryString = sb.toString();
		logService.info("getSupportingDocType :: queryString =" + queryString);

		QueryExecutionRequest queryExecutionRequest = queryService.newQueryExecutionRequestBuilder()
				.withQueryString(queryString).build();
		queryService.query(queryExecutionRequest).onSuccess(queryExecutionResponse -> {
			queryExecutionResponse.streamResults().forEach(r -> {
				if (r.getValue("supporting_document_type__c", ValueType.PICKLIST_VALUES) != null) {
					if (r.getValue("supporting_document_type__c", ValueType.PICKLIST_VALUES).get(0)
							.equalsIgnoreCase("Variable Multichannel Content")) {

						String type = r.getValue("supporting_document_type__c", ValueType.PICKLIST_VALUES).get(0);
						String id = r.getValue("global_id__sys", ValueType.STRING);
						String maj = r.getValue("major_version_number__v", ValueType.NUMBER).toString();
						String man = r.getValue("minor_version_number__v", ValueType.NUMBER).toString();
						String value = type + ":" + id.substring((id.indexOf("_") + 1), (id.length())) + "_" + maj + "_"
								+ man;
						// logService.info("=========>r ::" +value);
						supportingDocTypeList.add(value);
					}
				}
			});

			logService
					.info("Version Id list of all the found Supporting document of type 'Variable Multichannel Content' ="
							+ supportingDocTypeList);
		}).onError(queryOperationError -> {
			if (logService.isErrorEnabled()) {
				logService.error("Error {}", queryOperationError.getMessage());
			}
			throw new RollbackException("OPERATION_NOT_ALLOWED", queryOperationError.getMessage());
		}).execute();

		logService.info(
				"+++++++++++++++++++++++++++++++++++ Exiting  getsupportingDocs +++++++++++++++++++++++++++++++++++++++++++");

		return supportingDocTypeList;
	}

	@SuppressWarnings("unchecked")
	public List<List<Integer>> saveDoc(List<DocumentVersion> Ids) {

		List<PositionalDocumentVersionId> result = VaultCollections.newList();
		List<DocumentVersion> sub = VaultCollections.newList();
		List<List<Integer>> output = VaultCollections.newList();
		DocumentService docService = ServiceLocator.locate((DocumentService.class));
		LogService logService = ServiceLocator.locate(LogService.class);

		logService.info(
				"++++++++++++++++++++++++++++++++++++++++++++ In saveDoc ++++++++++++++++++++++++++++++++++++++++++++++++++");

		if (Ids.size() > 499) {
			int count = 0;
			int i = 0;
			int size = Ids.size();
			do {
				if (size > 499)
					sub = Ids.subList(i, i + 499);
				else
					sub = Ids.subList(i, Ids.size());
				i += 499;
				size -= 499;
				result = docService.saveDocumentVersions(sub).getSuccesses();
				if (result.size() == sub.size()) {
					output.add(VaultCollections.newList());
					output.get(count).add(sub.size());
					output.get(count).add(1);
				} else {
					output.add(VaultCollections.newList());
					output.get(count).add(sub.size());
					output.get(count).add(0);
				}
				count += 1;
			} while (size > 0);
		} else {
			int count = 0;
			sub = Ids.subList(0, Ids.size());
			result = docService.saveDocumentVersions(sub).getSuccesses();
			output.add(VaultCollections.newList());
			output.get(count).add(sub.size());
			output.get(count).add(1);
		}
		logService.info(
				"++++++++++++++++++++++++++++++++++++++++ Exiting saveDoc ++++++++++++++++++++++++++++++++++++++++++++++");
		return output;
	}
}
