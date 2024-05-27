package com.veeva.vault.custom.service;

import java.util.List;
import java.util.Map;

import com.veeva.vault.sdk.api.core.LogService;
import com.veeva.vault.sdk.api.core.RollbackException;
import com.veeva.vault.sdk.api.core.ServiceLocator;
import com.veeva.vault.sdk.api.core.UserDefinedServiceInfo;
import com.veeva.vault.sdk.api.core.ValueType;
import com.veeva.vault.sdk.api.core.VaultCollections;
import com.veeva.vault.sdk.api.job.JobInitContext;
import com.veeva.vault.sdk.api.job.JobItem;
import com.veeva.vault.sdk.api.query.QueryExecutionRequest;
import com.veeva.vault.sdk.api.query.QueryService;

@UserDefinedServiceInfo
public class VQLExecutorImpl implements VQLExecutor {

	private static final String Version_ID = "version_id";

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, List<String>> execute(String vql, int flag) {

		LogService logService = ServiceLocator.locate(LogService.class);
		if (logService.isInfoEnabled()) {
			logService.info("@@@@@@@@@@@@@IN UDS");
		}

		QueryService queryService = ServiceLocator.locate(QueryService.class);
		Map<String, List<String>> versionIDs = VaultCollections.newMap();
		// String id = null;
		StringBuilder sb = new StringBuilder();
		StringBuilder vb = new StringBuilder();

		QueryExecutionRequest queryExecutionRequest = queryService.newQueryExecutionRequestBuilder()
				.withQueryString(vql).build();
		queryService.query(queryExecutionRequest).onSuccess(queryExecutionResponse -> {
			if (queryExecutionResponse.getResultCount() > 0) {
				queryExecutionResponse.streamResults().forEach(r -> {
					if (flag == 1) {
						if (r.getValue("source__vr.version_id", ValueType.STRING) != null) {
							if (vb.length() == 0) {
								vb.append(r.getValue("target__vr.version_id", ValueType.STRING));
								sb.append(r.getValue("source__vr.version_id", ValueType.STRING));

							} else {
								String ID = vb.toString();
								if (ID.equals(r.getValue("target__vr.version_id", ValueType.STRING))) {
									sb.append(",").append(r.getValue("source__vr.version_id", ValueType.STRING));
								} else {
									String value = sb.toString();
									String id = vb.toString();
									List<String> ids = splitter(value);
									versionIDs.put(id, ids);
									sb.setLength(0);
									vb.setLength(0);
									sb.append(r.getValue("source__vr.version_id", ValueType.STRING));
									vb.append(r.getValue("target__vr.version_id", ValueType.STRING));
								}
							}
						}
					}

					else if (flag == 2) {
						if (r.getValue("target__vr.version_id", ValueType.STRING) != null) {
							if (vb.length() == 0) {
								vb.append(r.getValue("source__vr.version_id", ValueType.STRING));
								sb.append(r.getValue("target__vr.version_id", ValueType.STRING));
							} else {
								String ID = vb.toString();
								if (ID.equals(r.getValue("source__vr.version_id", ValueType.STRING))) {
									sb.append(",").append(r.getValue("target__vr.version_id", ValueType.STRING));
								} else {
									String value = sb.toString();
									String id = vb.toString();
									List<String> ids = splitter(value);
									versionIDs.put(id, ids);
									sb.setLength(0);
									vb.setLength(0);
									sb.append(r.getValue("target__vr.version_id", ValueType.STRING));
									vb.append(r.getValue("source__vr.version_id", ValueType.STRING));
								}
							}
						}
					} else {
						if (r.getValue("expiration_date__c", ValueType.DATE) != null) {
							versionIDs.put(r.getValue("version_id", ValueType.STRING), VaultCollections
									.asList(r.getValue("expiration_date__c", ValueType.DATE).toString()));
						} else
							versionIDs.put(r.getValue("version_id", ValueType.STRING), null);
					}
				});
				if ((flag == 1 || flag == 2) && (sb.length() > 0)) {
					String value = sb.toString();
					String ID = vb.toString();
					List<String> ids = splitter(value);
					versionIDs.put(ID, ids);
					sb.setLength(0);
					vb.setLength(0);
				}
			} else {
				if (logService.isInfoEnabled()) {
					logService.error("Info: Data not found for the respective vql.");
				}
			}
		}).onError(queryOperationError -> {
			if (logService.isErrorEnabled()) {
				logService.error("ExpiryDateService:: Error {}", queryOperationError.getMessage());
			}
			throw new RollbackException("OPERATION_NOT_ALLOWED", queryOperationError.getMessage());
		}).execute();

		return versionIDs;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<JobItem> matDocIDs(JobInitContext context) {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ").append(Version_ID).append(" FROM documents ")
				.append("where expiration_date_updated_on__c != null order by expiration_date_updated_on__c");

		String queryString = sb.toString();

		LogService logService = ServiceLocator.locate(LogService.class);
		if (logService.isInfoEnabled()) {
			logService.info("@@@@@@@@@@ In UDS");
		}

		List<JobItem> jobItems = VaultCollections.newList();

		QueryService queryService = ServiceLocator.locate(QueryService.class);
		if (logService.isInfoEnabled()) {
			logService.info("@@@@@@@@@@@ VQL {}", queryString);
		}

		QueryExecutionRequest queryExecutionRequest = queryService.newQueryExecutionRequestBuilder()
				.withQueryString(queryString).build();
		queryService.query(queryExecutionRequest).onSuccess(queryExecutionResponse -> {
			if (queryExecutionResponse.getResultCount() > 0) {
				queryExecutionResponse.streamResults().forEach(r -> {

					// Adding every fetched Version_ID as a job item
					JobItem jobItem = context.newJobItem();
					jobItem.setValue("id", r.getValue(Version_ID, ValueType.STRING));
					jobItems.add(jobItem);
				});
			} else {
				if (logService.isInfoEnabled()) {
					logService.error("Info: Data not found for the respective vql.");
				}
			}
		}).onError(queryOperationError -> {
			if (logService.isErrorEnabled()) {
				logService.error("ExpiryDateService:: Error {}", queryOperationError.getMessage());
			}
			throw new RollbackException("OPERATION_NOT_ALLOWED", queryOperationError.getMessage());
		}).execute();

		return jobItems;
	}

	@SuppressWarnings("unchecked")
	private List<String> splitter(String value) {

		List<String> data = VaultCollections.newList();
		int index = value.indexOf(',');
		int i = 0;
		if (value.length() > 0) {
			if (value.contains(",")) {
				do {
					String version = value.substring(i, index);
					data.add(version);
					i = index + 1;
					index = value.indexOf(',', i);
					if (index < 0) {
						data.add(value.substring(i, value.length()));
					}
				} while (index > 0);
			} else
				data.add(value);
		}
		return data;
	}

}