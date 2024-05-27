package com.veeva.vault.custom.actions;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.veeva.vault.custom.service.VQLExecutor;
import com.veeva.vault.sdk.api.core.LogService;
import com.veeva.vault.sdk.api.core.ServiceLocator;
import com.veeva.vault.sdk.api.core.VaultCollections;
import com.veeva.vault.sdk.api.document.DocumentService;
import com.veeva.vault.sdk.api.document.DocumentVersion;
import com.veeva.vault.sdk.api.job.Job;
import com.veeva.vault.sdk.api.job.JobCompletionContext;
import com.veeva.vault.sdk.api.job.JobInfo;
import com.veeva.vault.sdk.api.job.JobInitContext;
import com.veeva.vault.sdk.api.job.JobInputSupplier;
import com.veeva.vault.sdk.api.job.JobItem;
import com.veeva.vault.sdk.api.job.JobLogger;
import com.veeva.vault.sdk.api.job.JobProcessContext;
import com.veeva.vault.sdk.api.job.JobValueType;

@JobInfo(adminConfigurable = true)
public class CRMExpiryDateUpdationJob implements Job {

	@Override
	public JobInputSupplier init(JobInitContext context) {
		VQLExecutor vqlExecutor = ServiceLocator.locate((VQLExecutor.class));
		List<JobItem> jobItems = vqlExecutor.matDocIDs(context);
		return context.newJobInput(jobItems);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void process(JobProcessContext context) {

		DocumentService docService = ServiceLocator.locate((DocumentService.class));
		List<JobItem> jobItems = context.getCurrentTask().getItems();
		List<String> docPool = VaultCollections.newList();
		List<DocumentVersion> docVersionList = VaultCollections.newList();

		if (jobItems.size() > 0) {
			for (JobItem item : jobItems) {
				docPool.add(item.getValue("id", JobValueType.STRING));
			}
		}
		LogService logService = ServiceLocator.locate(LogService.class);
		if (logService.isInfoEnabled()) {
			logService.info("<----------------------Processing Material Document version: {}--------------------->}",
					docPool);
		}

		StringBuilder sb = new StringBuilder();
		sb.append(
				"Select target__vr.version_id, source__vr.version_id FROM relationships where target__vr.version_id contains ('")
				.append(String.join("','", docPool)).append("')")
				.append(" and relationship_type__v = 'related_approved_material__c' and ((toName(source__vr.type__v) = 'email_template__v') or (toName(source__vr.type__v) = 'email_fragment__v') or (toName(source__vr.type__v) = 'engage_presentation__v') or(toName(source__vr.type__v) = 'template_fragment__v')) and ((toName(source__vr.status__v) = 'approved__c') or (toName(source__vr.status__v) = 'ae_approved__v')) order by target_doc_id__v");

		String queryString = sb.toString();
		if (logService.isInfoEnabled()) {
			logService.info("@@@@@@@@@@@ VQL {}", queryString);
		}
		VQLExecutor vqlExecutor = ServiceLocator.locate((VQLExecutor.class));
		docPool.clear();

		Map<String, List<String>> CRMDocMap = vqlExecutor.execute(queryString, 1);
		if (logService.isInfoEnabled()) {
			logService.info("@@@@@@@@@@@ Output::CRMDocMap {}", CRMDocMap);
		}

		if (!CRMDocMap.isEmpty()) {

			CRMDocMap.entrySet().forEach(entry -> {
				List<String> list = entry.getValue();
				for (String value : list) {
					if (!docPool.contains(value))
						docPool.add(value);
				}
			});

			sb.setLength(0);
			sb.append(
					"Select source__vr.version_id, target__vr.version_id FROM relationships where source__vr.version_id Contains ('")
					.append(String.join("','", docPool)).append("')")
					.append(" AND relationship_type__v= 'related_approved_material__c' and ((toName(target__vr.type__v) = 'medical__c') or (toName(target__vr.type__v) = 'material__c')) order by source_doc_id__v");
			queryString = sb.toString();
			if (logService.isInfoEnabled()) {
				logService.info("@@@@@@@@@@@ VQL {}", queryString);
			}

			docPool.clear();
			Map<String, List<String>> MATDocMap = vqlExecutor.execute(queryString, 2);
			if (logService.isInfoEnabled()) {
				logService.info("@@@@@@@@@@@ Output::MATDocMap {}", MATDocMap);
			}

			if (!MATDocMap.isEmpty()) {

				MATDocMap.entrySet().forEach(entry -> {
					List<String> list = entry.getValue();
					for (String value : list) {
						if (!docPool.contains(value))
							docPool.add(value);
					}
					docPool.add(entry.getKey());
				});

				sb.setLength(0);
				sb.append("Select version_id, expiration_date__c from documents where version_id contains ('")
						.append(String.join("','", docPool)).append("')");
				queryString = sb.toString();

				if (logService.isInfoEnabled()) {
					logService.info("@@@@@@@@@@@ VQL {}", queryString);
				}

				Map<String, List<String>> DateMap = vqlExecutor.execute(queryString, 3);
				if (logService.isInfoEnabled()) {
					logService.info("@@@@@@@@@@@ Output::DateMap {}", DateMap);
				}

				MATDocMap.entrySet().forEach(entry -> {
					List<String> id = entry.getValue();
					List<LocalDate> date = VaultCollections.newList();
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
					for (String version : id) {
						date.add(LocalDate.parse(DateMap.get(version).get(0), formatter));
					}
					date.add(LocalDate.parse(DateMap.get(entry.getKey()).get(0), formatter));
					Collections.sort(date);
					if (logService.isInfoEnabled()) {
						logService.info("@@@@@@@@@@@ CRMDOC::Date List {}::{}", entry.getKey(), date);
					}
					DocumentVersion docVersion = docService.newVersionWithId(entry.getKey());
					docVersion.setValue("expiration_date__c", date.get(0));
					docVersionList.add(docVersion);

				});
			}
		}
		CRMDocMap.keySet().forEach(entry -> {
			DocumentVersion MATDocVersion = docService.newVersionWithId(entry);
			MATDocVersion.setValue("expiration_date_updated_on__c", null);
			docVersionList.add(MATDocVersion);
		});

		if (docVersionList.size() > 500) {
			List<DocumentVersion> subList = VaultCollections.newList();
			int i = 0;
			int size = docVersionList.size();
			do {
				if (size > 499)
					subList = docVersionList.subList(i, i + 499);
				else
					subList = docVersionList.subList(i, docVersionList.size());
				i += 500;
				size -= 499;
				docService.saveDocumentVersions(subList);
			} while (size > 0);
		} else
			docService.saveDocumentVersions(docVersionList);
	}

	@Override
	public void completeWithSuccess(JobCompletionContext context) {
		JobLogger logger = context.getJobLogger();
		logger.log("Job ran successfully");
		LogService logService = ServiceLocator.locate(LogService.class);
		if (logService.isInfoEnabled()) {
			logService.info("Job ran successfully");
		}

	}

	@Override
	public void completeWithError(JobCompletionContext context) {
		JobLogger logger = context.getJobLogger();
		logger.log("Job failed");
		LogService logService = ServiceLocator.locate(LogService.class);
		if (logService.isInfoEnabled()) {
			logService.info("Job failed");
		}

	}

}
