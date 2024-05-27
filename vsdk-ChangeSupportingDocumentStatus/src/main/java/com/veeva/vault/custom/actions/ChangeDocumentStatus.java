package com.veeva.vault.custom.actions;

import com.veeva.vault.custom.usd.SupportingDocumentClass;

import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.action.DocumentAction;
import com.veeva.vault.sdk.api.action.DocumentActionContext;
import com.veeva.vault.sdk.api.action.DocumentActionInfo;
import com.veeva.vault.sdk.api.document.*;

import java.time.LocalDate;
import java.util.List;

@DocumentActionInfo(label = "Change Supporting Document Status Action")
public class ChangeDocumentStatus implements DocumentAction {

	@SuppressWarnings("unchecked")
	public void execute(DocumentActionContext documentActionContext) {

		DocumentService docService = ServiceLocator.locate((DocumentService.class));
		SupportingDocumentClass suppDoc = ServiceLocator.locate(SupportingDocumentClass.class);
		DocumentVersion docVersion = documentActionContext.getDocumentVersions().get(0);
		LogService logService = ServiceLocator.locate(LogService.class);

		List<DocumentVersion> suppDocVersionList = VaultCollections.newList();
		List<String> versionIdList = VaultCollections.newList();
		List<DocumentVersion> docVersionList = VaultCollections.newList();
		List<DocumentVersion> finalList = VaultCollections.newList();
		// List<PositionalDocumentVersionId> result =
		// VaultCollections.newList();
		List<List<Integer>> result = VaultCollections.newList();

		logService.info("");
		logService.info(
				"-----------------------------------Start of Execution------------------------------------------");
		logService.info("");

		String version_id = docVersion.getValue("version_id", ValueType.STRING);

		String tobeLifecycleState = docVersion.getValue("to_be_lifecycle_state__c", ValueType.STRING);

		// Calling function of UDS SupportingDocumentClass
		suppDocVersionList = suppDoc.gettargetversionIds(version_id);

		if (suppDocVersionList != null && suppDocVersionList.size() >= 1) {
			logService.info("");
			logService.info(
					"*************************************************************************************************");
			logService.info(
					"                                    List of target version id                                    ");
			logService.info("");
			suppDocVersionList.forEach(id -> {
				String supportingDocId = id.getValue("id", ValueType.STRING);
				String supportingDocMajorVersion = id.getValue("major_version_number__v", ValueType.NUMBER).toString();
				String supportingDocMinorVersion = id.getValue("minor_version_number__v", ValueType.NUMBER).toString();
				String version = supportingDocId + "_" + supportingDocMajorVersion + "_" + supportingDocMinorVersion;
				if (supportingDocId != null && supportingDocMajorVersion != null && supportingDocMinorVersion != null)
					versionIdList.add(version);
				logService.info(id + ":::" + version);
			});
			logService.info(
					"*************************************************************************************************");
			logService.info("");

			logService.info("--List of all the found & processed target version Ids =" + versionIdList);
			logService.info("");
		}

		if (versionIdList != null && versionIdList.size() >= 1) {

			List<String> documentTypeList = suppDoc.getsupportingDocs(versionIdList);

			if (documentTypeList != null && documentTypeList.size() >= 1) {
				logService.info("");
				logService.info("###DocTypeList =" + documentTypeList + "::" + documentTypeList.size());

				for (int k = 0; k < documentTypeList.size(); k++) {

					String ID = documentTypeList.get(k).substring((documentTypeList.get(k).indexOf(":") + 1),
							(documentTypeList.get(k).length()));
					int index = versionIdList.indexOf(ID);
					logService.info("");
					logService.info(
							"<==========" + ID + "::'" + index + "'::" + suppDocVersionList.get(index) + "==========>");

					LocalDate now = LocalDate.now();

					if (tobeLifecycleState != null && (tobeLifecycleState.equalsIgnoreCase("Expired")
							|| tobeLifecycleState.equalsIgnoreCase("Withdrawn"))) {
						suppDocVersionList.get(index).setValue("related_material_expiration_date__c", now);
						logService.info("---------Value set for " + documentTypeList.get(k) + "--------");
					} else if (tobeLifecycleState != null && tobeLifecycleState.equalsIgnoreCase("Cancelled")) {
						suppDocVersionList.get(index).setValue("related_material_cancellation_date__c", now);
						logService.info("---------Value set for " + documentTypeList.get(k) + "--------");
					}
					finalList.add(suppDocVersionList.get(index));
				}
				logService.info("");
				// Calling function of UDS SupportingDocumentClass
				result = suppDoc.saveDoc(finalList);
				int size = result.size();
				int flag = 0;
				for (int i = 0; i < size; i++) {
					if (result.get(i).get(1) == 0) {
						flag += 1;
						if (result.get(i).get(0) < 4) {
							// System.out.println(i*4+ " , " +
							// finalList.size());
							List<String> sublist = documentTypeList.subList(i * 4, finalList.size());
							logService.info("These Document does not saved: " + sublist);
						} else {
							// System.out.println(i*4+ " , " + ((i*4)+4));
							List<String> sublist = documentTypeList.subList(i * 4, ((i * 4) + 4));
							logService.info("These Document does not saved: " + sublist);
						}
					}
				}
				if (flag == 0)
					logService.info("                     All the documents saved successfully                  ");
				// result =
				// docService.saveDocumentVersions(finalList).getSuccesses();
				logService.info("%%%%  saved docs =" + result + "::" + result.size());
			} else {
				logService.info("");
				logService.info("NO SUPPORTING DOCUMENT OF TYPE 'VARIABLE MULTICHANNEL CONTENT' FOUND");
				logService.info("");
			}
		}

		else {
			logService.info("");
			logService.info(
					"NO SUPPORTING DOCUMENT ARE ASSOCIATED WITH THIS DOCUMENT, HENCE NO TARGET VERSION IDs FOUND");
			logService.info("");
		}

		docVersion.setValue("to_be_lifecycle_state__c", "");
		docVersionList.add(docVersion);
		docService.saveDocumentVersions(docVersionList);

		logService.info("");
		logService
				.info("-----------------------------------End of Execution------------------------------------------");
		logService.info("");
	}

	public boolean isExecutable(DocumentActionContext documentActionContext) {
		return true;
	}
}
