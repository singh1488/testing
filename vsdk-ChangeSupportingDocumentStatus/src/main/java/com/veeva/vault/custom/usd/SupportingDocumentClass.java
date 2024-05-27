package com.veeva.vault.custom.usd;

import java.util.List;

import com.veeva.vault.sdk.api.core.UserDefinedService;
import com.veeva.vault.sdk.api.core.UserDefinedServiceInfo;
import com.veeva.vault.sdk.api.document.DocumentVersion;

@UserDefinedServiceInfo
public interface SupportingDocumentClass extends UserDefinedService {
	List<DocumentVersion> gettargetversionIds(String version_Id);

	List<String> getsupportingDocs(List<String> docIds);

	List<List<Integer>> saveDoc(List<DocumentVersion> Ids);

}