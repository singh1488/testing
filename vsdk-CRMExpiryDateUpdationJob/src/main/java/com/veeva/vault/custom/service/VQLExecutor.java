package com.veeva.vault.custom.service;

import java.util.List;
import java.util.Map;

import com.veeva.vault.sdk.api.core.UserDefinedService;
import com.veeva.vault.sdk.api.core.UserDefinedServiceInfo;
import com.veeva.vault.sdk.api.job.JobInitContext;
import com.veeva.vault.sdk.api.job.JobItem;

@UserDefinedServiceInfo
public interface VQLExecutor extends UserDefinedService {

	List<JobItem> matDocIDs(JobInitContext context);

	Map<String, List<String>> execute(String vql, int flag);
}
