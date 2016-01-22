package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import com.google.common.collect.Sets;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Iterator;
import java.util.Set;

public class ConnectorIaasClient {

	private final RestClient restClient;

	public static RestClient generateRestClient(String connectorIaasURL) {
		return new RestClient(connectorIaasURL);
	}

	public ConnectorIaasClient(RestClient restClient) {
		this.restClient = restClient;
	}

	public String createInfrastructure(String infrastructureJson) {
		return restClient.postToInfrastructuresWebResource(infrastructureJson);
	}

	public Set<String> createInstances(String infrastructureId, String instanceJson) {
		String response = restClient.postToInstancesWebResource(infrastructureId, instanceJson);

		JSONArray instancesJSONObjects = new JSONArray(response);

		Set<String> instancesIds = Sets.newHashSet();

		Iterator<Object> instancesJSONObjectsIterator = instancesJSONObjects.iterator();

		while (instancesJSONObjectsIterator.hasNext()) {
			instancesIds.add(((JSONObject) instancesJSONObjectsIterator.next()).getString("id"));
		}

		return instancesIds;
	}

	public void terminateInstance(String infrastructureId, String instanceId) {
		restClient.deleteToInstancesWebResource(infrastructureId, "instanceId", instanceId);
	}

	public void terminateInfrastructure(String infrastructureId) {
		restClient.deleteInfrastructuresWebResource(infrastructureId);
	}

	public String runScriptOnInstance(String infrastructureId, String instanceId, String instanceScriptJson) {
		return restClient.postToScriptsWebResource(infrastructureId, "instanceId", instanceId, instanceScriptJson);

	}

}
