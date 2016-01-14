package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import java.util.Iterator;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.common.collect.Sets;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.WebResource;

public class ConnectorIaasClient {

	private final WebResource infrastructuresWebResource;
	private final WebResource instancesWebResource;
	private final WebResource scriptsWebResource;

	public static final Client jerseyClient = Client.create();

	public ConnectorIaasClient(Client client, String connectorIaasUrl, String infrastructureId,
			String infrastructureJson) {
		infrastructuresWebResource = client.resource(connectorIaasUrl + "/infrastructures");
		instancesWebResource = client
				.resource(connectorIaasUrl + "/infrastructures/" + infrastructureId + "/instances");
		scriptsWebResource = client
				.resource(connectorIaasUrl + "/infrastructures/" + infrastructureId + "/instance/scripts");

		createInfrastructure(infrastructureJson);
	}

	private String createInfrastructure(String infrastructureJson) {
		return checkResponseIsOK(
				infrastructuresWebResource.type("application/json").post(ClientResponse.class, infrastructureJson))
						.getEntity(String.class);
	}

	public Set<String> createInstances(String instanceJson) {
		ClientResponse response = checkResponseIsOK(
				instancesWebResource.type("application/json").post(ClientResponse.class, instanceJson));

		JSONArray instancesJSONObjects = new JSONArray(response.getEntity(String.class));

		Set<String> instancesIds = Sets.newHashSet();

		Iterator<Object> instancesJSONObjectsIterator = instancesJSONObjects.iterator();

		while (instancesJSONObjectsIterator.hasNext()) {
			instancesIds.add(((JSONObject) instancesJSONObjectsIterator.next()).getString("id"));
		}

		return instancesIds;
	}

	public void terminateInstance(String instanceId) {
		instancesWebResource.queryParam("instanceId", instanceId).accept("application/json").delete();
	}

	public String runScriptOnInstances(String instanceId, String instanceScriptJson) {
		return checkResponseIsOK(scriptsWebResource.queryParam("instanceId", instanceId).type("application/json")
				.post(ClientResponse.class, instanceScriptJson)).getEntity(String.class);
	}

	private ClientResponse checkResponseIsOK(ClientResponse response) {
		if (response.getStatus() != Status.OK.getStatusCode()) {
			throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
		}
		return response;
	}

}
