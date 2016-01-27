package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.common.collect.Sets;


public class ConnectorIaasClient {

    private static final Logger logger = Logger.getLogger(ConnectorIaasClient.class);

    private static final int MAX_RETRIES_TO_CONNECT_TO_CONNECTOR_IAAS = 50;
    private static final int SLEEP_TIME_RETRIES_TO_CONNECT_TO_CONNECTOR_IAAS = 5000;

    private final RestClient restClient;

    public static RestClient generateRestClient(String connectorIaasURL) {
        return new RestClient(connectorIaasURL);
    }

    public ConnectorIaasClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public void waitForConnectorIaasToBeUP() {
        int count = 0;
        while (true) {
            try {
                restClient.getInfrastructures();
                return;
            } catch (Exception e) {
                if (++count == MAX_RETRIES_TO_CONNECT_TO_CONNECTOR_IAAS) {
                    logger.error(e);
                    throw e;
                } else {
                    sleepFor(SLEEP_TIME_RETRIES_TO_CONNECT_TO_CONNECTOR_IAAS);
                }
            }
        }

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
        return restClient.postToScriptsWebResource(infrastructureId, "instanceId", instanceId,
                instanceScriptJson);
    }

    private void sleepFor(long millisecondsToSleep) {
        try {
            Thread.sleep(millisecondsToSleep);
        } catch (InterruptedException e) {
        }
    }

}
