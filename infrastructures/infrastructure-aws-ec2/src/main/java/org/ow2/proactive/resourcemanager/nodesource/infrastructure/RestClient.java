package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.WebResource;


public class RestClient {

    private final Client jerseyClient;
    private final String connectorIaasURL;

    public RestClient(String connectorIaasURL) {
        this.jerseyClient = Client.create();
        this.connectorIaasURL = connectorIaasURL;

    }

    public String getInfrastructures() {
        WebResource infrastructuresWebResource = jerseyClient.resource(connectorIaasURL + "/infrastructures");
        return checkResponseIsOK(
                infrastructuresWebResource.type("application/json").get(ClientResponse.class));
    }

    public String postToInfrastructuresWebResource(String infrastructureJson) {
        WebResource infrastructuresWebResource = jerseyClient.resource(connectorIaasURL + "/infrastructures");
        return checkResponseIsOK(infrastructuresWebResource.type("application/json")
                .post(ClientResponse.class, infrastructureJson));
    }

    public void deleteInfrastructuresWebResource(String infrastructureId) {
        WebResource infrastructuresWebResource = jerseyClient
                .resource(connectorIaasURL + "/infrastructures/" + infrastructureId);
        checkResponseIsOK(infrastructuresWebResource.type("application/json").delete(ClientResponse.class));
    }

    public String postToInstancesWebResource(String infrastructureId, String instanceJson) {
        WebResource instancesWebResource = jerseyClient
                .resource(connectorIaasURL + "/infrastructures/" + infrastructureId + "/instances");
        return checkResponseIsOK(
                instancesWebResource.type("application/json").post(ClientResponse.class, instanceJson));
    }

    public void deleteToInstancesWebResource(String infrastructureId, String key, String value) {
        WebResource instancesWebResource = jerseyClient
                .resource(connectorIaasURL + "/infrastructures/" + infrastructureId + "/instances");
        checkResponseIsOK(instancesWebResource.queryParam(key, value).accept("application/json")
                .delete(ClientResponse.class));
    }

    public String postToScriptsWebResource(String infrastructureId, String key, String value,
            String scriptJson) {
        WebResource scriptsWebResource = jerseyClient
                .resource(connectorIaasURL + "/infrastructures/" + infrastructureId + "/instance/scripts");
        return checkResponseIsOK(scriptsWebResource.queryParam(key, value).type("application/json")
                .post(ClientResponse.class, scriptJson));
    }

    private String checkResponseIsOK(ClientResponse response) {
        if (response.getStatus() != Status.OK.getStatusCode()) {
            throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
        }
        return response.getEntity(String.class);
    }

}
