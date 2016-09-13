package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import java.net.HttpURLConnection;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;


public class RestClient {

    private final ResteasyClient restEasyClient;
    private final String connectorIaasURL;

    public RestClient(String connectorIaasURL) {
        this.restEasyClient = new ResteasyClientBuilder().build();
        this.connectorIaasURL = connectorIaasURL;
    }

    public String getInfrastructures() {
        ResteasyWebTarget target = initWebTarget(connectorIaasURL + "/infrastructures");
        Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
        return checkAndGetResponse(response);
    }

    public String getInstancesByInfrastructure(String infrastructureId) {
        ResteasyWebTarget target = initWebTarget(connectorIaasURL + "/infrastructures/" + infrastructureId + "/instances");
        Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
        return checkAndGetResponse(response);
    }

    public String postToInfrastructuresWebResource(String infrastructureJson) {
        ResteasyWebTarget target = initWebTarget(connectorIaasURL + "/infrastructures");
        Response response = target.request().post(Entity.entity(infrastructureJson, MediaType.APPLICATION_JSON_TYPE));
        return checkAndGetResponse(response);
    }

    public void deleteInfrastructuresWebResource(String infrastructureId) {
        ResteasyWebTarget target = initWebTarget(connectorIaasURL + "/infrastructures/" + infrastructureId);
        Response response = target.request(MediaType.APPLICATION_JSON_TYPE).delete();
        checkAndGetResponse(response);
    }

    public String postToInstancesWebResource(String infrastructureId, String instanceJson) {
        ResteasyWebTarget target = initWebTarget(connectorIaasURL + "/infrastructures/" + infrastructureId + "/instances");
        Response response = target.request().post(Entity.entity(instanceJson, MediaType.APPLICATION_JSON_TYPE));
        return checkAndGetResponse(response);
    }

    public void deleteToInstancesWebResource(String infrastructureId, String key, String value) {
        ResteasyWebTarget target = initWebTarget(connectorIaasURL + "/infrastructures/" + infrastructureId + "/instances");
        Response response = target.queryParam(key, value).request(MediaType.APPLICATION_JSON_TYPE).delete();
        checkAndGetResponse(response);
    }

    public String postToScriptsWebResource(String infrastructureId, String key, String value,
            String scriptJson) {
        ResteasyWebTarget target = initWebTarget(connectorIaasURL + "/infrastructures/" + infrastructureId + "/instances/scripts");
        Response response = target.queryParam(key, value).request().post(Entity.entity(scriptJson, MediaType.APPLICATION_JSON_TYPE));
        return checkAndGetResponse(response);
    }

    private Response checkResponseIsOK(Response response) {
        if (response.getStatus() != HttpURLConnection.HTTP_OK) {
            throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
        }
        return response;
    }

    private ResteasyWebTarget initWebTarget(String url){
        return restEasyClient.target(url);
    }

    private String checkAndGetResponse(Response response){
        try {
            return checkResponseIsOK(response).readEntity(String.class);
        }
        finally {
            if (response != null) {
                response.close();
            }
        }
    }
}
