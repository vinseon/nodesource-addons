package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;


public class ConnectorIaasJSONTransformer {

    private ConnectorIaasJSONTransformer() {
    }

    public static String getInfrastructureJSONWithEndPoint(String infrastructureId, String type,
            String username, String password, String endpoint, boolean toBeRemovedOnShutdown) {
        JSONObject credentials = new JSONObject();
        credentials.put("username", username);
        credentials.put("password", password);
        JSONObject infrastructure = new JSONObject().put("id", infrastructureId).put("type", type)
                .put("credentials", credentials).put("toBeRemovedOnShutdown", toBeRemovedOnShutdown);

        if (endpoint != null && !endpoint.isEmpty()) {
            infrastructure = infrastructure.put("endpoint", endpoint);
        }

        return infrastructure.toString();
    }

    public static String getInstanceJSON(String tag, String image, String number, String cpu, String ram,
            String spotPrice, String securityGroupNames, String subnetId, String macAddresses) {
        JSONObject hardware = new JSONObject();
        hardware.put("minCores", cpu);
        hardware.put("minRam", ram);
        
        
        JSONObject options = new JSONObject();
        
        if (spotPrice != null && !spotPrice.isEmpty()) {
            options.put("spotPrice", spotPrice);
        }
        
        if (securityGroupNames != null && !securityGroupNames.isEmpty()) {	
        	String[] groups = securityGroupNames.split(",");
        	Set<String> securityGroupNamesSet = new HashSet<String>(Arrays.asList(groups));
            options.put("securityGroupNames", securityGroupNamesSet);
        }
        
        if (subnetId != null && !subnetId.isEmpty()) {
            options.put("subnetId", subnetId);
        }
         
        if (macAddresses != null && !macAddresses.isEmpty()) {
            /*JSONArray addresses = new JSONArray();
            for (String address : macAddresses.split(",")) {
                addresses.put(address);
            }*/
            String[] addresses = macAddresses.split(",");
            Set<String> addressesSet = new HashSet<>(Arrays.asList(addresses));
            options.put("macAddresses", addressesSet);
        }
        
        return new JSONObject().put("tag", tag).put("image", image).put("number", number)
                .put("hardware", hardware).put("options", options).toString();
    }

    public static String getInstanceJSONWithPublicKeyAndScripts(String tag, String image, String number,
            String publicKeyName, String type, List<String> scripts) {
        JSONObject credentials = new JSONObject();
        credentials.put("publicKeyName", publicKeyName);
        JSONObject hardware = new JSONObject();
        hardware.put("type", type);
        JSONObject script = new JSONObject();
        script.put("scripts", new JSONArray(scripts));
        return new JSONObject().put("tag", tag).put("image", image).put("number", number)
                .put("credentials", credentials).put("hardware", hardware).put("initScript", script)
                .toString();
    }

    public static String getScriptInstanceJSONWithCredentials(List<String> scripts, String username,
            String password) {
        JSONObject scriptObject = new JSONObject().put("scripts", new JSONArray(scripts));
        if (username != null && password != null) {
            JSONObject credentials = new JSONObject();
            credentials.put("username", username);
            credentials.put("password", password);
            scriptObject = scriptObject.put("credentials", credentials);
        }
        return scriptObject.toString();
    }

}
