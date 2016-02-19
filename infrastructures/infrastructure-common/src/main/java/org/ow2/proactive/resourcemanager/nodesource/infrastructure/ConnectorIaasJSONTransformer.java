package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import java.util.List;

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

    public static String getInstanceJSON(String tag, String image, String number, String cpu, String ram) {
        JSONObject hardware = new JSONObject();
        hardware.put("minCores", cpu);
        hardware.put("minRam", ram);
        return new JSONObject().put("tag", tag).put("image", image).put("number", number)
                .put("hardware", hardware).toString();
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

    public static String getScriptInstanceJSON(List<String> scripts) {
        return new JSONObject().put("scripts", new JSONArray(scripts)).toString();
    }

    public static String getScriptInstanceJSONWithCredentials(List<String> scripts, String vmUsername,
            String vmPassword) {
        JSONObject credentials = new JSONObject();
        credentials.put("username", vmUsername);
        credentials.put("password", vmPassword);
        return new JSONObject().put("credentials", credentials).put("scripts", new JSONArray(scripts))
                .toString();
    }

}
