package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;


public class ConnectorIaasJSONTransformer {

    private ConnectorIaasJSONTransformer() {
    }

    public static String getInfrastructureJSON(String infrastructureId, String type, String username,
            String password, String endpoint) {
        JSONObject credentials = new JSONObject();
        credentials.put("username", username);
        credentials.put("password", password);
        return new JSONObject().put("id", infrastructureId).put("type", type).put("credentials", credentials)
                .put("endpoint", endpoint).toString();
    }

    public static String getInstanceJSON(String tag, String image, String number, String cpu, String ram) {
        JSONObject hardware = new JSONObject();
        hardware.put("minCores", cpu);
        hardware.put("minRam", ram);
        return new JSONObject().put("tag", tag).put("image", image).put("number", number)
                .put("hardware", hardware).toString();
    }

    public static String getScriptInstanceJSON(List<String> scripts, String vmUsername, String vmPassword) {
        JSONObject credentials = new JSONObject();
        credentials.put("username", vmUsername);
        credentials.put("password", vmPassword);
        return new JSONObject().put("credentials", credentials).put("scripts", new JSONArray(scripts))
                .toString();
    }

}
