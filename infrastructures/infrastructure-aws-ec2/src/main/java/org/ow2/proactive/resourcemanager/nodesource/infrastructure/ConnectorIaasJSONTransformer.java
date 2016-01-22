package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

public class ConnectorIaasJSONTransformer {

	private ConnectorIaasJSONTransformer() {
	}

	public static String getInfrastructureJSON(String infrastructureId, String type, String username, String password) {
		JSONObject credentials = new JSONObject();
		credentials.put("username", username);
		credentials.put("password", password);
		return new JSONObject().put("id", infrastructureId).put("type", type).put("credentials", credentials)
				.toString();
	}

	public static String getInstanceJSON(String tag, String image, String number, String cpu, String ram) {
		return new JSONObject().put("tag", tag).put("image", image).put("number", number).put("cpu", cpu)
				.put("ram", ram).toString();
	}

	public static String getScriptInstanceJSON(List<String> scripts) {
		return new JSONObject().put("scripts", new JSONArray(scripts)).toString();
	}

}
