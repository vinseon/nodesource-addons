package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

public class ConnectorIaasJSONTransformer {

	private ConnectorIaasJSONTransformer() {
	}

	public static String getInfrastructureJSON(String infrastructureId, String type, String username, String password,
			String endpoint) {
		JSONObject credentials = new JSONObject();
		credentials.put("username", username);
		credentials.put("password", password);
		return new JSONObject().put("id", infrastructureId).put("type", type).put("credentials", credentials)
				.put("endpoint", endpoint).toString();
	}

	public static String getInstanceJSON(String tag, String image, String number, String publicKeyName, String type,
			List<String> scripts) {
		JSONObject credentials = new JSONObject();
		credentials.put("publicKeyName", publicKeyName);
		JSONObject hardware = new JSONObject();
		hardware.put("type", type);
		JSONObject script = new JSONObject();
		script.put("scripts", new JSONArray(scripts));
		return new JSONObject().put("tag", tag).put("image", image).put("number", number)
				.put("credentials", credentials).put("hardware", hardware).put("script", script).toString();
	}

}
