package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.json.JSONObject;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ConnectorIaasJSONTransformerTest {

	@Test
	public void testGetInfrastructureJSON() {

		JSONObject actual = new JSONObject(ConnectorIaasJSONTransformer.getInfrastructureJSON("infrastructureId",
				"type", "username", "password", "endpoint"));

		assertThat(actual.getString("id"), is("infrastructureId"));
		assertThat(actual.getString("type"), is("type"));
		assertThat(actual.getJSONObject("credentials").getString("username"), is("username"));
		assertThat(actual.getJSONObject("credentials").getString("password"), is("password"));
	}

	@Test
	public void testGetInstanceJSON() {
		JSONObject actual = new JSONObject(
				ConnectorIaasJSONTransformer.getInstanceJSON("tag", "image", "number", "minCores", "minRam"));

		assertThat(actual.getString("tag"), is("tag"));
		assertThat(actual.getString("image"), is("image"));
		assertThat(actual.getString("number"), is("number"));
		assertThat(actual.getString("minCores"), is("minCores"));
		assertThat(actual.getString("minRam"), is("minRam"));
	}

	@Test
	public void testGetScriptInstanceJSON() {
		List<String> scripts = Lists.newArrayList("ls", "wget url");
		assertThat(ConnectorIaasJSONTransformer.getScriptInstanceJSON(scripts),
				is("{\"scripts\":[\"ls\",\"wget url\"]}"));
	}

}
