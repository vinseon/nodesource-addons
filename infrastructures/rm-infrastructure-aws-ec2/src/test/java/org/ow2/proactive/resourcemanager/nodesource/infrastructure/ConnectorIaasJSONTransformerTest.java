package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

public class ConnectorIaasJSONTransformerTest {

	@Test
	public void testGetInfrastructureJSON() {
		assertThat(
				ConnectorIaasJSONTransformer.getInfrastructureJSON("infrastructureId", "type", "username", "password"),
				is("{\"credentials\":{\"password\":\"password\",\"username\":\"username\"},\"id\":\"infrastructureId\",\"type\":\"type\"}"));
	}

	@Test
	public void testGetInstanceJSON() {
		assertThat(ConnectorIaasJSONTransformer.getInstanceJSON("tag", "image", "number", "cpu", "ram"),
				is("{\"image\":\"image\",\"number\":\"number\",\"cpu\":\"cpu\",\"tag\":\"tag\",\"ram\":\"ram\"}"));
	}

	@Test
	public void testGetScriptInstanceJSON() {
		List<String> scripts = Lists.newArrayList("ls", "wget url");
		assertThat(ConnectorIaasJSONTransformer.getScriptInstanceJSON(scripts),
				is("{\"scripts\":[\"ls\",\"wget url\"]}"));
	}

}
