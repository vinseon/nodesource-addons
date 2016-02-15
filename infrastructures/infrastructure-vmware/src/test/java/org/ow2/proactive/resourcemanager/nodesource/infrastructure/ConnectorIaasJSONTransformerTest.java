package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.json.JSONObject;
import org.junit.Test;


public class ConnectorIaasJSONTransformerTest {

    @Test
    public void testGetInfrastructureJSON() {

        JSONObject actual = new JSONObject(ConnectorIaasJSONTransformer
                .getInfrastructureJSON("infrastructureId", "type", "username", "password", "endpoint"));

        assertThat(actual.getString("id"), is("infrastructureId"));
        assertThat(actual.getString("type"), is("type"));
        assertThat(actual.getJSONObject("credentials").getString("username"), is("username"));
        assertThat(actual.getJSONObject("credentials").getString("password"), is("password"));
    }

    @Test
    public void testGetInstanceJSON() {

        JSONObject actual = new JSONObject(
            ConnectorIaasJSONTransformer.getInstanceJSON("tag", "image", "number", "1", "512"));

        assertThat(actual.getString("tag"), is("tag"));
        assertThat(actual.getString("image"), is("image"));
        assertThat(actual.getString("number"), is("number"));
        assertThat(actual.getJSONObject("hardware").getString("minCores"), is("1"));
        assertThat(actual.getJSONObject("hardware").getString("minRam"), is("512"));
    }

}
