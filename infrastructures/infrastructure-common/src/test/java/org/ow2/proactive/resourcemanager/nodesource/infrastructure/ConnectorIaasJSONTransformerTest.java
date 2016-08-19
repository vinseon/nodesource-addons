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

        JSONObject actual = new JSONObject(ConnectorIaasJSONTransformer.getInfrastructureJSONWithEndPoint(
                "infrastructureId", "type", "username", "password", null, false));

        assertThat(actual.getString("id"), is("infrastructureId"));
        assertThat(actual.getString("type"), is("type"));
        assertThat(actual.getJSONObject("credentials").getString("username"), is("username"));
        assertThat(actual.getJSONObject("credentials").getString("password"), is("password"));
    }

    @Test
    public void testGetInstanceJSON() {
        JSONObject actual = new JSONObject(ConnectorIaasJSONTransformer.getInstanceJSON("tag", "image",
                "number", "minCores", "minRam", null));

        assertThat(actual.getString("tag"), is("tag"));
        assertThat(actual.getString("image"), is("image"));
        assertThat(actual.getString("number"), is("number"));
        assertThat(actual.getJSONObject("hardware").getString("minCores"), is("minCores"));
        assertThat(actual.getJSONObject("hardware").getString("minRam"), is("minRam"));
        assertThat(actual.getJSONObject("options").toString(), is("{}"));
    }

    @Test
    public void testGetInstanceJSONWithSpotPrice() {
        JSONObject actual = new JSONObject(ConnectorIaasJSONTransformer.getInstanceJSON("tag", "image",
                "number", "minCores", "minRam", "0.05"));

        assertThat(actual.getString("tag"), is("tag"));
        assertThat(actual.getString("image"), is("image"));
        assertThat(actual.getString("number"), is("number"));
        assertThat(actual.getJSONObject("hardware").getString("minCores"), is("minCores"));
        assertThat(actual.getJSONObject("hardware").getString("minRam"), is("minRam"));
        assertThat(actual.getJSONObject("options").getString("spotPrice"), is("0.05"));
    }

    @Test
    public void testGetScriptInstanceJSON() {
        List<String> scripts = Lists.newArrayList("ls", "wget url");
        assertThat(ConnectorIaasJSONTransformer.getScriptInstanceJSONWithCredentials(scripts, null, null),
                is("{\"scripts\":[\"ls\",\"wget url\"]}"));
    }

    @Test
    public void testGetInfrastructureJSONWithEndpoint() {

        JSONObject actual = new JSONObject(ConnectorIaasJSONTransformer.getInfrastructureJSONWithEndPoint(
                "infrastructureId", "type", "username", "password", "endpoint", true));

        assertThat(actual.getString("id"), is("infrastructureId"));
        assertThat(actual.getString("type"), is("type"));
        assertThat(actual.getString("endpoint"), is("endpoint"));
        assertThat(actual.getBoolean("toBeRemovedOnShutdown"), is(true));
        assertThat(actual.getJSONObject("credentials").getString("username"), is("username"));
        assertThat(actual.getJSONObject("credentials").getString("password"), is("password"));
    }

    @Test
    public void testGetInstanceJSONWithPublicKeyAndScripts() {

        List<String> scripts = null;
        JSONObject actual = new JSONObject(
            ConnectorIaasJSONTransformer.getInstanceJSONWithPublicKeyAndScripts("tag", "image", "number",
                    "publicKeyName", "type", scripts));

        assertThat(actual.getString("tag"), is("tag"));
        assertThat(actual.getString("image"), is("image"));
        assertThat(actual.getString("number"), is("number"));
        assertThat(actual.getJSONObject("credentials").getString("publicKeyName"), is("publicKeyName"));
        assertThat(actual.getJSONObject("hardware").getString("type"), is("type"));
        assertThat(actual.getJSONObject("initScript").getJSONArray("scripts").length(), is(0));
    }

}
