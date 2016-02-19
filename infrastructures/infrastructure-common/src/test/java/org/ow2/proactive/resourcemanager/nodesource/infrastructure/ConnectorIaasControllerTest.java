package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.objectweb.proactive.core.node.Node;
import org.objectweb.proactive.core.node.NodeInformation;
import org.objectweb.proactive.core.runtime.ProActiveRuntime;
import org.ow2.proactive.resourcemanager.nodesource.NodeSource;
import org.python.google.common.collect.Sets;

import com.google.common.collect.Lists;


public class ConnectorIaasControllerTest {

    private ConnectorIaasController connectorIaasController;

    @Mock
    private ConnectorIaasClient connectorIaasClient;
    @Mock
    private NodeSource nodeSource;
    @Mock
    private Node node;
    @Mock
    private ProActiveRuntime proActiveRuntime;
    @Mock
    private NodeInformation nodeInformation;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        connectorIaasController = new ConnectorIaasController(connectorIaasClient, "someType");

    }

    @Test
    public void testWaitForConnectorIaasToBeUP() {
        connectorIaasController.waitForConnectorIaasToBeUP();
        verify(connectorIaasClient).waitForConnectorIaasToBeUP();
    }

    @Test
    public void testTerminateInstance() {
        connectorIaasController.terminateInstance("infrastructureId", "instanceId");
        verify(connectorIaasClient).terminateInstance("infrastructureId", "instanceId");
    }

    @Test
    public void testCreateInfrastructure() {

        String infrastructureJson = ConnectorIaasJSONTransformer.getInfrastructureJSONWithEndPoint(
                "node_source_name", "someType", "username", "password", "endPoint", true);

        when(connectorIaasClient.createInfrastructure("node_source_name", infrastructureJson))
                .thenReturn("node_source_name");

        String infrastructureId = connectorIaasController.createInfrastructure("node_source_name", "username",
                "password", "endPoint", true);

        assertThat(infrastructureId, is("node_source_name"));

        verify(connectorIaasClient).createInfrastructure("node_source_name", infrastructureJson);

    }

    @Test
    public void testCreateInstances() {

        String instanceJson = ConnectorIaasJSONTransformer.getInstanceJSON("node_source_name", "image", "2",
                "1", "512");

        Set<String> instanceIds = Sets.newHashSet("123", "456");

        when(connectorIaasClient.createInstancesIfNotExisist(anyString(), anyString(), anyString(), anySet()))
                .thenReturn(instanceIds);

        Set<JSONObject> existingInstances = Sets.newHashSet();

        when(connectorIaasClient.getAllJsonInstancesByInfrastructureId("node_source_name"))
                .thenReturn(existingInstances);

        Set<String> instancesIds = connectorIaasController.createInstances("node_source_name",
                "node_source_name", "image", 2, 1, 512);

        assertThat(instancesIds.size(), is(2));
        assertThat(instancesIds.containsAll(instanceIds), is(true));

        verify(connectorIaasClient).createInstancesIfNotExisist("node_source_name", "node_source_name",
                instanceJson, existingInstances);

    }

    @Test
    public void testCreateInstancesWithPublicKeyNameAndInitScript() {

        List<String> scripts = Lists.newArrayList();
        scripts.add("ls -lrt");

        String instanceJson = ConnectorIaasJSONTransformer.getInstanceJSONWithPublicKeyAndScripts(
                "node_source_name", "image", "1", "publicKeyName", "3", scripts);

        Set<String> instanceIds = Sets.newHashSet("123", "456");

        when(connectorIaasClient.createInstancesIfNotExisist(anyString(), anyString(), anyString(), anySet()))
                .thenReturn(instanceIds);

        Set<JSONObject> existingInstances = Sets.newHashSet();

        when(connectorIaasClient.getAllJsonInstancesByInfrastructureId("node_source_name"))
                .thenReturn(existingInstances);

        Set<String> instancesIds = connectorIaasController.createInstancesWithPublicKeyNameAndInitScript(
                "node_source_name", "node_source_name", "image", 1, 3, "publicKeyName", scripts);

        assertThat(instancesIds.size(), is(2));
        assertThat(instancesIds.containsAll(instanceIds), is(true));

        verify(connectorIaasClient).createInstancesIfNotExisist("node_source_name", "node_source_name",
                instanceJson, existingInstances);

    }

    @Test
    public void testCreateInstancesAlreadyExistent() {

        String instanceJson = ConnectorIaasJSONTransformer.getInstanceJSON("node_source_name", "image", "2",
                "1", "512");

        Set<String> instanceIds = Sets.newHashSet("123", "456");

        when(connectorIaasClient.createInstancesIfNotExisist(anyString(), anyString(), anyString(), anySet()))
                .thenReturn(instanceIds);

        Set<JSONObject> existingInstances = Sets.newHashSet(new JSONObject());

        when(connectorIaasClient.getAllJsonInstancesByInfrastructureId("node_source_name"))
                .thenReturn(existingInstances);

        Set<String> instancesIds = connectorIaasController.createInstances("node_source_name",
                "node_source_name", "image", 2, 1, 512);

        assertThat(instancesIds.size(), is(2));
        assertThat(instancesIds.containsAll(instanceIds), is(true));

        verify(connectorIaasClient).createInstancesIfNotExisist("node_source_name", "node_source_name",
                instanceJson, existingInstances);

    }

    @Test
    public void testExecuteScript() {

        List<String> scripts = Lists.newArrayList();
        scripts.add("ls -lrt");

        String instanceScriptJson = ConnectorIaasJSONTransformer.getScriptInstanceJSONWithCredentials(scripts,
                null, null);

        when(connectorIaasClient.runScriptOnInstance("node_source_name", "instanceId", instanceScriptJson))
                .thenReturn("all ok");

        connectorIaasController.executeScript("node_source_name", "instanceId", scripts);

        verify(connectorIaasClient).runScriptOnInstance("node_source_name", "instanceId", instanceScriptJson);

    }

    @Test
    public void testExecuteScriptWithCredentials() {

        List<String> scripts = Lists.newArrayList();
        scripts.add("ls -lrt");

        String instanceScriptJson = ConnectorIaasJSONTransformer.getScriptInstanceJSONWithCredentials(scripts,
                "user1", "user2");

        when(connectorIaasClient.runScriptOnInstance("node_source_name", "instanceId", instanceScriptJson))
                .thenReturn("all ok");

        connectorIaasController.executeScriptWithCredentials("node_source_name", "instanceId", scripts,
                "user1", "user2");

        verify(connectorIaasClient).runScriptOnInstance("node_source_name", "instanceId", instanceScriptJson);

    }

}
