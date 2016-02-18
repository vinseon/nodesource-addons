package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.objectweb.proactive.core.ProActiveException;
import org.objectweb.proactive.core.node.Node;
import org.objectweb.proactive.core.node.NodeInformation;
import org.objectweb.proactive.core.runtime.ProActiveRuntime;
import org.ow2.proactive.resourcemanager.exception.RMException;
import org.ow2.proactive.resourcemanager.nodesource.NodeSource;
import org.python.google.common.collect.Sets;


public class OpenstackInfrastructureTest {

    private OpenstackInfrastructure openstackInfrastructure;

    @Mock
    private ConnectorIaasController connectorIaasController;
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
        openstackInfrastructure = new OpenstackInfrastructure();

    }

    @Test
    public void testInitialParamateres() {
        assertThat(openstackInfrastructure.username, is(nullValue()));
        assertThat(openstackInfrastructure.password, is(nullValue()));
        assertThat(openstackInfrastructure.endpoint, is(nullValue()));
        assertThat(openstackInfrastructure.flavor, is(3));
        assertThat(openstackInfrastructure.publicKeyName, is(nullValue()));
        assertThat(openstackInfrastructure.rmHostname, is(not(nullValue())));
        assertThat(openstackInfrastructure.connectorIaasURL,
                is("http://" + openstackInfrastructure.rmHostname + ":8080/connector-iaas"));
        assertThat(openstackInfrastructure.image, is(nullValue()));
        assertThat(openstackInfrastructure.numberOfInstances, is(1));
        assertThat(openstackInfrastructure.numberOfNodesPerInstance, is(1));
        if (System.getProperty("os.name").contains("Windows")) {
            assertThat(openstackInfrastructure.downloadCommand,
                    is("powershell -command \"& { (New-Object Net.WebClient).DownloadFile('" +
                        openstackInfrastructure.rmHostname + "/rest/node.jar', 'node.jar') }\""));
        } else {
            assertThat(openstackInfrastructure.downloadCommand,
                    is("wget -nv " + openstackInfrastructure.rmHostname + "/rest/node.jar"));

        }
        assertThat(openstackInfrastructure.additionalProperties, is("-Dproactive.useIPaddress=true"));

    }

    @Test
    public void testConfigure() {
        openstackInfrastructure.configure("username", "password", "endpoint", "test.activeeon.com",
                "http://localhost:8088/connector-iaas", "openstack-image", "3", "publicKeyName", "2", "3",
                "wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value");
    }

    @Test(expected = IllegalArgumentException.class)
    public void tesConfigureNotEnoughParameters() {

        openstackInfrastructure.configure("username", "password", "endpoint", "test.activeeon.com",
                "http://localhost:8088/connector-iaas", "publicKeyName", "2", "3",
                "wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value");
    }

    @Test
    public void testAcquireNode() {

        openstackInfrastructure.configure("username", "password", "endpoint", "test.activeeon.com",
                "http://localhost:8088/connector-iaas", "openstack-image", "3", "publicKeyName", "2", "3",
                "wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value");

        openstackInfrastructure.connectorIaasController = connectorIaasController;
        when(nodeSource.getName()).thenReturn("node_source_name");
        openstackInfrastructure.nodeSource = nodeSource;
        openstackInfrastructure.rmUrl = "http://test.activeeon.com";

        when(connectorIaasController.createInfrastructure("node_source_name", "username", "password",
                "endpoint", false)).thenReturn("node_source_name");

        when(connectorIaasController.createInstancesWithPublicKeyNameAndInitScript(anyString(), anyString(),
                anyString(), anyInt(), anyInt(), anyString(), anyList()))
                        .thenReturn(Sets.newHashSet("123", "456"));

        openstackInfrastructure.acquireNode();

        verify(connectorIaasController, times(1)).waitForConnectorIaasToBeUP();

        verify(connectorIaasController).createInfrastructure("node_source_name", "username", "password",
                "endpoint", false);

        verify(connectorIaasController, times(2)).createInstancesWithPublicKeyNameAndInitScript(anyString(),
                anyString(), anyString(), anyInt(), anyInt(), anyString(), anyList());

        verify(connectorIaasController, times(0)).executeScript(anyString(), anyString(), anyList());

    }

    @Test
    public void testAcquireAllNodes() {
        testAcquireNode();
    }

    @Test
    public void testRemoveNode() throws ProActiveException, RMException {
        openstackInfrastructure.configure("username", "password", "endpoint", "test.activeeon.com",
                "http://localhost:8088/connector-iaas", "openstack-image", "3", "publicKeyName", "2", "3",
                "wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value");

        openstackInfrastructure.connectorIaasController = connectorIaasController;

        when(nodeSource.getName()).thenReturn("node source name");
        openstackInfrastructure.nodeSource = nodeSource;

        when(node.getProperty(OpenstackInfrastructure.INSTANCE_TAG_NODE_PROPERTY)).thenReturn("123");

        when(node.getNodeInformation()).thenReturn(nodeInformation);

        when(node.getProActiveRuntime()).thenReturn(proActiveRuntime);

        when(nodeInformation.getName()).thenReturn("nodename");

        openstackInfrastructure.nodesPerInstances.put("123", Sets.newHashSet("nodename"));

        openstackInfrastructure.removeNode(node);

        verify(proActiveRuntime).killNode("nodename");

        verify(connectorIaasController).terminateInstance(null, "123");

        assertThat(openstackInfrastructure.nodesPerInstances.isEmpty(), is(true));

    }

    @Test
    public void testNotifyAcquiredNode() throws ProActiveException, RMException {
        openstackInfrastructure.configure("username", "password", "endpoint", "test.activeeon.com",
                "http://localhost:8088/connector-iaas", "openstack-image", "3", "publicKeyName", "2", "3",
                "wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value");

        openstackInfrastructure.connectorIaasController = connectorIaasController;

        when(node.getProperty(OpenstackInfrastructure.INSTANCE_TAG_NODE_PROPERTY)).thenReturn("123");

        when(node.getNodeInformation()).thenReturn(nodeInformation);

        when(nodeInformation.getName()).thenReturn("nodename");

        openstackInfrastructure.notifyAcquiredNode(node);

        assertThat(openstackInfrastructure.nodesPerInstances.get("123").isEmpty(), is(false));
        assertThat(openstackInfrastructure.nodesPerInstances.get("123").size(), is(1));
        assertThat(openstackInfrastructure.nodesPerInstances.get("123").contains("nodename"), is(true));

    }

    @Test
    public void testGetDescription() {
        assertThat(openstackInfrastructure.getDescription(),
                is("Handles nodes from the Amazon Elastic Compute Cloud Service."));
    }

}
