package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

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
import org.python.google.common.collect.Lists;
import org.python.google.common.collect.Sets;


public class VMWareInfrastructureTest {

    private VMWareInfrastructure openstackInfrastructure;

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
        openstackInfrastructure = new VMWareInfrastructure();

    }

    @Test
    public void testInitialParamateres() {
        assertThat(openstackInfrastructure.username, is(nullValue()));
        assertThat(openstackInfrastructure.password, is(nullValue()));
        assertThat(openstackInfrastructure.endpoint, is(nullValue()));
        assertThat(openstackInfrastructure.ram, is(512));
        assertThat(openstackInfrastructure.cores, is(1));
        assertThat(openstackInfrastructure.vmUsername, is(nullValue()));
        assertThat(openstackInfrastructure.vmPassword, is(nullValue()));
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
                    is("-c 'wget -nv " + openstackInfrastructure.rmHostname + "/rest/node.jar'"));

        }
        assertThat(openstackInfrastructure.additionalProperties, is("-Dproactive.useIPaddress=true"));

    }

    @Test
    public void testConfigure() {
        openstackInfrastructure.configure("username", "password", "endpoint", "test.activeeon.com",
                "http://localhost:8088/connector-iaas", "openstack-image", "1", "512", "vmUsername",
                "vmPassword", "2", "3", "wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value");
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
                "http://localhost:8088/connector-iaas", "openstack-image", "512", "1", "vmUsername",
                "vmPassword", "1", "3", "wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value");

        openstackInfrastructure.connectorIaasClient = connectorIaasClient;
        when(nodeSource.getName()).thenReturn("node source name");
        openstackInfrastructure.nodeSource = nodeSource;
        openstackInfrastructure.rmUrl = "http://test.activeeon.com";

        Set<String> instanceIds = Sets.newHashSet("123", "456");
        when(connectorIaasClient.createInstances(anyString(), anyString())).thenReturn(instanceIds);

        openstackInfrastructure.acquireNode();

        String infrastructureJson = ConnectorIaasJSONTransformer.getInfrastructureJSONWithEndPoint(
                "node_source_name", VMWareInfrastructure.INFRASTRUCTURE_TYPE, "username", "password",
                "endpoint");

        verify(connectorIaasClient, times(1)).waitForConnectorIaasToBeUP();

        verify(connectorIaasClient).createInfrastructure("node_source_name", infrastructureJson);

        List<String> scripts = Lists.newArrayList();
        scripts.add("wget -nv test.activeeon.com/rest/node.jar");
        scripts.add(
                "nohup java -jar node.jar -Dproactive.communication.protocol=http -Dproactive.pamr.router.address=test.activeeon.com -DinstanceTag=node_source_name_1 -Dnew=value -r http://test.activeeon.com -s node source name -w 3  &");

        String instanceJson = ConnectorIaasJSONTransformer.getInstanceJSON("node_source_name",
                "openstack-image", "1", "1", "512");

        verify(connectorIaasClient).createInstances("node_source_name", instanceJson);

    }

    @Test
    public void testAcquireAllNodes() {
        testAcquireNode();
    }

    @Test
    public void testRemoveNode() throws ProActiveException, RMException {
        openstackInfrastructure.configure("username", "password", "endpoint", "test.activeeon.com",
                "http://localhost:8088/connector-iaas", "openstack-image", "1", "512", "vmUsername",
                "vmPassword", "2", "3", "wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value");

        openstackInfrastructure.connectorIaasClient = connectorIaasClient;

        when(nodeSource.getName()).thenReturn("node source name");
        openstackInfrastructure.nodeSource = nodeSource;

        when(node.getProperty(VMWareInfrastructure.INSTANCE_TAG_NODE_PROPERTY)).thenReturn("123");

        when(node.getNodeInformation()).thenReturn(nodeInformation);

        when(node.getProActiveRuntime()).thenReturn(proActiveRuntime);

        when(nodeInformation.getName()).thenReturn("nodename");

        openstackInfrastructure.nodesPerInstances.put("123", Sets.newHashSet("nodename"));

        openstackInfrastructure.removeNode(node);

        verify(proActiveRuntime).killNode("nodename");

        verify(connectorIaasClient).terminateInstance(null, "123");

        assertThat(openstackInfrastructure.nodesPerInstances.isEmpty(), is(true));

    }

    @Test
    public void testNotifyAcquiredNode() throws ProActiveException, RMException {
        openstackInfrastructure.configure("username", "password", "endpoint", "test.activeeon.com",
                "http://localhost:8088/connector-iaas", "openstack-image", "1", "512", "vmUsername",
                "vmPassword", "2", "3", "wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value");

        openstackInfrastructure.connectorIaasClient = connectorIaasClient;

        when(node.getProperty(VMWareInfrastructure.INSTANCE_TAG_NODE_PROPERTY)).thenReturn("123");

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

    @Test
    public void testshutdown() {
        openstackInfrastructure.configure("username", "password", "endpoint", "test.activeeon.com",
                "http://localhost:8088/connector-iaas", "openstack-image", "1", "512", "vmUsername",
                "vmPassword", "2", "3", "wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value");

        openstackInfrastructure.connectorIaasClient = connectorIaasClient;
        openstackInfrastructure.infrastructureId = "nodename";

        openstackInfrastructure.nodesPerInstances.put("123", Sets.newHashSet("nodeurl"));

        openstackInfrastructure.shutDown();

        assertThat(openstackInfrastructure.nodesPerInstances.isEmpty(), is(true));

        verify(connectorIaasClient).terminateInfrastructure("nodename");
    }

}
