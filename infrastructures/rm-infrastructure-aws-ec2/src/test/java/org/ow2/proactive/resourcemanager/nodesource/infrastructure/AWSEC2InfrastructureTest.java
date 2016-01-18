package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.python.google.common.collect.Sets;

public class AWSEC2InfrastructureTest {

	private AWSEC2Infrastructure awsec2Infrastructure;

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
		awsec2Infrastructure = new AWSEC2Infrastructure();

	}

	@Test
	public void testInitialParamateres() {
		assertThat(awsec2Infrastructure.aws_key, is(nullValue()));
		assertThat(awsec2Infrastructure.aws_secret_key, is(nullValue()));
		assertThat(awsec2Infrastructure.rmDomain, is(not(nullValue())));
		assertThat(awsec2Infrastructure.connectorIaasURL, is("http://localhost:8088/connector-iaas"));
		assertThat(awsec2Infrastructure.image, is(nullValue()));
		assertThat(awsec2Infrastructure.numberOfInstances, is(1));
		assertThat(awsec2Infrastructure.numberOfNodesPerInstance, is(1));
		assertThat(awsec2Infrastructure.operatingSystem, is("linux"));
		assertThat(awsec2Infrastructure.downloadCommand,
				is("wget -nv " + awsec2Infrastructure.rmDomain + "/rest/node.jar"));
		assertThat(awsec2Infrastructure.additionalProperties, is(""));
		assertThat(awsec2Infrastructure.ram, is(512));
		assertThat(awsec2Infrastructure.cpu, is(1));
	}

	@Test
	public void testConfigure() {

		awsec2Infrastructure.configure("aws_key", "aws_secret_key", "test.activeeon.com",
				"http://localhost:8088/connector-iaas", "aws-image", "2", "3", "mac",
				"wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value", 512, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void tesConfigureNotEnoughParameters() {

		awsec2Infrastructure.configure("aws_key", "aws_secret_key", "test.activeeon.com",
				"http://localhost:8088/connector-iaas", "aws-image", "mac", "wget -nv test.activeeon.com/rest/node.jar",
				"-Dnew=value", 512, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testConfigureWrongOperatingSystem() {

		awsec2Infrastructure.configure("aws_key", "aws_secret_key", "test.activeeon.com",
				"http://localhost:8088/connector-iaas", "aws-image", "2", "3", "solaris",
				"wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value", 512, 1);
	}

	@Test
	public void testAcquireNode() {

		awsec2Infrastructure.configure("aws_key", "aws_secret_key", "test.activeeon.com",
				"http://localhost:8088/connector-iaas", "aws-image", "2", "3", "mac",
				"wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value", 512, 1);

		awsec2Infrastructure.connectorIaasClient = connectorIaasClient;
		when(nodeSource.getName()).thenReturn("node source name");
		awsec2Infrastructure.nodeSource = nodeSource;
		awsec2Infrastructure.rmUrl = "http://test.activeeon.com";

		Set<String> instanceIds = Sets.newHashSet("123", "456");
		when(connectorIaasClient.createInstances(anyString(), anyString())).thenReturn(instanceIds);

		awsec2Infrastructure.acquireNode();

		String infrastructureJson = ConnectorIaasJSONTransformer.getInfrastructureJSON("node_source_name",
				AWSEC2Infrastructure.INFRASTRUCTURE_TYPE, "aws_key", "aws_secret_key");

		verify(connectorIaasClient).createInfrastructure(infrastructureJson);

		String instanceJson = ConnectorIaasJSONTransformer.getInstanceJSON("node_source_name", "aws-image", "2", "1",
				"512");

		verify(connectorIaasClient).createInstances("node_source_name", instanceJson);

		String scripts123 = "{\"scripts\":[\"wget -nv test.activeeon.com/rest/node.jar\",\"nohup java -jar node.jar -Dproactive.communication.protocol=http -Dproactive.pamr.router.address=test.activeeon.com -DinstanceId=123 -Dnew=value -r http://test.activeeon.com -s node source name -w 3  &\"]}";
		String scripts456 = "{\"scripts\":[\"wget -nv test.activeeon.com/rest/node.jar\",\"nohup java -jar node.jar -Dproactive.communication.protocol=http -Dproactive.pamr.router.address=test.activeeon.com -DinstanceId=456 -Dnew=value -r http://test.activeeon.com -s node source name -w 3  &\"]}";

		verify(connectorIaasClient, times(1)).runScriptOnInstance("node_source_name", "123", scripts123);
		verify(connectorIaasClient, times(1)).runScriptOnInstance("node_source_name", "456", scripts456);

	}

	@Test
	public void testAcquireAllNodes() {
		testAcquireNode();
	}

	@Test
	public void testRemoveNode() throws ProActiveException, RMException {
		awsec2Infrastructure.configure("aws_key", "aws_secret_key", "test.activeeon.com",
				"http://localhost:8088/connector-iaas", "aws-image", "2", "3", "mac",
				"wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value", 512, 1);

		awsec2Infrastructure.connectorIaasClient = connectorIaasClient;

		when(nodeSource.getName()).thenReturn("node source name");
		awsec2Infrastructure.nodeSource = nodeSource;

		when(node.getProperty(AWSEC2Infrastructure.INSTANCE_ID_NODE_PROPERTY)).thenReturn("123");

		when(node.getNodeInformation()).thenReturn(nodeInformation);

		when(node.getProActiveRuntime()).thenReturn(proActiveRuntime);

		when(nodeInformation.getName()).thenReturn("nodename");

		awsec2Infrastructure.nodesPerInstances.put("123", Sets.newHashSet("nodename"));

		awsec2Infrastructure.removeNode(node);

		verify(proActiveRuntime).killNode("nodename");

		verify(connectorIaasClient).terminateInstance(null, "123");

		assertThat(awsec2Infrastructure.nodesPerInstances.isEmpty(), is(true));

	}

	@Test
	public void testNotifyAcquiredNode() throws ProActiveException, RMException {
		awsec2Infrastructure.configure("aws_key", "aws_secret_key", "test.activeeon.com",
				"http://localhost:8088/connector-iaas", "aws-image", "2", "3", "mac",
				"wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value", 512, 1);

		awsec2Infrastructure.connectorIaasClient = connectorIaasClient;

		when(node.getProperty(AWSEC2Infrastructure.INSTANCE_ID_NODE_PROPERTY)).thenReturn("123");

		when(node.getNodeInformation()).thenReturn(nodeInformation);

		when(nodeInformation.getName()).thenReturn("nodename");

		awsec2Infrastructure.notifyAcquiredNode(node);

		assertThat(awsec2Infrastructure.nodesPerInstances.get("123").isEmpty(), is(false));
		assertThat(awsec2Infrastructure.nodesPerInstances.get("123").size(), is(1));
		assertThat(awsec2Infrastructure.nodesPerInstances.get("123").contains("nodename"), is(true));

	}

	@Test
	public void testGetDescription() {
		assertThat(awsec2Infrastructure.getDescription(),
				is("Handles nodes from the Amazon Elastic Compute Cloud Service."));
	}

	@Test
	public void testshutdown() {
		awsec2Infrastructure.configure("aws_key", "aws_secret_key", "test.activeeon.com",
				"http://localhost:8088/connector-iaas", "aws-image", "2", "3", "mac",
				"wget -nv test.activeeon.com/rest/node.jar", "-Dnew=value", 512, 1);

		awsec2Infrastructure.connectorIaasClient = connectorIaasClient;
		awsec2Infrastructure.infrastructureId = "nodename";

		awsec2Infrastructure.nodesPerInstances.put("123", Sets.newHashSet("nodeurl"));

		awsec2Infrastructure.shutDown();

		assertThat(awsec2Infrastructure.nodesPerInstances.isEmpty(), is(true));

		verify(connectorIaasClient).terminateInfrastructure("nodename");
	}

}
