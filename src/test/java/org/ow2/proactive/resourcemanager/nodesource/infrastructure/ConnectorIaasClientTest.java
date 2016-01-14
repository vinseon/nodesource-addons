package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;

public class ConnectorIaasClientTest {

	private ConnectorIaasClient connectorIaasClient;

	@Mock
	private Client jerseyClientMock;
	@Mock
	private WebResource infrastructuresWebResourceMock;
	@Mock
	private WebResource instancesWebResourceMock;
	@Mock
	private WebResource scriptsWebResourceMock;
	@Mock
	private Builder resourceBuilderMock;
	@Mock
	private ClientResponse responseMock;

	@Before
	public void init() {
		MockitoAnnotations.initMocks(this);

		String connectorIaasUrl = "connectorIaasUrl";
		String infrastructureId = "123";
		String infrastructureJson = "{id=\"123\"}";

		Mockito.when(jerseyClientMock.resource(connectorIaasUrl + "/infrastructures"))
				.thenReturn(infrastructuresWebResourceMock);
		Mockito.when(
				jerseyClientMock.resource(connectorIaasUrl + "/infrastructures/" + infrastructureId + "/instances"))
				.thenReturn(infrastructuresWebResourceMock);
		Mockito.when(jerseyClientMock
				.resource(connectorIaasUrl + "/infrastructures/" + infrastructureId + "/instance/scripts"))
				.thenReturn(infrastructuresWebResourceMock);

		Mockito.when(infrastructuresWebResourceMock.type("application/json")).thenReturn(resourceBuilderMock);

		Mockito.when(resourceBuilderMock.post(ClientResponse.class, infrastructureJson)).thenReturn(responseMock);

		connectorIaasClient = new ConnectorIaasClient(jerseyClientMock, connectorIaasUrl, infrastructureId,
				infrastructureJson);
	}

	@Test
	public void testCreateInstances() {

	}

}
