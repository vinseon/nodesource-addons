package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Sets;


public class ConnectorIaasClientTest {

    private ConnectorIaasClient connectorIaasClient;

    private final String infrastructureJson = "{id=\"123\"}";

    @Mock
    private RestClient restClient;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        when(restClient.getInfrastructures()).thenReturn("[]");
        connectorIaasClient = new ConnectorIaasClient(restClient);

    }

    @Test
    public void testCreateInfrastructure() {

        Mockito.when(restClient.postToInfrastructuresWebResource(infrastructureJson))
                .thenReturn("{'infrastructureId' : 'infra123'}");
        connectorIaasClient.createInfrastructure(infrastructureJson);

        Mockito.verify(restClient).postToInfrastructuresWebResource(infrastructureJson);
    }

    @Test
    public void testCreateInstances() {

        Mockito.when(restClient.postToInstancesWebResource("infra123", "{id=\"123\"}"))
                .thenReturn("[{'id' : 'instance123'}]");

        Set<String> instances = connectorIaasClient.createInstances("infra123", "{id=\"123\"}");

        assertThat(instances.size(), is(1));

        assertThat(instances.iterator().next(), is("instance123"));

    }

    @Test
    public void testCreateMultipleInstances() {

        Mockito.when(restClient.postToInstancesWebResource("infra123", "{id=\"123\"}"))
                .thenReturn("[{'id' : 'instance123'},{'id' : 'instance456'},{'id' : 'instance789'}]");

        Set<String> instances = connectorIaasClient.createInstances("infra123", "{id=\"123\"}");

        assertThat(instances.size(), is(3));

        Set<String> expectedId = Sets.newHashSet("instance123", "instance456", "instance789");

        for (String id : instances) {
            assertThat(expectedId.remove(id), is(true));
        }

    }

    @Test
    public void testTerminateInstance() {

        connectorIaasClient.terminateInstance("infra123", "123456");

        Mockito.verify(restClient).deleteToInstancesWebResource("infra123", "instanceId", "123456");

    }

    @Test
    public void testTerminateInfrastructure() {

        connectorIaasClient.terminateInfrastructure("infra123");

        Mockito.verify(restClient).deleteInfrastructuresWebResource("infra123");

    }

    @Test
    public void testRunScriptOnInstance() {

        connectorIaasClient.runScriptOnInstance("infra123", "123456", "somescriptjason");

        Mockito.verify(restClient).postToScriptsWebResource("infra123", "instanceId", "123456",
                "somescriptjason");

    }

}
