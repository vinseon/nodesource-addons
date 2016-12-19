package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONObject;


public class ConnectorIaasController {

    private static final Logger logger = Logger.getLogger(ConnectorIaasController.class);

    protected final ConnectorIaasClient connectorIaasClient;
    private final String infrastructureType;

    public ConnectorIaasController(String connectorIaasURL, String infrastructureType) {
        this.connectorIaasClient = new ConnectorIaasClient(
            ConnectorIaasClient.generateRestClient(connectorIaasURL));
        this.infrastructureType = infrastructureType;

    }

    public ConnectorIaasController(ConnectorIaasClient connectorIaasClient, String infrastructureType) {
        this.connectorIaasClient = connectorIaasClient;
        this.infrastructureType = infrastructureType;

    }

    public void waitForConnectorIaasToBeUP() {
        connectorIaasClient.waitForConnectorIaasToBeUP();
    }

    public String createInfrastructure(String infrastructureId, String username, String password,
            String endPoint, boolean destroyOnShutdown) {

        String infrastructureJson = ConnectorIaasJSONTransformer.getInfrastructureJSONWithEndPoint(
                infrastructureId, infrastructureType, username, password, endPoint, destroyOnShutdown);

        logger.info("Creating infrastructure : " + infrastructureJson);

        connectorIaasClient.createInfrastructure(infrastructureId, infrastructureJson);

        logger.info("Infrastructure created");

        return infrastructureId;
    }

    public Set<String> createInstancesWithOptions(String infrastructureId, String instanceTag, String image,
            int numberOfInstances, int cores, int ram, String spotPrice, String securityGroupNames, String subnetId, String macAddresses) {

        String instanceJson = ConnectorIaasJSONTransformer.getInstanceJSON(instanceTag, image,
                "" + numberOfInstances, "" + cores, "" + ram, spotPrice, securityGroupNames, subnetId, macAddresses);

        return createInstance(infrastructureId, instanceTag, instanceJson);
    }

    public Set<String> createInstances(String infrastructureId, String instanceTag, String image,
            int numberOfInstances, int cores, int ram) {

        String instanceJson = ConnectorIaasJSONTransformer.getInstanceJSON(instanceTag, image,
                "" + numberOfInstances, "" + cores, "" + ram, null, null, null, null);

        return createInstance(infrastructureId, instanceTag, instanceJson);
    }

    public Set<String> createInstancesWithPublicKeyNameAndInitScript(String infrastructureId,
            String instanceTag, String image, int numberOfInstances, int hardwareType, String publicKeyName,
            List<String> scripts) {

        String instanceJson = ConnectorIaasJSONTransformer.getInstanceJSONWithPublicKeyAndScripts(instanceTag,
                image, String.valueOf(numberOfInstances), publicKeyName, String.valueOf(hardwareType),
                scripts);

        return createInstance(infrastructureId, instanceTag, instanceJson);
    }

    public void executeScript(String infrastructureId, String instanceId, List<String> scripts) {
        executeScriptWithCredentials(infrastructureId, instanceId, scripts, null, null);

    }

    public void executeScriptWithCredentials(String infrastructureId, String instanceId, List<String> scripts,
            String username, String password) {

        String instanceScriptJson = ConnectorIaasJSONTransformer.getScriptInstanceJSONWithCredentials(scripts,
                username, password);

        String scriptResult = null;
        try {
        	
            scriptResult = connectorIaasClient.runScriptOnInstance(infrastructureId, instanceId,
                    instanceScriptJson);
       
            	logger.info("Executed successfully script for instance id :" + instanceId );
            	logger.info("InstanceScriptJson : " + instanceScriptJson);
                logger.info("Script result : " + scriptResult);
            
        } catch (Exception e) {
            logger.error("Error while executing script :\n" + instanceScriptJson, e);
        }
    }

    public void terminateInstance(String infrastructureId, String instanceId) {
        connectorIaasClient.terminateInstance(infrastructureId, instanceId);
    }

    private Set<String> createInstance(String infrastructureId, String instanceTag, String instanceJson) {
        Set<JSONObject> existingInstancesByInfrastructureId = connectorIaasClient
                .getAllJsonInstancesByInfrastructureId(infrastructureId);

        logger.info("Total existing Instances By Infrastructure Id : " +
            existingInstancesByInfrastructureId.size());

        logger.info("InstanceJson : " + instanceJson);

        Set<String> instancesIds = connectorIaasClient.createInstancesIfNotExisist(infrastructureId,
                instanceTag, instanceJson, existingInstancesByInfrastructureId);

        logger.info("Instances ids created : " + instancesIds);

        return instancesIds;
    }

}
