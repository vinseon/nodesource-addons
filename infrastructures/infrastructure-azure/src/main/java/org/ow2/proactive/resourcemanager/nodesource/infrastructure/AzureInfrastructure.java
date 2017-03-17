/*
 * ProActive Parallel Suite(TM):
 * The Open Source library for parallel and distributed
 * Workflows & Scheduling, Orchestration, Cloud Automation
 * and Big Data Analysis on Enterprise Grids & Clouds.
 *
 * Copyright (c) 2007 - 2017 ActiveEon
 * Contact: contact@activeeon.com
 *
 * This library is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation: version 3 of
 * the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * If needed, contact us to obtain a release under GPL Version 2 or 3
 * or a different license than the AGPL.
 */
package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.objectweb.proactive.core.ProActiveException;
import org.objectweb.proactive.core.node.Node;
import org.ow2.proactive.resourcemanager.exception.RMException;
import org.ow2.proactive.resourcemanager.nodesource.common.Configurable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


public class AzureInfrastructure extends InfrastructureManager {

    public static final String INSTANCE_ID_NODE_PROPERTY = "instanceId";

    public static final String INFRASTRUCTURE_TYPE = "azure";

    private static final Logger logger = Logger.getLogger(AzureInfrastructure.class);

    @Configurable(description = "The Azure clientId")
    protected String clientId = null;

    @Configurable(description = "The Azure secret key")
    protected String secret = null;

    @Configurable(description = "The Azure domain or tenantId")
    protected String domain = null;

    @Configurable(description = "The Azure subscriptionId to use (if not specified, it will try to use the default one")
    protected String subscriptionId = null;

    @Configurable(description = "Optional authentication endpoint from specific Azure environment")
    protected String authenticationEndpoint = null;

    @Configurable(description = "Optional management endpoint from specific Azure environment")
    protected String managementEndpoint = null;

    @Configurable(description = "Optional resource manager endpoint from specific Azure environment")
    protected String resourceManagerEndpoint = null;

    @Configurable(description = "Optional graph endpoint from specific Azure environment")
    protected String graphEndpoint = null;

    @Configurable(description = "Resource manager hostname or ip address")
    protected String rmHostname = generateDefaultRMHostname();

    @Configurable(description = "Connector-iaas URL")
    protected String connectorIaasURL = "http://" + generateDefaultRMHostname() + ":8080/connector-iaas";

    @Configurable(description = "Image (name or key)")
    protected String image = null;

    @Configurable(description = "Azure virtual machine size type (by default: 'Standard_D1_v2'")
    protected String vmSizeType = null;

    @Configurable(description = "The virtual machine Username")
    protected String vmUsername = null;

    @Configurable(description = "The virtual machine Password")
    protected String vmPassword = null;

    @Configurable(description = "A public key to allow SSH connection to the VM")
    protected String vmPublicKey = null;

    @Configurable(description = "The Azure resourceGroup to use (if not specified, the one from the image will be used")
    protected String resourceGroup = null;

    @Configurable(description = "The Azure Region to use (if not specified, the one from the image will be used")
    protected String region = null;

    @Configurable(description = "Total instance to create")
    protected int numberOfInstances = 1;

    @Configurable(description = "Total nodes to create per instance")
    protected int numberOfNodesPerInstance = 1;

    @Configurable(description = "Command used to download the worker jar")
    protected String downloadCommand = generateDefaultDownloadCommand();

    @Configurable(description = "Optional network CIDR to attach with new VM(s) (by default: '10.0.0.0/24')")
    protected String privateNetworkCIDR = null;

    @Configurable(description = "Optional flag to specify if the public IP(s) of the new VM(s) must be static ('true' by default)")
    protected boolean staticPublicIP = true;

    @Configurable(description = "Additional Java command properties (e.g. \"-Dpropertyname=propertyvalue\")")
    protected String additionalProperties = "-Dproactive.useIPaddress=true";

    protected ConnectorIaasController connectorIaasController = null;

    protected final Map<String, Set<String>> nodesPerInstances;

    /**
     * Default constructor
     */
    public AzureInfrastructure() {
        nodesPerInstances = Maps.newConcurrentMap();
    }

    @Override
    public void configure(Object... parameters) {

        logger.info("Validating parameters : " + parameters);
        validate(parameters);

        this.clientId = parameters[0].toString().trim();
        this.secret = parameters[1].toString().trim();
        this.domain = parameters[2].toString().trim();
        this.subscriptionId = parameters[3].toString().trim();
        this.authenticationEndpoint = parameters[4].toString().trim();
        this.managementEndpoint = parameters[5].toString().trim();
        this.resourceManagerEndpoint = parameters[6].toString().trim();
        this.graphEndpoint = parameters[7].toString().trim();
        this.rmHostname = parameters[8].toString().trim();
        this.connectorIaasURL = parameters[9].toString().trim();
        this.image = parameters[10].toString().trim();
        this.vmSizeType = parameters[11].toString().trim();
        this.vmUsername = parameters[12].toString().trim();
        this.vmPassword = parameters[13].toString().trim();
        this.vmPublicKey = parameters[14].toString().trim();
        this.resourceGroup = parameters[15].toString().trim();
        this.region = parameters[16].toString().trim();
        this.numberOfInstances = Integer.parseInt(parameters[17].toString().trim());
        this.numberOfNodesPerInstance = Integer.parseInt(parameters[18].toString().trim());
        this.downloadCommand = parameters[19].toString().trim();
        this.privateNetworkCIDR = parameters[20].toString().trim();
        this.staticPublicIP = Boolean.parseBoolean(parameters[21].toString().trim());
        this.additionalProperties = parameters[22].toString().trim();

        connectorIaasController = new ConnectorIaasController(connectorIaasURL, INFRASTRUCTURE_TYPE);

    }

    private void validate(Object[] parameters) {
        if (parameters == null || parameters.length < 23) {
            throw new IllegalArgumentException("Invalid parameters for AzureInfrastructure creation");
        }

        if (parameters[0] == null) {
            throw new IllegalArgumentException("Azure clientId must be specified");
        }

        if (parameters[1] == null) {
            throw new IllegalArgumentException("Azure secret key must be specified");
        }

        if (parameters[2] == null) {
            throw new IllegalArgumentException("Azure domain or tenantId must be specified");
        }

        if (parameters[8] == null) {
            throw new IllegalArgumentException("The Resource manager hostname must be specified");
        }

        if (parameters[9] == null) {
            throw new IllegalArgumentException("The connector-iaas URL must be specified");
        }

        if (parameters[10] == null) {
            throw new IllegalArgumentException("The image id must be specified");
        }

        if (parameters[12] == null) {
            throw new IllegalArgumentException("The virtual machine username must be specified");
        }

        if (parameters[13] == null) {
            throw new IllegalArgumentException("The virtual machine password must be specified");
        }

        if (parameters[17] == null) {
            throw new IllegalArgumentException("The number of instances to create must be specified");
        }

        if (parameters[18] == null) {
            throw new IllegalArgumentException("The number of nodes per instance to deploy must be specified");
        }

        if (parameters[19] == null) {
            throw new IllegalArgumentException("The download node.jar command must be specified");
        }

        if (parameters[22] == null) {
            parameters[22] = "";
        }
    }

    @Override
    public void acquireNode() {

        connectorIaasController.waitForConnectorIaasToBeUP();

        connectorIaasController.createAzureInfrastructure(getInfrastructureId(),
                                                          clientId,
                                                          secret,
                                                          domain,
                                                          subscriptionId,
                                                          authenticationEndpoint,
                                                          managementEndpoint,
                                                          resourceManagerEndpoint,
                                                          graphEndpoint,
                                                          false);

        String instanceTag = getInfrastructureId();
        Set<String> instancesIds;
        instancesIds = connectorIaasController.createAzureInstances(getInfrastructureId(),
                                                                    instanceTag,
                                                                    image,
                                                                    numberOfInstances,
                                                                    vmUsername,
                                                                    vmPassword,
                                                                    vmPublicKey,
                                                                    vmSizeType,
                                                                    resourceGroup,
                                                                    region,
                                                                    privateNetworkCIDR,
                                                                    staticPublicIP);

        logger.info("Instances ids created : " + instancesIds);

        /*
         * TODO: could be injected on boot just like OpenStack BUT scripts need to be customized
         * with instanceTag/id
         * and therefore we need to create the VMs one by one so the parallel instances creation
         * feature from Azure
         * will be lost.
         */
        for (String instanceId : instancesIds) {

            String fullScript = "-c '" + this.downloadCommand + ";nohup " +
                                generateDefaultStartNodeCommand(instanceId) + "  &'";

            connectorIaasController.executeScriptWithCredentials(getInfrastructureId(),
                                                                 instanceId,
                                                                 Lists.newArrayList(fullScript),
                                                                 vmUsername,
                                                                 vmPassword);
        }

    }

    @Override
    public void acquireAllNodes() {
        acquireNode();
    }

    @Override
    public void removeNode(Node node) throws RMException {

        String instanceId = getInstanceIdProperty(node);

        try {
            node.getProActiveRuntime().killNode(node.getNodeInformation().getName());

        } catch (Exception e) {
            logger.warn(e);
        }

        synchronized (this) {
            nodesPerInstances.get(instanceId).remove(node.getNodeInformation().getName());
            logger.info("Removed node : " + node.getNodeInformation().getName());

            if (nodesPerInstances.get(instanceId).isEmpty()) {
                connectorIaasController.terminateInstance(getInfrastructureId(), instanceId);
                nodesPerInstances.remove(instanceId);
                logger.info("Removed instance : " + instanceId);
            }
        }
    }

    @Override
    public void notifyAcquiredNode(Node node) throws RMException {

        String instanceId = getInstanceIdProperty(node);

        synchronized (this) {
            if (!nodesPerInstances.containsKey(instanceId)) {
                nodesPerInstances.put(instanceId, new HashSet<String>());
            }
            nodesPerInstances.get(instanceId).add(node.getNodeInformation().getName());
        }
    }

    @Override
    public String getDescription() {
        return "Handles nodes from the Amazon Elastic Compute Cloud Service.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getDescription();
    }

    private String generateDefaultRMHostname() {
        try {
            // best effort, may not work for all machines
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            logger.warn(e);
            return "localhost";
        }
    }

    private String generateDefaultDownloadCommand() {
        if (System.getProperty("os.name").contains("Windows")) {
            return "powershell -command \"& { (New-Object Net.WebClient).DownloadFile('http://" + this.rmHostname +
                   ":8080/rest/node.jar" + "', 'node.jar') }\"";
        } else {
            return "wget -nv http://" + this.rmHostname + ":8080/rest/node.jar";
        }
    }

    private String generateDefaultStartNodeCommand(String instanceId) {
        try {
            String rmUrlToUse = rmUrl;

            String protocol = rmUrlToUse.substring(0, rmUrlToUse.indexOf(':')).trim();
            return "java -jar node.jar -Dproactive.communication.protocol=" + protocol +
                   " -Dproactive.pamr.router.address=" + rmHostname + " -D" + INSTANCE_ID_NODE_PROPERTY + "=" +
                   instanceId + " " + additionalProperties + " -r " + rmUrlToUse + " -s " + nodeSource.getName() +
                   " -w " + numberOfNodesPerInstance;
        } catch (Exception e) {
            logger.error("Exception when generating the command, fallback on default value", e);
            return "java -jar node.jar -D" + INSTANCE_ID_NODE_PROPERTY + "=" + instanceId + " " + additionalProperties +
                   " -r " + rmUrl + " -s " + nodeSource.getName() + " -w " + numberOfNodesPerInstance;
        }
    }

    private String getInstanceIdProperty(Node node) throws RMException {
        try {
            return node.getProperty(INSTANCE_ID_NODE_PROPERTY);
        } catch (ProActiveException e) {
            throw new RMException(e);
        }
    }

    private String getInfrastructureId() {
        return nodeSource.getName().trim().replace(" ", "_").toLowerCase();
    }

}
