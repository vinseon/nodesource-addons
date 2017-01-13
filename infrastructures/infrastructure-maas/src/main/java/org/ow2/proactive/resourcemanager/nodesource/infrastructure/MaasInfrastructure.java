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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import org.objectweb.proactive.core.ProActiveException;
import org.objectweb.proactive.core.node.Node;
import org.ow2.proactive.resourcemanager.exception.RMException;
import org.ow2.proactive.resourcemanager.nodesource.common.Configurable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Vincent Kherbache
 * @since 09/01/17
 */
public class MaasInfrastructure extends InfrastructureManager {

    public static final String INSTANCE_ID_NODE_PROPERTY = "instanceId";

    public static final String INFRASTRUCTURE_TYPE = "maas";

    private static final Logger logger = Logger.getLogger(MaasInfrastructure.class);

    @Configurable(description = "The MAAS API token")
    protected String token = null;

    @Configurable(description = "The MAAS endpoint")
    protected String endpoint = null;

    @Configurable(description = "Resource manager hostname or ip address")
    protected String rmHostname = generateDefaultRMHostname();

    @Configurable(description = "Connector-iaas URL")
    protected String connectorIaasURL = "http://" + generateDefaultRMHostname() + ":8080/connector-iaas";

    @Configurable(description = "Total instance to create")
    protected int numberOfInstances = 1;

    @Configurable(description = "The MAC address of the node")
    protected String macAddress = null;

    @Configurable(description = "The architecture of the node (possible values are 'amd64' and 'i386'")
    protected String architecture = "amd64";

    @Configurable(description = "The VIRSH address of the node")
    protected String powerAddress = null;

    @Configurable(description = "The VIRSH id of the node")
    protected String powerId = null;

    @Configurable(description = "The (optional) VIRSH pass of the node")
    protected String powerPass = null;

    @Configurable(description = "Total nodes to create per instance")
    protected int numberOfNodesPerInstance = 1;

    @Configurable(description = "Command used to download the worker jar")
    protected String downloadCommand = generateDefaultDownloadCommand();

    @Configurable(description = "Additional Java command properties (e.g. \"-Dpropertyname=propertyvalue\")")
    protected String additionalProperties = "-Dproactive.useIPaddress=true";

    protected ConnectorIaasController connectorIaasController = null;

    protected final Map<String, Set<String>> nodesPerInstances;


    /**
     * Default constructor
     */
    public MaasInfrastructure() {
        nodesPerInstances = Maps.newConcurrentMap();
    }

    @Override
    protected void configure(Object... parameters) {

        logger.info("Validating parameters : " + parameters);
        validate(parameters);

        this.token = parameters[0].toString().trim();
        this.endpoint = parameters[1].toString().trim();
        this.rmHostname = parameters[2].toString().trim();
        this.connectorIaasURL = parameters[3].toString().trim();
        this.macAddress = parameters[4].toString().trim();
        this.architecture = parameters[5].toString().trim();
        this.numberOfInstances = Integer.parseInt(parameters[6].toString().trim());
        this.numberOfNodesPerInstance = Integer.parseInt(parameters[7].toString().trim());
        this.powerAddress = parameters[8].toString().trim();
        this.powerId = parameters[9].toString().trim();
        this.powerPass = parameters[10].toString().trim();
        this.downloadCommand = parameters[11].toString().trim();
        this.additionalProperties = parameters[12].toString().trim();

        connectorIaasController = new ConnectorIaasController(connectorIaasURL, INFRASTRUCTURE_TYPE);
    }

    private void validate(Object[] parameters) {
        if (parameters == null || parameters.length < 12) {
            throw new IllegalArgumentException("Invalid parameters for MaasInfrastructure creation");
        }

        if (parameters[0] == null) {
            throw new IllegalArgumentException("MAAS API token must be specified");
        }

        if (parameters[1] == null) {
            throw new IllegalArgumentException("MAAS endpoint must be specified");
        }

        if (parameters[2] == null) {
            throw new IllegalArgumentException("The Resource manager hostname must be specified");
        }

        if (parameters[3] == null) {
            throw new IllegalArgumentException("The connector-iaas URL must be specified");
        }

        if (parameters[4] == null) {
            throw new IllegalArgumentException("The MAC address must be specified");
        }

        if (parameters[5] == null) {
            throw new IllegalArgumentException("The architecture must be specified");
        }

        if (parameters[6] == null) {
            throw new IllegalArgumentException("The number of instances to create must be specified");
        }

        if (parameters[7] == null) {
            throw new IllegalArgumentException(
                    "The number of nodes per instance to deploy must be specified");
        }

        if (parameters[8] == null) {
            throw new IllegalArgumentException("The VIRSH power address must be specified");
        }

        if (parameters[9] == null) {
            throw new IllegalArgumentException("The VIRSH power id must be specified");
        }

        if (parameters[10] == null) {
            parameters[10] = "";
        }

        if (parameters[11] == null) {
            throw new IllegalArgumentException("The download node.jar command must be specified");
        }

        if (parameters[12] == null) {
            parameters[12] = "";
        }

        if (parameters[13] == null) {
            parameters[13] = "";
        }
    }

    @Override
    public void acquireNode() {

        connectorIaasController.waitForConnectorIaasToBeUP();

        connectorIaasController.createInfrastructure(getInfrastructureId(), "", token, endpoint,
                false);

        String instanceId = getInfrastructureId();

        // Upload commissioning script first
        String fullScript = "-c '" + this.downloadCommand + ";nohup " +
                generateDefaultStartNodeCommand(instanceId) + "  &'";
        connectorIaasController.executeScript(getInfrastructureId(), instanceId, Lists.newArrayList(fullScript));

        /* TODO: need modifications on ConnectorIaasController side to pass all desired options
        Set<String> instancesIds;
        instancesIds = connectorIaasController.createInstancesWithOptions(getInfrastructureId(), instanceTag, image,
                    numberOfInstances, cores, ram, null, null, null, macAddresses);
        logger.info("Instances ids created : " + instancesIds);
        */
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
        return "Handles nodes from Metal As A Service (MAAS).";
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
            return "powershell -command \"& { (New-Object Net.WebClient).DownloadFile('" + this.rmHostname +
                    ":8080/rest/node.jar" + "', 'node.jar') }\"";
        } else {
            return "wget -nv " + this.rmHostname + ":8080/rest/node.jar";
        }
    }

    private String generateDefaultStartNodeCommand(String instanceId) {
        try {
            String rmUrlToUse = rmUrl;

            String protocol = rmUrlToUse.substring(0, rmUrlToUse.indexOf(':')).trim();
            return "java -jar node.jar -Dproactive.communication.protocol=" + protocol +
                    " -Dproactive.pamr.router.address=" + rmHostname + " -D" + INSTANCE_ID_NODE_PROPERTY + "=" +
                    instanceId + " " + additionalProperties + " -r " + rmUrlToUse + " -s " +
                    nodeSource.getName() + " -w " + numberOfNodesPerInstance;
        } catch (Exception e) {
            logger.error("Exception when generating the command, fallback on default value", e);
            return "java -jar node.jar -D" + INSTANCE_ID_NODE_PROPERTY + "=" + instanceId + " " +
                    additionalProperties + " -r " + rmUrl + " -s " + nodeSource.getName() + " -w " +
                    numberOfNodesPerInstance;
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
