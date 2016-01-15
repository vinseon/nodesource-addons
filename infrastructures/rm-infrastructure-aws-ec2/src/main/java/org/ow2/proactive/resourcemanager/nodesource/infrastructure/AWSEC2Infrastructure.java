/*
 * ################################################################
 *
 * ProActive Parallel Suite(TM): The Java(TM) library for
 *    Parallel, Distributed, Multi-Core Computing for
 *    Enterprise Grids & Clouds
 *
 * Copyright (C) 1997-2015 INRIA/University of
 *                 Nice-Sophia Antipolis/ActiveEon
 * Contact: proactive@ow2.org or contact@activeeon.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation; version 3 of
 * the License.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 *
 * If needed, contact us to obtain a release under GPL Version 2 or 3
 * or a different license than the AGPL.
 *
 *  Initial developer(s):               The ProActive Team
 *                        http://proactive.inria.fr/team_members.htm
 *  Contributor(s):
 *
 * ################################################################
 * $$PROACTIVE_INITIAL_DEV$$
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
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AWSEC2Infrastructure extends InfrastructureManager {

	private static final String INSTANCE_ID_NODE_PROPERTY = "instanceId";

	private static final String INFRASTRUCTURE_TYPE = "aws-ec2";

	/** logger */
	private static final Logger logger = Logger.getLogger(AWSEC2Infrastructure.class);

	@Configurable(description = "The infrastructure unique id")
	protected String infrastructureId = null;

	@Configurable(description = "The AWS_AKEY")
	protected String aws_key = null;

	@Configurable(description = "The AWS_SKEY")
	protected String aws_secret_key = null;

	@Configurable(description = "Resource manager domain")
	protected String rmDomain = generateDefaultRMDomain();

	@Configurable(description = "Connector-iaas URL")
	protected String connectorIaasURL = "http://localhost:8081/connector-iaas";

	@Configurable(description = "Instance tag")
	protected String instanceTag = "proactive";

	@Configurable(description = "Image")
	protected String image = null;

	@Configurable(description = "Total instance to create")
	protected int numberOfInstances = 1;

	@Configurable(description = "Total nodes to create per instance")
	protected int numberOfNodesPerInstance = 1;

	@Configurable(description = "Target Operating System (windows/linux/mac)")
	protected String operatingSystem = "linux";

	@Configurable(description = "Command used to download the worker jar")
	protected String downloadCommand = generateDefaultDownloadCommand();

	@Configurable(description = "Additional java command properties (e.g. \"-Dpropertyname=propertyvalue\")")
	protected String additionalProperties = "";

	@Configurable(description = "RAM (in Mega Bytes))")
	protected int ram = 512;

	@Configurable(description = "CPU")
	protected int cpu = 1;

	@Configurable(description = "Alternate Resource Manager url (if we don't want to use the default one, for example in case of multi-protocol")
	protected String alternateRMURL = "";

	private ConnectorIaasClient connectorIaasClient;

	private final Map<String, Set<String>> nodesPerInstances;

	/**
	 * Default constructor
	 */
	public AWSEC2Infrastructure() {
		nodesPerInstances = Maps.newConcurrentMap();
	}

	@Override
	protected void configure(Object... parameters) {

		logger.info("Validating parameters : " + parameters);
		validate(parameters);

		this.infrastructureId = parameters[0].toString().trim();
		this.aws_key = parameters[1].toString().trim();
		this.aws_secret_key = parameters[2].toString().trim();
		this.rmDomain = parameters[3].toString().trim();
		this.connectorIaasURL = parameters[4].toString().trim();
		this.instanceTag = parameters[5].toString().trim();
		this.image = parameters[6].toString().trim();
		this.numberOfInstances = Integer.parseInt(parameters[7].toString().trim());
		this.numberOfNodesPerInstance = Integer.parseInt(parameters[8].toString().trim());
		this.operatingSystem = parameters[9].toString().trim();
		this.downloadCommand = parameters[10].toString().trim();
		this.additionalProperties = (parameters[11] != null )? parameters[11].toString().trim() : "";
		this.ram = Integer.parseInt(parameters[12].toString().trim());
		this.cpu = Integer.parseInt(parameters[13].toString().trim());
		this.alternateRMURL = parameters[14].toString().trim();

		String infrastructureJson = ConnectorIaasJSONTransformer.getInfrastructureJSON(infrastructureId,
				INFRASTRUCTURE_TYPE, aws_key, aws_secret_key);

		logger.info("creating infrastructure : " + infrastructureJson);

		connectorIaasClient = new ConnectorIaasClient(ConnectorIaasClient.jerseyClient, connectorIaasURL,
				infrastructureId, infrastructureJson);

		logger.info("Infrastructure created");

	}

	private void validate(Object[] parameters) {
		if (parameters == null || parameters.length < 15) {
			throw new IllegalArgumentException("Invalid parameters for EC2Infrastructure creation");
		}

		if (parameters[0] == null) {
			throw new IllegalArgumentException("The infrastructure id must be specified");
		} else if (parameters[0].toString().trim().contains(" ")) {
			throw new IllegalArgumentException("The infrastructure id must not contain spaces");
		}

		if (parameters[1] == null) {
			throw new IllegalArgumentException("EC2 key must be specified");
		}

		if (parameters[2] == null) {
			throw new IllegalArgumentException("EC2 secret key  must be specified");
		}

		if (parameters[3] == null) {
			throw new IllegalArgumentException("The Resource manager domain must be specified");
		}

		if (parameters[4] == null) {
			throw new IllegalArgumentException("The connector-iaas URL must be specified");
		}

		if (parameters[5] == null) {
			throw new IllegalArgumentException("The instance tag must be specified");
		}

		if (parameters[6] == null) {
			throw new IllegalArgumentException("The image id must be specified");
		}

		if (parameters[7] == null) {
			throw new IllegalArgumentException("The number of instances to create must be specified");
		}

		if (parameters[8] == null) {
			throw new IllegalArgumentException("The number of nodes per instance to deploy must be specified");
		}

		if (parameters[9] == null) {
			throw new IllegalArgumentException("The operating system must be specified");
		}

		if (parameters[10] == null) {
			throw new IllegalArgumentException("The download node.jar command must be specified");
		}

		if (parameters[11] == null) {
			parameters[11] = "";
		}

		if (parameters[12] == null) {
			throw new IllegalArgumentException("The amount of RAM must be specified");
		}

		if (parameters[13] == null) {
			throw new IllegalArgumentException("The CPU must be specified");
		}

		if (parameters[14] == null) {
			parameters[14] = "";
		}

	}

	@Override
	public void acquireNode() {

		String instanceJson = ConnectorIaasJSONTransformer.getInstanceJSON(instanceTag, image, "" + numberOfInstances,
				"" + cpu, "" + ram);

		logger.info("InstanceJson : " + instanceJson);

		Set<String> instancesIds = connectorIaasClient.createInstances(instanceJson);

		logger.info("Instances ids created : " + instancesIds);

		for (String instanceId : instancesIds) {
			List<String> scripts = Lists.newArrayList(this.downloadCommand,
					"nohup " + generateDefaultStartNodeCommand(instanceId) + "  &");
			String instanceScriptJson = ConnectorIaasJSONTransformer.getScriptInstanceJSON(scripts);

			executeScript(instanceId, instanceScriptJson);
		}

	}

	private void executeScript(String instanceId, String instanceScriptJson) {
		String scriptResult = null;
		try {
			scriptResult = connectorIaasClient.runScriptOnInstance(instanceId, instanceScriptJson);
			if (logger.isDebugEnabled()) {
				logger.debug("Executed successfully script for instance id :" + instanceId + "\nScript contents : " + instanceScriptJson + " \nResult : " + scriptResult);
			} else {
				logger.info("Script result for instance id " + instanceId + " : " + scriptResult);
			}
		} catch (Exception e) {
			logger.error("Error while executing script :\n" + instanceScriptJson, e);
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

		} catch (ProActiveException e) {
			throw new RMException(e);
		}

		synchronized (this) {
			nodesPerInstances.get(instanceId).remove(node.getNodeInformation().getName());

			if (nodesPerInstances.get(instanceId).isEmpty()) {
				connectorIaasClient.terminateInstance(instanceId);
			}
		}

	}

	@Override
	protected void notifyAcquiredNode(Node node) throws RMException {

		String instanceId = getInstanceIdProperty(node);

		synchronized (this) {
			if (!nodesPerInstances.containsKey(instanceId)) {
				nodesPerInstances.put(instanceId, new HashSet<String>());
			}
			nodesPerInstances.get(instanceId).add(node.getNodeInformation().getName());
		}
	}

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


	private String generateDefaultRMDomain() {
		try {
			// best effort, may not work for all machines
			return InetAddress.getLocalHost().getCanonicalHostName();
		} catch (UnknownHostException e) {
			logger.warn(e);
			return "localhost";
		}
	}

	private String generateDefaultDownloadCommand() {
		if ("windows".equals(operatingSystem)) {
			return "powershell -command \"& { (New-Object Net.WebClient).DownloadFile('" + this.rmDomain
					+ "/rest/node.jar" + "', 'node.jar') }\"";
		} else {
			return "wget " + this.rmDomain + "/rest/node.jar";
		}
	}

	private String generateDefaultStartNodeCommand(String instanceId) {
		try {
			String rmUrlToUse = rmUrl;
			if ((alternateRMURL != null) && (!alternateRMURL.isEmpty())) {
				rmUrlToUse = alternateRMURL;
			}
			String protocol = rmUrlToUse.substring(0, rmUrlToUse.indexOf(':')).trim();
			return "java -jar node.jar -Dproactive.communication.protocol=" + protocol
					+ " -Dproactive.pamr.router.address=" + rmDomain + " -DinstanceId=" + instanceId + " " + additionalProperties + " -r " + rmUrlToUse + " -s " + nodeSource.getName()
					+ " -w " + numberOfNodesPerInstance;
		} catch (Exception e) {
			logger.error("Exception when generating the command, fallback on default value", e);
			return "java -jar node.jar -DinstanceId=" + instanceId + " " + additionalProperties + " -r " + rmUrl + " -s " + nodeSource.getName() + " -w " + numberOfNodesPerInstance;
		}
	}

	private String getInstanceIdProperty(Node node) throws RMException {
		try {
			return node.getProperty(INSTANCE_ID_NODE_PROPERTY);
		} catch (ProActiveException e) {
			throw new RMException(e);
		}
	}

}