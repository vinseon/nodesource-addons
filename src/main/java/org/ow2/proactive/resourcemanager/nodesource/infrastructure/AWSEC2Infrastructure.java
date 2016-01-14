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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.objectweb.proactive.core.ProActiveException;
import org.objectweb.proactive.core.node.Node;
import org.ow2.proactive.resourcemanager.exception.RMException;
import org.ow2.proactive.resourcemanager.nodesource.common.Configurable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class AWSEC2Infrastructure extends InfrastructureManager {

	private static final String INSTANCE_ID_NODE_PROPERTY = "instanceId";

	private static final String INFRASTRUCTURE_TYPE = "aws-ec2";

	/** logger */
	private static final Logger logger = Logger.getLogger(AWSEC2Infrastructure.class);

	@Configurable(description = "The infrastructure unique id")
	protected String infrastructureId = "aws-ec2-proactive";

	@Configurable(description = "The AWS_AKEY")
	protected String aws_key = "AKIAIXZCJACIJA7YL3AQ";

	@Configurable(description = "The AWS_SKEY")
	protected String aws_secret_key = "fMWyE93klwSIzLxO8wTAnGlQNdNHWForaN6hMOq+";

	@Configurable(description = "Resource manager domain")
	protected String rmDomain = "trydev.activeeon.com";

	@Configurable(description = "Connector-iaas URL")
	protected String connectorIaasURL = "http://localhost:8081/connector-iaas";

	@Configurable(description = "Node to RM Connection Command")
	protected String nodeToRMConnectionCommand = "java -jar node.jar -Dproactive.communication.protocol=pamr -Dproactive.pamr.router.address=trydev.activeeon.com    -r pamr://4096/";

	@Configurable(description = "Instance tag")
	protected String instanceTag = "proactive-instance";

	@Configurable(description = "Image")
	protected String image = "eu-central-1/ami-bc1021a1";

	@Configurable(description = "Total instance to create")
	protected String numberOfInstances = "1";

	@Configurable(description = "Total nodes to create per instance")
	protected int numberOfNodesPerInstance = 1;

	@Configurable(description = "RAM")
	protected String ram = "512";

	@Configurable(description = "CPU")
	protected String cpu = "1";

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

		logger.info("Valiodating parameters : " + parameters);
		validate(parameters);

		this.infrastructureId = parameters[0].toString();
		this.aws_key = parameters[1].toString();
		this.aws_secret_key = parameters[2].toString();
		this.rmDomain = parameters[3].toString();
		this.connectorIaasURL = parameters[4].toString();
		this.nodeToRMConnectionCommand = parameters[5].toString();
		this.instanceTag = parameters[6].toString();
		this.image = parameters[7].toString();
		this.numberOfInstances = parameters[8].toString();
		this.numberOfNodesPerInstance = Integer.parseInt(parameters[9].toString());
		this.ram = parameters[10].toString();
		this.cpu = parameters[11].toString();

		String infrastructureJson = ConnectorIaasJSONTransformer.getInfrastructureJSON(infrastructureId,
				INFRASTRUCTURE_TYPE, aws_key, aws_secret_key);

		logger.info("creating infrastructure : " + infrastructureJson);

		connectorIaasClient = new ConnectorIaasClient(ConnectorIaasClient.jerseyClient, connectorIaasURL,
				infrastructureId, infrastructureJson);

		logger.info("Infrastructure created");

	}

	@Override
	public void acquireNode() {

		String instanceJson = ConnectorIaasJSONTransformer.getInstanceJSON(instanceTag, image, numberOfInstances, cpu,
				ram);

		logger.info("instanceJson : " + instanceJson);

		Set<String> instancesId = connectorIaasClient.createInstances(instanceJson);

		logger.info("instance id created : " + instancesId);

		for (String instanceId : instancesId) {
			List<String> scripts = Lists.newArrayList("wget " + this.rmDomain + "/rest/node.jar",
					"nohup " + this.nodeToRMConnectionCommand + " -DinstanceId=" + instanceId + "  &");
			String instanceScriptJson = ConnectorIaasJSONTransformer.getScriptInstanceJSON(scripts);

			executeScript(instanceId, instanceScriptJson);

		}

	}

	private void executeScript(String instanceId, String instanceScriptJson) {
		for (int index = 0; index < numberOfNodesPerInstance; index++) {
			String scriptResult = connectorIaasClient.runScriptOnInstances(instanceId, instanceScriptJson);

			logger.info("scriptResult for instance id " + instanceId + " : " + scriptResult);
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

	private void validate(Object[] parameters) {
		if (parameters == null || parameters.length < 10) {
			throw new IllegalArgumentException("Invalid parameters for EC2Infrastructure creation");
		}

		if (parameters[0] == null) {
			throw new IllegalArgumentException("The infrastructure id must be specified");
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
			throw new IllegalArgumentException("The Connection command must be specified");
		}

		if (parameters[6] == null) {
			throw new IllegalArgumentException("The instance tag must be specified");
		}

		if (parameters[7] == null) {
			throw new IllegalArgumentException("The image id must be specified");
		}

		if (parameters[8] == null) {
			throw new IllegalArgumentException("The number of instances to create must be specified");
		}

		if (parameters[9] == null) {
			throw new IllegalArgumentException("The number of nodes per instance to deploy must be specified");
		}

		if (parameters[10] == null) {
			throw new IllegalArgumentException("The amount of RAM must be specified");
		}

		if (parameters[11] == null) {
			throw new IllegalArgumentException("The CPU must be specified");
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
