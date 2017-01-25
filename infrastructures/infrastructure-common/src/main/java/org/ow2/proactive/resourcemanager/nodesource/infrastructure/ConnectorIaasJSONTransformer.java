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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;


public class ConnectorIaasJSONTransformer {

    private ConnectorIaasJSONTransformer() {
    }

    public static String getInfrastructureJSONWithEndPoint(String infrastructureId, String type, String username,
            String password, String endpoint, boolean toBeRemovedOnShutdown) {
        JSONObject credentials = new JSONObject();
        credentials.put("username", username);
        credentials.put("password", password);
        JSONObject infrastructure = new JSONObject().put("id", infrastructureId)
                                                    .put("type", type)
                                                    .put("credentials", credentials)
                                                    .put("toBeRemovedOnShutdown", toBeRemovedOnShutdown);

        if (endpoint != null && !endpoint.isEmpty()) {
            infrastructure = infrastructure.put("endpoint", endpoint);
        }

        return infrastructure.toString();
    }

    public static String getInstanceJSON(String tag, String image, String number, String cpu, String ram,
            String spotPrice, String securityGroupNames, String subnetId, String macAddresses) {
        JSONObject hardware = new JSONObject();
        hardware.put("minCores", cpu);
        hardware.put("minRam", ram);

        JSONObject options = new JSONObject();

        if (spotPrice != null && !spotPrice.isEmpty()) {
            options.put("spotPrice", spotPrice);
        }

        if (securityGroupNames != null && !securityGroupNames.isEmpty()) {
            String[] groups = securityGroupNames.split(",");
            Set<String> securityGroupNamesSet = new HashSet<String>(Arrays.asList(groups));
            options.put("securityGroupNames", securityGroupNamesSet);
        }

        if (subnetId != null && !subnetId.isEmpty()) {
            options.put("subnetId", subnetId);
        }

        if (macAddresses != null && !macAddresses.isEmpty()) {
            String[] addresses = macAddresses.split(",");
            List<String> addressesList = Arrays.asList(addresses);
            options.put("macAddresses", addressesList);
        }

        return new JSONObject().put("tag", tag)
                               .put("image", image)
                               .put("number", number)
                               .put("hardware", hardware)
                               .put("options", options)
                               .toString();
    }

    public static String getInstanceJSONWithPublicKeyAndScripts(String tag, String image, String number,
            String publicKeyName, String type, List<String> scripts) {
        JSONObject credentials = new JSONObject();
        credentials.put("publicKeyName", publicKeyName);
        JSONObject hardware = new JSONObject();
        hardware.put("type", type);
        JSONObject script = new JSONObject();
        script.put("scripts", new JSONArray(scripts));
        return new JSONObject().put("tag", tag)
                               .put("image", image)
                               .put("number", number)
                               .put("credentials", credentials)
                               .put("hardware", hardware)
                               .put("initScript", script)
                               .toString();
    }

    public static String getScriptInstanceJSONWithCredentials(List<String> scripts, String username, String password) {
        JSONObject scriptObject = new JSONObject().put("scripts", new JSONArray(scripts));
        if (username != null && password != null) {
            JSONObject credentials = new JSONObject();
            credentials.put("username", username);
            credentials.put("password", password);
            scriptObject = scriptObject.put("credentials", credentials);
        }
        return scriptObject.toString();
    }

}
