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

import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.ConfigurationContextFactory;
import org.apache.log4j.Logger;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.ActivityDocumentType;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.Application_Type;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.CreateActivity;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.CreateActivityResponse;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.CreateActivityType;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.EndpointReferenceType;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.FileName_Type;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.GetActivityStatusResponseType;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.GetActivityStatuses;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.GetActivityStatusesResponse;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.GetActivityStatusesType;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.HPCProfileApplication_Type;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.JobDefinition_Type;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.JobDescription_Type;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.TerminateActivities;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.TerminateActivitiesResponse;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.TerminateActivitiesType;
import org.ggf.schemas.bes._2006._08.bes_factory.HPCBPServiceStub.TerminateActivityResponseType;
import org.ogf.hpcbp.Client;


/**
 *
 * This class communicates to the Windows HPC Scheduler web service, generates jsdl documents
 * from the provided command and submits jobs to the scheduler.
 *
 */
public class WinHPCDeployer {

    /** logger */
    protected static Logger logger = Logger.getLogger(WinHPCDeployer.class);

    private HPCBPServiceStub proxy;

    public WinHPCDeployer(String axisRep, String serviceURL, String username, String password) throws Exception {
        // Creates the proxy to the HPCBP service
        String confFile = axisRep + System.getProperty("file.separator") + "conf" +
                          System.getProperty("file.separator") + "axis2.xml";
        ConfigurationContext config = ConfigurationContextFactory.createConfigurationContextFromFileSystem(axisRep,
                                                                                                           confFile);
        proxy = new HPCBPServiceStub(config, serviceURL);
        Client.WSSUsername = username;
        Client.WSSPassword = password;
        proxy._getServiceClient().engageModule("rampart");
    }

    /**
     *
     * Creates JSDL document
     *
     */
    public static JobDefinition_Type createJSDLDocument(String command) {
        JobDefinition_Type jobDefinition = new JobDefinition_Type();
        JobDescription_Type jobDescription = new JobDescription_Type();
        Application_Type application = new Application_Type();
        HPCProfileApplication_Type hpcApplication = new HPCProfileApplication_Type();
        FileName_Type executable = new FileName_Type();
        executable.setString(command);
        hpcApplication.setExecutable(executable);
        application.setHPCProfileApplication(hpcApplication);

        jobDescription.setApplication(application);
        jobDefinition.setJobDescription(jobDescription);

        return jobDefinition;
    }

    public EndpointReferenceType createActivity(JobDefinition_Type jobDefinitionType) {
        return sendCreateActivityMessage(jobDefinitionType);
    }

    private EndpointReferenceType sendCreateActivityMessage(JobDefinition_Type jobDefinitionType) {

        ActivityDocumentType activityDocumentType = new ActivityDocumentType();
        activityDocumentType.setJobDefinition(jobDefinitionType);
        CreateActivityType createActivityType = new CreateActivityType();
        createActivityType.setActivityDocument(activityDocumentType);
        CreateActivity activity = new CreateActivity();
        activity.setCreateActivity(createActivityType);
        try {
            CreateActivityResponse response = proxy.CreateActivity(activity);
            return response.getCreateActivityResponse().getActivityIdentifier();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the status of one or more activity
     *
     */
    public GetActivityStatusResponseType[] getActivityStatuses(EndpointReferenceType[] activities) {
        try {
            GetActivityStatusesType getActivityStatusesType = new GetActivityStatusesType();
            getActivityStatusesType.setActivityIdentifier(activities);
            GetActivityStatuses getActivityStatuses = new GetActivityStatuses();
            getActivityStatuses.setGetActivityStatuses(getActivityStatusesType);
            GetActivityStatusesResponse response = proxy.GetActivityStatuses(getActivityStatuses);
            return response.getGetActivityStatusesResponse().getResponse();
        } catch (Exception ex) {
            logger.warn(ex.getMessage(), ex);
            return new GetActivityStatusResponseType[0];
        }
    }

    /**
     *  Terminates activities
     */
    public TerminateActivityResponseType[] terminateActivity(EndpointReferenceType[] activityEPRs) {
        try {
            TerminateActivitiesType terminateActivitiesType = new TerminateActivitiesType();
            terminateActivitiesType.setActivityIdentifier(activityEPRs);
            TerminateActivities terminateActivities = new TerminateActivities();
            terminateActivities.setTerminateActivities(terminateActivitiesType);
            TerminateActivitiesResponse response = proxy.TerminateActivities(terminateActivities);
            return response.getTerminateActivitiesResponse().getResponse();
        } catch (Exception ex) {
            logger.warn(ex.getMessage(), ex);
            return new TerminateActivityResponseType[0];
        }
    }

}
