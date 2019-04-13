package org.apache.loader.demo;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.loader.tools.configuration.ToolsConfiguration;
import org.apache.loader.tools.connection.ConnectionHandler;
import org.apache.loader.tools.connection.DConnection;
import org.apache.loader.tools.connection.impl.SftpConnection;
import org.apache.loader.tools.job.DJob;
import org.apache.loader.tools.job.JobHandler;
import org.apache.loader.tools.step.TransStep;
import org.apache.loader.tools.step.TransStepField;
import org.apache.loader.tools.step.TransStepHandler;
import org.apache.loader.tools.utils.EncryptTool;
import org.apache.loader.tools.utils.SqoopClientManager;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MJob;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loader Demo
 */
public class LoaderExample
{
    private static final Logger LOG = LoggerFactory.getLogger(LoaderExample.class);
    
    //TODO login file
    private static final String DEFAULT_LOGIN_FILE = System.getProperty("user.dir") + File.separator + "/config/login-info.xml";
    
    private static final String DEFAULT_LOGIN_KRB5FILE = System.getProperty("user.dir") + File.separator + "/config/krb5.conf";
    
    public void login() {
        login(null, null);
    }
    
    /**
     * user login
     * @param loginFile
     * @param krb5File
     */
    public void login(String loginFile, String krb5File) {
        if(StringUtils.isBlank(loginFile)) {
            loginFile = DEFAULT_LOGIN_FILE;
        }
        
        //read login file
        ToolsConfiguration.getInstance().initConfigByFile(loginFile);
        if(ToolsConfiguration.getInstance().isUseKeytab()) {
            if(StringUtils.isBlank(krb5File)) {
                krb5File = DEFAULT_LOGIN_KRB5FILE;
            }
            
            loadKRB5File(krb5File);
        }
        
        //get sqoop client
        SqoopClientManager.getInstance().initialize();
    }

    private void loadKRB5File(String krb5File)
    {
        System.setProperty("java.security.krb5.conf", krb5File);
    }
    
    private String encrypt(String password) {
        return EncryptTool.encrypt(password);
    }
    
    /**
     * create connection
     */
    public void createConnection()
    {
        LOG.info("Begin to create connection.");
        // 1. Create an instance of a certain connector. For example:sftp-connector.
        DConnection dc = generateDconnection();
        
        // 2.create connection
        createOrUpdateConnection(dc);
    }
    
    /**
     * Create an SFTP connection 's entity . 
     */
    private DConnection generateDconnection()
    {
        DConnection connection = new DConnection(SftpConnection.getInstance().getConnectorType());
        
        connection.setConnectionName("test-sftp-hdfs");
        
        connection.saveValue("connection.sftpServerIp", "10.1.0.1");
        connection.saveValue("connection.sftpServerPort", "22");
        connection.saveValue("connection.sftpUser", "root");
        //TODO password need encrypt
        connection.saveValue("connection.sftpPassword", encrypt("Root@123"));
       
        return connection;
    }
    
    /**
     * update connection
     */
    public void updateConnection(String connectionName)
    {
        // get an instance of a certain connector. For example:sftp-connector.
        ConnectionHandler handler = getConnectionHandler();
        DConnection connection = handler.getConnection(connectionName);
        
        //update ip
        connection.saveValue("connection.sftpServerIp", "10.1.1.2");
        connection.saveValue("connection.sftpPassword", encrypt("Root@123"));
       
        //invoke the common methods to finish the operations(create/update/query/delete)
        createOrUpdateConnection(connection);
    }
    
    /**
     * Create or update the connection.
     * When connection not exist in database, then create it.
     * or ,update it.
     * @param dConnection
     * @return
     */
    private DConnection createOrUpdateConnection(DConnection dConnection)
    {
        ConnectionHandler handler = getConnectionHandler(); 
        
        long connId = handler.getIDByName(dConnection.getConnectionName());
        
        try
        {
            if (connId == ConnectionHandler.INVALID_CONNECTION_ID)
            {
                LOG.warn("start to create a connection");
                handler.createConnection(dConnection);
            }
            else
            {
                LOG.warn("start to update a connection");
                dConnection.setConnectionId(connId);
                handler.updateConnection(dConnection);
            }
        }
        catch (ParseException e)
        {
            LOG.warn("create or update Connection fail. [exception] : " + e);
        }
        
        return handler.getConnection(dConnection.getConnectionName());
    }
    
    /**
     * query a connection
     */
    public DConnection queryConnection(String connectionName)
    {
        ConnectionHandler handler = getConnectionHandler();
        
        DConnection connection = handler.getConnection(connectionName);
        
        return connection;
    }
    
    /**
     * delete a connection
     */
    public void deleteConnection(String connectionName)
    {
        ConnectionHandler handler = getConnectionHandler();
        
        handler.deleteConnection(connectionName);
    }

    private ConnectionHandler getConnectionHandler()
    {
        ConnectionHandler handler = ConnectionHandler.getInstance();
        handler.initialize();
        
        return handler;
    }
    
    /**
     * create a job
     */
    public void createJob()
    {
        DJob dJob = generateDJob();
        
        try
        {
            createJob(dJob);
        }
        catch (ParseException e)
        {
            throw new SqoopException(LoaderExampleError.CREATE_JOB_FAIL, e);
        }
    }
    
    /**
     * Create a job By DJob.
     * @param dJob
     * @throws ParseException
     */
    private void createJob(DJob dJob)
        throws ParseException
    {
        JobHandler jobHandler = getJobHandler();
        
        Long jId = jobHandler.getJobIDByName(dJob.getJobName());
        if (jId == JobHandler.INVALID_JOB_ID)
        {
            LOG.info("Start to create job [ " + dJob.getJobName() + "] by connection[" + dJob.getConnectionName() + "]");
            jobHandler.createJob(dJob);
        }
        else
        {
            throw new SqoopException(LoaderExampleError.CREATE_JOB_FAIL, dJob.getJobName() + " already exist.");
        }
    }

    private JobHandler getJobHandler()
    {
        JobHandler jobHandler = JobHandler.getInstance();
        jobHandler.initialize();
        return jobHandler;
    }
    
    /**
     * update a job 
     */
    public void updateJob(String jobName)
    {
        JobHandler jobHandler = getJobHandler();
        
        // 5. Create job instance for a special scene by job template. For example: SFTP-To-HDFS.
        DJob dJob = jobHandler.getJob(jobName);
        
        dJob.saveValue("file.splitType", "FILE");
        dJob.saveValue("file.filterType", "WILDCARD");
        dJob.saveValue("file.pathFilter", "");
        dJob.saveValue("file.fileFilter", "*");
        
        try
        {
            jobHandler.updateJob(dJob);
        }
        catch (ParseException e)
        {
            throw new SqoopException(LoaderExampleError.UPDATE_JOB_FAIL);
        }
    }
    
    /**
     * Create a job entity for special scene 'sftp_to_hdfs' according job template.
     * 
     * @param jobName
     * @param jobTemplate  path of job's template
     */
    private DJob generateDJob()
    {
        //Whether read the transform information from job template file.When false ,you need to generate it later by yourself.
        String jobQueue = "default";
        String jobPriority = "NORMAL";
        DJob dJob = new DJob("sftp-hdfs-import", "IMPORT", SftpConnection.getInstance().getConnectorType(), jobQueue, jobPriority);
        //String jobType = "IMPORT"; // Create a job by template. The jobType is fixed. 
        String connName = "test-sftp-hdfs";//the connection must already exist.
        
        dJob.setConnectionName(connName);
        
        //2.From    
        dJob.saveValue("file.inputPath", "/opt/tempfile/ftp.txt");
        dJob.saveValue("file.splitType", "FILE");
        dJob.saveValue("file.filterType", "WILDCARD");
        dJob.saveValue("file.pathFilter", "");
        dJob.saveValue("file.fileFilter", "*");
        dJob.saveValue("file.encodeType", "UTF-8");
        dJob.saveValue("file.suffixName", "");
        dJob.saveValue("file.isCompressive", "false");
        
        //3.Transform  : create the transform steps for current scene.You need to identify different attributes in steps for your needs.
        String trans = createTransStep();// create the transform steps for current scene. [sftp_to_hdfs]
        dJob.setTrans(trans);
        
        //4.To
        dJob.setStorageType("HDFS");
        dJob.saveValue("output.outputDirectory", "/user/loader/test");
        dJob.saveValue("output.fileOprType", "OVERRIDE");
        dJob.saveValue("throttling.extractors", "3");
        //dJob.saveValue("throttling.extractorSize", "100");//note : throttling.extractors and throttling.extractorSize are mutually exclusive. 
        dJob.saveValue("output.fileType", "TEXT_FILE");
        
        return dJob;
    }
    
    /**
     * delete a job
     */
    public void deleteJob(String jobName)
    {
        JobHandler jobHandler = getJobHandler();
        
        try
        {
            jobHandler.deleteJob(jobName);
        }
        catch (SqoopException e)
        {
            throw new SqoopException(LoaderExampleError.UPDATE_JOB_FAIL, e);
        }
    }
    
    /**
     * query a job
     */
    public MJob queryJob(String jobName)
    {
        MJob job = null;
        try {
            SqoopClient sqoopClient = SqoopClientManager.getInstance().getSqoopClient();
            job = sqoopClient.getJob(jobName);
        } catch (Exception e) {
            
        }
        
        return job;
    }
    
    /**
     * submit a job
     */
    public void submitJob(String jobName)
    {
        JobHandler jobHandler = getJobHandler();
        // 5. invoke the common methods to finish the operations(create/update/query/delete)
        long jobId = jobHandler.getJobIDByName(jobName);
        jobHandler.jobStart(jobId);
    }
    
    /********************************************
    * (3)  Create/Update DJob's Transform Demo 
    * ******************************************
    */
    private String createTransStep()
    {
        String inputStepName = "CSV File Input";
        String normalStepName = "String Reverse";
        String outputStepName = "File Output";
        
        TransStepHandler stepHandler = TransStepHandler.getInstance();
        stepHandler.initialize();
        
        TransStep inputStep = stepHandler.getTransStepByName(inputStepName);
        if(inputStep == null) {
            return null;
        }
        
        inputStep.setAttrsValue("delimiter", ",");
        inputStep.setAttrsValue("line_delimiter", "");
        inputStep.setAttrsValue("file_name_as_field", "");
        inputStep.setAttrsValue("validate_field", "YES");
        inputStep.setAttrsValue("is_absolute_file_name", "false");
        
        ArrayList<TransStepField> stepFields = new ArrayList<TransStepField>();
        TransStepField stepField = null;
        
        stepField = stepHandler.getTransStepFieldByName(inputStep.getStepName());
        stepField.setFieldAttrsValue("position", "1");
        stepField.setFieldAttrsValue("field_name", "id");
        stepField.setFieldAttrsValue("type", "INTEGER");
        stepField.setFieldAttrsValue("date_format", "");
        stepField.setFieldAttrsValue("length", null);
        stepFields.add(stepField);
        
        stepField = stepHandler.getTransStepFieldByName(inputStep.getStepName());
        stepField.setFieldAttrsValue("position", "2");
        stepField.setFieldAttrsValue("field_name", "name");
        stepField.setFieldAttrsValue("type", "VARCHAR");
        stepField.setFieldAttrsValue("date_format", "");
        stepField.setFieldAttrsValue("length", "255");
        stepFields.add(stepField);
        //System.out.println("stepField--2 : " + stepField.toString());
        
        stepField = stepHandler.getTransStepFieldByName(inputStep.getStepName());
        stepField.setFieldAttrsValue("position", "3");
        stepField.setFieldAttrsValue("field_name", "content");
        stepField.setFieldAttrsValue("type", "VARCHAR");
        stepField.setFieldAttrsValue("date_format", "");
        stepField.setFieldAttrsValue("length", "255");
        stepFields.add(stepField);
        
        inputStep.setStepFields(stepFields);
        
        TransStep stringReverseStep = stepHandler.getTransStepByName(normalStepName);
        if(stringReverseStep == null) {
            return null;
        }
        
        ArrayList<TransStepField> stringReverse_StepFields = new ArrayList<TransStepField>();
        TransStepField stringReverse_StepField = stepHandler.getTransStepFieldByName(stringReverseStep.getStepName());
        
        stringReverse_StepField.setFieldAttrsValue("in_field", "content");
        stringReverse_StepField.setFieldAttrsValue("out_field", "new_content");
        stringReverse_StepField.setFieldAttrsValue("type", "VARCHAR");
        stringReverse_StepField.setFieldAttrsValue("length", "255");
        stringReverse_StepFields.add(stringReverse_StepField);
        
        stringReverseStep.setStepFields(stringReverse_StepFields);
        
        TransStep outputStep = stepHandler.getTransStepByName(outputStepName);
        if(outputStep == null) {
            return null;
        }
        
        outputStep.setAttrsValue("delimiter", ",");
        outputStep.setAttrsValue("line_delimiter", "\n");
        
        ArrayList<TransStepField> output_StepFields = new ArrayList<TransStepField>();
        TransStepField output_StepField = stepHandler.getTransStepFieldByName(outputStep.getStepName());
        output_StepField.setFieldAttrsValue("position", "1");
        output_StepField.setFieldAttrsValue("field_name", "id");
        output_StepField.setFieldAttrsValue("type", "INTEGER");
        output_StepField.setFieldAttrsValue("length", null);
        output_StepFields.add(output_StepField);
        
        output_StepField = stepHandler.getTransStepFieldByName(outputStep.getStepName());
        output_StepField.setFieldAttrsValue("position", "2");
        output_StepField.setFieldAttrsValue("field_name", "name");
        output_StepField.setFieldAttrsValue("type", "VARCHAR");
        output_StepField.setFieldAttrsValue("length", "255");
        output_StepFields.add(output_StepField);
        
        output_StepField = stepHandler.getTransStepFieldByName(outputStep.getStepName());
        output_StepField.setFieldAttrsValue("position", "3");
        output_StepField.setFieldAttrsValue("field_name", "new_content");
        output_StepField.setFieldAttrsValue("type", "VARCHAR");
        output_StepField.setFieldAttrsValue("length", "255");
        output_StepFields.add(output_StepField);
        
        outputStep.setStepFields(output_StepFields);
        
        ArrayList<TransStep> transSteps = new ArrayList<TransStep>();
        transSteps.add(0, inputStep);
        transSteps.add(1, stringReverseStep);
        transSteps.add(2, outputStep);
        
        String transInfo = stepHandler.generateTransInfo(transSteps);
        return transInfo;
    }
}

