/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Created by Benoit COLAS - 08/10/2014
 * 
 ******************************************************************************/

package org.pentaho.di.trans.steps.sforcebulkutils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import javax.xml.namespace.QName;
import javax.xml.soap.SOAPException;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.steps.sforcebulkinput.SalesforceBulkInputMeta;
import org.w3c.dom.Element;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.GetUserInfoResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.LoginResult;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.fault.ExceptionCode;
import com.sforce.soap.partner.fault.LoginFault;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.bind.XmlObject;
import com.sforce.ws.wsdl.Constants;

public class SalesforceBulkConnection {
	private static Class<?> PKG = SalesforceBulkConnectionUtils.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
	
	private String url;
	private String username;
	private String password;
	private String module;
	private int timeout;
	private int rowMax;
	private int batchSize;
	private String condition;
	
	private PartnerConnection binding;
	private LoginResult loginResult;
	private GetUserInfoResult userInfo;
	private String sql;
	private Date serverTimestamp;
	private QueryResult  qr ;
	private GregorianCalendar startDate;
	private GregorianCalendar endDate;
	private int queryResultSize;
	private int recordsCount;
	private boolean useCompression;
	private boolean rollbackAllChangesOnError;
	private boolean queryAll;
	
	private BulkConnection bulkConnection;
	private JobInfo job;
	private List<BatchInfo> batchInfos;
	
	private CSVReader csvr;
	private List<String> resultHeader;

	private LogChannelInterface	log;
	
	
	/**
	 * Construct a new Salesforce Connection
	 */
	public SalesforceBulkConnection(LogChannelInterface logInterface, String url, String username, String password) throws KettleException {
		this.log = logInterface;
		this.url=url;
		setUsername(username);
		setPassword(password);
		setTimeOut(0);
	
		this.binding=null;
		this.loginResult = null;
		this.userInfo = null;
		this.sql=null;
		this.serverTimestamp=null;
		this.qr=null;
		this.condition=null;
		this.startDate=null;
		this.endDate=null;
		this.queryResultSize=0;
		this.recordsCount=0;
		setUsingCompression(false);
		rollbackAllChangesOnError(false);
		
		this.bulkConnection = null;
		this.csvr = null;
		this.batchInfos = null;
		
		// check target URL
		if(Const.isEmpty(getURL()))	throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.TargetURL"));
		
		// check username
		if(Const.isEmpty(getUsername())) throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.UsernameMissing"));
				
		if(log.isDetailed()) logInterface.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkConnection.Log.NewConnection"));
	}
    public boolean isRollbackAllChangesOnError() {
    	return this.rollbackAllChangesOnError;
    }
    public void rollbackAllChangesOnError(boolean value) {
    	this.rollbackAllChangesOnError=value;
    }
    public boolean isQueryAll() {
    	return this.queryAll;
    }
    public void queryAll(boolean value) {
    	this.queryAll=value;
    }
	public void setCalendar(int recordsFilter,GregorianCalendar startDate, GregorianCalendar endDate) throws KettleException {
		this.startDate=startDate;
		this.endDate=endDate;
		if(this.startDate==null || this.endDate==null) throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.Error.EmptyStartDateOrEndDate"));
		if(this.startDate.getTime().compareTo(this.endDate.getTime())>=0) throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.Error.WrongDates"));
		// Calculate difference in days        
		long diffDays = (this.startDate.getTime().getTime() - this.endDate.getTime().getTime()) / (24 * 60 * 60 * 1000);
		if(diffDays>30) {
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.Error.StartDateTooOlder"));
		}
	}
	public void setCondition(String condition) {
		this.condition=condition;
	}
	public String getCondition() {
		return this.condition;
	}
	public void setSQL(String sql) {
		this.sql=sql;
	}
	public void setModule(String module) {
		this.module=module;
	}
	public String getURL() {
		return this.url;
	}
	public String getSQL(){
		return this.sql;
	}
	public Date getServerTimestamp(){
		return this.serverTimestamp;
	}
	public String getModule() {
		return this.module;
	}
	public QueryResult getQueryResult() {
		return this.qr;
	}
	
	public PartnerConnection getBinding() {
	    return this.binding;
	}
	
	public PartnerConnection createBinding( ConnectorConfig config ) throws ConnectionException {
	    if ( this.binding == null ) {
	      this.binding = new PartnerConnection( config );
	    }
	    return this.binding;
	}
	
	public void setRowMax(int rowMax){
		this.rowMax=rowMax;
	}
	public int getRowMax(){
		return this.rowMax;
	}
	public void setTimeOut(int timeout){
		this.timeout=timeout;
	}
	public int getTimeOut(){
		return this.timeout;
	}
	public boolean isUsingCompression() {
		return this.useCompression;
	}
	public void setUsingCompression(boolean useCompression) {
		this.useCompression=useCompression;
	}
	public String getUsername() {
		return this.username;
	}

	public void setUsername(String value) {
		this.username = value;
	}

	public String getPassword() {
		return this.password;
	}

	public void setPassword(String value) {
		this.password = value;
	}


	public void connect() throws KettleException{
		try {
		  ConnectorConfig partnerConfig = new ConnectorConfig();
		  partnerConfig.setAuthEndpoint( getURL() );
		  partnerConfig.setUsername( getUsername() );
		  partnerConfig.setPassword( getPassword() );

	      String proxyUrl = System.getProperty( "http.proxyHost", null );
	      if ( StringUtils.isNotEmpty( proxyUrl ) ) {
	        int proxyPort = Integer.parseInt( System.getProperty( "http.proxyPort", "80" ) );
	        String proxyUser = System.getProperty( "http.proxyUser", null );
	        String proxyPassword = Encr.decryptPasswordOptionallyEncrypted( System.getProperty( "http.proxyPassword", null ) );
	        partnerConfig.setProxy( proxyUrl, proxyPort );
	        partnerConfig.setProxyUsername( proxyUser );
	        partnerConfig.setProxyPassword( proxyPassword );
	      }
	      
	      // Creating the connection automatically handles login and stores
	      // the session in partnerConfig
	      createBinding(partnerConfig);
	      
	      ConnectorConfig config = new ConnectorConfig();
	      config.setSessionId(partnerConfig.getSessionId());
	      if ( StringUtils.isNotEmpty( proxyUrl ) ) {
		    int proxyPort = Integer.parseInt( System.getProperty( "http.proxyPort", "80" ) );
		    String proxyUser = System.getProperty( "http.proxyUser", null );
		    String proxyPassword = Encr.decryptPasswordOptionallyEncrypted( System.getProperty( "http.proxyPassword", null ) );
		    config.setProxy( proxyUrl, proxyPort );
		    config.setProxyUsername( proxyUser );
		    config.setProxyPassword( proxyPassword );
		  }
	      // The endpoint for the Bulk API service is the same as for the normal
	      // SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
	      String soapEndpoint = partnerConfig.getServiceEndpoint();
	      String apiVersion = SalesforceBulkConnectionUtils.LIB_VERSION;
	      String restEndpoint = soapEndpoint.substring(0,soapEndpoint.indexOf("Soap/"))+"async/" + apiVersion;
	      config.setRestEndpoint(restEndpoint);
	      config.setCompression( isUsingCompression() );
	    
	      // Set timeout
	      if ( getTimeOut() > 0 ) {
	        if ( log.isDebug() ) {
	          log.logDebug( BaseMessages.getString( PKG, "SalesforceBulkConnection.Log.SettingTimeout", "" + this.timeout ) );
	        }
	        config.setConnectionTimeout( getTimeOut() );
	        config.setReadTimeout( getTimeOut() );
	      }
	    
	      if ( log.isDetailed() ) {
	        log.logDetailed( BaseMessages.getString( PKG, "SalesforceBulkConnexion.Log.LoginURL", config.getAuthEndpoint() ) );
	      }
	        
	      /*if ( isRollbackAllChangesOnError() ) {
	        // Set the SOAP header to rollback all changes
	        // unless all records are processed successfully.
	        pConnection.setAllOrNoneHeader( true );
	      }*/
	        
	      // Attempt the login giving the user feedback
	      if ( log.isDetailed() ) {
	        log.logDetailed( BaseMessages.getString( PKG, "SalesforceBulkConnection.Log.LoginNow" ) );
	        log.logDetailed( "----------------------------------------->" );
	        log.logDetailed( BaseMessages.getString( PKG, "SalesforceBulkConnection.Log.LoginURL", getURL() ) );
	        log.logDetailed( BaseMessages.getString( PKG, "SalesforceBulkConnection.Log.LoginUsername", getUsername() ) );
	        if ( getModule() != null ) {
	          log.logDetailed( BaseMessages.getString( PKG, "SalesforceBulkConnection.Log.LoginModule", getModule() ) );
	        }
	        log.logDetailed( "<-----------------------------------------" );
	      }
	        
	      this.bulkConnection = new BulkConnection(config);
	 	  if(log.isDetailed()) log.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkConnection.Log.NewConnection"));
		
		} catch (AsyncApiException aae) {
			aae.printStackTrace();
		} catch (LoginFault ex) {
			// The LoginFault derives from AxisFault
            ExceptionCode exCode = ex.getExceptionCode();
            if (exCode == ExceptionCode.FUNCTIONALITY_NOT_ENABLED ||
                exCode == ExceptionCode.INVALID_CLIENT ||
                exCode == ExceptionCode.INVALID_LOGIN ||
                exCode == ExceptionCode.LOGIN_DURING_RESTRICTED_DOMAIN ||
                exCode == ExceptionCode.LOGIN_DURING_RESTRICTED_TIME ||
                exCode == ExceptionCode.ORG_LOCKED ||
                exCode == ExceptionCode.PASSWORD_LOCKOUT ||
                exCode == ExceptionCode.SERVER_UNAVAILABLE ||
                exCode == ExceptionCode.TRIAL_EXPIRED ||
                exCode == ExceptionCode.UNSUPPORTED_CLIENT) {
            	throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.InvalidUsernameOrPassword"));
            }
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.Connection"), ex);
		} catch (ConnectionException ce) {
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.Connection"), ce);
		} catch(Exception e){
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.Connection"), e);
		}
	}
	
	public void initJob(String sobjectType, OperationEnum soperation) throws KettleException
	{
	  try {
		if (log.isDetailed()) log.logDetailed("Creating Job("+getModule()+")");
		this.job = new JobInfo();
	    this.job.setObject(sobjectType);
	      
	    this.job.setOperation(soperation);
	    this.job.setConcurrencyMode(ConcurrencyMode.Parallel);
	    this.job.setContentType(ContentType.CSV);
	    
	    if (log.isDetailed()) log.logDetailed("Creating Job ...");
	    this.job = this.bulkConnection.createJob(this.job);
	    assert this.job.getId() != null;
	    
	    if (log.isDetailed()) log.logDetailed("Job ID : "+this.job.getId());
	    
	    this.job = this.bulkConnection.getJobStatus(this.job.getId());
	    
	    if (log.isDetailed()) log.logDetailed("Job Status : "+this.job.getState().toString());
	  } catch (AsyncApiException aae) {
		throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.Job"), aae);
	  }
		
	}
	
	public String createBatch(java.io.InputStream inputStream) throws KettleException {
	  String batchId = null;
	  if (this.batchInfos==null) this.batchInfos = new ArrayList<BatchInfo>();
	  try {
		  BatchInfo batchInfo = this.bulkConnection.createBatchFromStream(this.job, inputStream);
		  if (log.isDetailed()) log.logDetailed(batchInfo.toString());
		  batchId = batchInfo.getId();
		  this.batchInfos.add(batchInfo);
	  }
	  catch (Exception e) {
		throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.Batch"), e);
	  }
	  finally {
		try {
		  inputStream.close();
		}
		catch (IOException ioe) { }
	  }
	  return batchId;
	}
	
	public void closeJob() throws KettleException {
	  try {
		JobInfo job = new JobInfo();
		job.setId(this.job.getId());
		job.setState(JobStateEnum.Closed);
		this.bulkConnection.updateJob(job);
	  } catch (AsyncApiException aae) {
	    throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkDelete.Error.Job"), aae);
      }
	}
	
	public void awaitCompletion() throws KettleException {
      try {
		long sleepTime = 1000L;
		Set<String> incomplete = new HashSet<String>();
		for (BatchInfo bi : this.batchInfos) {
			incomplete.add(bi.getId());
		}
		while (!incomplete.isEmpty()) {
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {}
			sleepTime = 10000L;
			BatchInfo[] statusList = this.bulkConnection.getBatchInfoList(this.job.getId()).getBatchInfo();
			for (BatchInfo b : statusList) {
				if (b.getState() == BatchStateEnum.Completed
						|| b.getState() == BatchStateEnum.Failed) {
					if (incomplete.remove(b.getId())) {
						if (b.getState() == BatchStateEnum.Failed)
							log.logError("Batch failed " + b);
						if (log.isDetailed()) log.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkConnection.Log.BatchStatus"), b.getId(), b.getState());
					}
				}
				else 
					if (log.isDetailed()) log.logDetailed("Batch "+b.getId()+" processing time : " + b.getTotalProcessingTime());
			}
		}
      } catch (AsyncApiException aae) {
  	    throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.Job"), aae);
      }
	}

	public List checkResults() throws KettleException {
	  List<Map> results = new ArrayList<Map>();
	  for (BatchInfo b : this.batchInfos) {
	    List<Map> batchResults = checkResults(b.getId());
		results.addAll(batchResults);
	  }
	  return results;
	}

	public List checkResults(String batchId) throws KettleException {
	  List<Map> results = new ArrayList<Map>();
	  try {
		this.csvr = new CSVReader(this.bulkConnection.getBatchResultStream(this.job.getId(), batchId));
		List<String> resultHeader = this.csvr.nextRecord();
		int resultCols = resultHeader.size();
			  
		List<String> row;
		while ((row=this.csvr.nextRecord()) != null) {
		  Map<String, String> resultInfo = new HashMap<String, String>();
		  for (int i=0; i<resultCols; i++) {
		  resultInfo.put(resultHeader.get(i), row.get(i));
		  }
		  results.add(resultInfo);
		}
	  } catch (AsyncApiException aae) {
	    throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.Job"), aae);
	  } catch (IOException ioe) {
		throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.IO"), ioe);
	  }  
	  return results;
	}
	 
	public void query(boolean queryAll) throws KettleException {
		 
		if(getBinding()==null)  throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.CanNotGetBinding"));
		
	    try {
			// check if we can query this Object
			DescribeSObjectResult describeSObjectResult = getBinding().describeSObject(getModule());
			if (describeSObjectResult == null) throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.GettingObject"));  
			if(!describeSObjectResult.isQueryable()) throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.ObjectNotQueryable",getModule()));
		    			        
		    if (getSQL()!=null && log.isDetailed()) log.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkConnection.Log.SQLString") + " : " +  getSQL());        
		  
		    initJob(getModule(), (queryAll?OperationEnum.queryAll:OperationEnum.query));
		      
		    ByteArrayInputStream bout = new ByteArrayInputStream(getSQL().getBytes());
		    createBatch(bout);
		    closeJob();
		    
		    awaitCompletion();
		    
		    for (BatchInfo info : this.batchInfos) {
		      QueryResultList qrl = this.bulkConnection.getQueryResultList(this.job.getId(), info.getId());
		      String[] queryResults = qrl.getResult();
		    
		      if (queryResults != null) {
		        for (String resultId : queryResults) {
		        	if (log.isDetailed()) log.logDetailed("Result Id : "+resultId);
		        	this.csvr = new CSVReader(this.bulkConnection.getQueryResultStream(job.getId(), info.getId(), resultId), "UTF-8");
		        	if (this.getRowMax()>0 && log.isDetailed()) log.logDetailed("setMaxRowsInFile("+this.getRowMax()+")");
		        	if(this.getRowMax()>0)
		        		this.csvr.setMaxRowsInFile(this.getRowMax());
		        	this.csvr.setMaxCharsInFile(Integer.MAX_VALUE);
		        	this.resultHeader = this.csvr.nextRecord();
		        }
		      }
		    }
	    } catch (AsyncApiException aae) {
	    	log.logError(Const.getStackTracker(aae));
	    	aae.printStackTrace();
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.Query"),aae);
		} catch(Exception e){
			log.logError(Const.getStackTracker(e));
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.Query"),e);
		}
	 }
	 
	 public void close() throws KettleException
	 {
		 try {
			 this.job.setState(JobStateEnum.Closed);
		     this.bulkConnection.updateJob(this.job);
		     if(log.isDetailed()) log.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkConnection.Log.JobClosed"));
		 }
	     catch (AsyncApiException aae) {
	    	 
	     }
		 catch(Exception e){
			 throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.ClosingJob"),e);
		 };
	 }
	 public int getQueryResultSize() {
		 return this.queryResultSize;
	 }
	 public int getRecordsCount(){
		return this.recordsCount;
	 }

	 public Map<String, String> getNextRecord() throws IOException {
		 List<String> row = this.csvr.nextRecord();
		 if (row == null) return null;
         Map<String, String> record = new HashMap<String, String>();
         for (int i = 0; i < resultHeader.size(); i++) {
        	 record.put(this.resultHeader.get(i), row.get(i));
         }
		 return record;
	 }
	 
	 public String getRecordValue(Map<String, String> row, String fieldname) throws KettleException {
		 return row.get(fieldname);
	 }
	  
	// Get SOQL meta data (not a Good way but i don't see any other way !)
	  // TODO : Go back to this one
	  // I am sure there is an easy way to return meta for a SOQL result
	  public XmlObject[] getElements() throws Exception {
	    // Query first
	    this.qr = getBinding().query( getSQL() );
	    // and then return records
	    SObject con = getQueryResult().getRecords()[0];
	    if ( con == null ) {
	      return null;
	    }
	    return getChildren( con );
	}
	 
	public String[] getAllAvailableObjects(boolean OnlyQueryableObjects) throws KettleException
	{
	  DescribeGlobalResult dgr=null;
	  List<String> objects = null;
	  DescribeGlobalSObjectResult[] sobjectResults=null;
	  try  {
		  // Get object
		  dgr = getBinding().describeGlobal();
		  // let's get all objects
	      sobjectResults = dgr.getSobjects();
	      int nrObjects= dgr.getSobjects().length;
	      
	      objects = new ArrayList<String>();
	      
	      for(int i=0; i<nrObjects; i++) {
	    	  DescribeGlobalSObjectResult o = dgr.getSobjects()[i];
	    	  if((OnlyQueryableObjects && o.isQueryable()) || !OnlyQueryableObjects) {
	    		  objects.add(o.getName());
	    	  }
	      }
	      return  (String[]) objects.toArray(new String[objects.size()]);
	   } catch(Exception e){
		   throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.GettingModules"),e);
	   }finally  {
		   if(dgr!=null) dgr=null;
		   if(objects!=null) {
			   objects.clear();
			   objects=null;
		   }
		   if(sobjectResults!=null) {
			   sobjectResults=null;
		   }
	   }
	}  
	public Field[] getObjectFields(String objectName) throws KettleException
	{
		DescribeSObjectResult describeSObjectResult=null;
		try  {
			// Get object
			describeSObjectResult = getBinding().describeSObject(objectName);
			if(describeSObjectResult==null) return null;
     
			if(!describeSObjectResult.isQueryable()){
				throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.ObjectNotQueryable",this.module));
			}else{
				// we can query this object
				return  describeSObjectResult.getFields();
			}
		} catch(Exception e){
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.GettingModuleFields", this.module),e);
		}finally  {
			if(describeSObjectResult!=null) describeSObjectResult=null;
		}
	}  

	public String[] getFields(String objectName) throws KettleException
	{
		Field[] fields= getObjectFields(objectName);
		if(fields!=null) {
		    int nrFields=fields.length;
		    String[] fieldsMapp= new String[nrFields];
		    
	        for (int i = 0; i < nrFields; i++)  {
	        	Field field= fields[i];
	    
	         	if(field.getRelationshipName()!=null) {
	         		fieldsMapp[i]= field.getRelationshipName();
            	}else {
            		fieldsMapp[i]=field.getName();
            	}
	         	 
             } 
	        return fieldsMapp;
		}
		return null;
	}
	public UpsertResult[] upsert(String upsertField, SObject[] sfBuffer) throws KettleException
	{
		try {
			return getBinding().upsert(upsertField, sfBuffer);
		}catch(Exception e) {
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.Upsert"), e);
		}
	}
	public SaveResult[] insert(SObject[] sfBuffer) throws KettleException
	{
		try {
			return getBinding().create(sfBuffer);
		}catch(Exception e) {
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.Insert"), e);
		}
	}
	public SaveResult[] update(SObject[] sfBuffer) throws KettleException
	{
		try {
			return getBinding().update(sfBuffer);
		}catch(Exception e) {
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.Update"), e);
		}
	}
	public DeleteResult[] delete(String[] id) throws KettleException
	{
		try {
			return getBinding().delete(id);
		}catch(Exception e) {
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Error.Delete"), e);
		}
	}
	public static XmlObject createMessageElement( String name, Object value, boolean useExternalKey ) throws Exception {

	    XmlObject me = null;

	    if ( useExternalKey ) {
	      // We use an external key
	      // the structure should be like this :
	      // object:externalId/lookupField
	      // where
	      // object is the type of the object
	      // externalId is the name of the field in the object to resolve the value
	      // lookupField is the name of the field in the current object to update (is the "__r" version)

	      int indexOfType = name.indexOf( ":" );
	      if ( indexOfType > 0 ) {
	        String type = name.substring( 0, indexOfType );
	        String extIdName = null;
	        String lookupField = null;

	        String rest = name.substring( indexOfType + 1, name.length() );
	        int indexOfExtId = rest.indexOf( "/" );
	        if ( indexOfExtId > 0 ) {
	          extIdName = rest.substring( 0, indexOfExtId );
	          lookupField = rest.substring( indexOfExtId + 1, rest.length() );
	        } else {
	          extIdName = rest;
	          lookupField = extIdName;
	        }
	        me = createForeignKeyElement( type, lookupField, extIdName, value );
	      } else {
	        throw new KettleException( BaseMessages.getString( PKG, "SalesforceBulkConnection.Error.UnableToFindObjectType" ) );
	      }
	    } else {
	      me = fromTemplateElement( name, value, true );
	    }

	    return me;
	}
	private static XmlObject createForeignKeyElement( String type, String lookupField, String extIdName,
      Object extIdValue ) throws Exception {

	  // Foreign key relationship to the object
	  XmlObject me = fromTemplateElement( lookupField, null, false );
	  me.addField( "type", type );
	  me.addField( extIdName, extIdValue );
      
	  return me;
    }
	
	public static XmlObject fromTemplateElement( String name, Object value, boolean setValue ) throws SOAPException {
	  // Use the TEMPLATE org.w3c.dom.Element to create new Message Elements
	  XmlObject me = new XmlObject();
	  if ( setValue ) {
	     me.setValue( value );
	  }
	  me.setName( new QName( name ) );
	  return me;
	}
	
	public static XmlObject[] getChildren( SObject object ) {
	  List<String> reservedFieldNames = Arrays.asList( "type", "fieldsToNull" );
	  if ( object == null ) {
	    return null;
	  }
	  List<XmlObject> children = new ArrayList<>();
	  Iterator<XmlObject> iterator = object.getChildren();
	  while ( iterator.hasNext() ) {
	    XmlObject child = iterator.next();
	    if ( child.getName().getNamespaceURI().equals( Constants.PARTNER_SOBJECT_NS )
	        && reservedFieldNames.contains( child.getName().getLocalPart() ) ) {
	      continue;
	    }
	    children.add( child );
	  }
	  if ( children.size() == 0 ) {
	    return null;
	  }
	  return children.toArray( new XmlObject[children.size()] );
	}
}
