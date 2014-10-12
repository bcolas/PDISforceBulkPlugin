/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Created by Benoit COLAS - 08/10/2014
 * 
 ******************************************************************************/

package org.pentaho.di.trans.steps.sforcebulkinput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.soap.SOAPException;

import org.apache.axis.message.MessageElement;
import org.apache.axis.transport.http.HTTPConstants;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
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
import com.sforce.soap.partner.AllOrNoneHeader;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.GetUserInfoResult;
import com.sforce.soap.partner.LoginResult;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.SessionHeader;
import com.sforce.soap.partner.SforceServiceLocator;
import com.sforce.soap.partner.SoapBindingStub;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.fault.ExceptionCode;
import com.sforce.soap.partner.fault.LoginFault;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectorConfig;

public class SalesforceBulkConnection {
	private static Class<?> PKG = SalesforceBulkInputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
	
	private String url;
	private String username;
	private String password;
	private String module;
	private int timeout;
	private String condition;
	
	private SoapBindingStub binding;
	private LoginResult loginResult;
	private GetUserInfoResult userInfo;
	private String sql;
	private Date serverTimestamp;
	private List<String> qr ;
	private GregorianCalendar startDate;
	private GregorianCalendar endDate;
	private int queryResultSize;
	private int recordsCount;
	private boolean useCompression;
	private boolean rollbackAllChangesOnError;
	private boolean queryAll;
	
	private BulkConnection bulkConnection;
	private JobInfo job;
	private BatchInfo info;
	
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
		
		// check target URL
		if(Const.isEmpty(getURL()))	throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.TargetURLMissing.Error"));
		
		// check username
		if(Const.isEmpty(getUsername())) throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.UsernameMissing.Error"));
				
		if(log.isDetailed()) logInterface.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.NewConnection"));
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
	public List<String> getQueryResult() {
		return this.qr;
	}
	
	public SoapBindingStub getBinding(){
		return this.binding;
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

		try{
			this.binding = (SoapBindingStub) new SforceServiceLocator().getSoap();
			if (log.isDetailed()) log.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.LoginURL", binding._getProperty(SoapBindingStub.ENDPOINT_ADDRESS_PROPERTY)));
		      
	        //  Set timeout
	      	if(getTimeOut()>0) {
	      		this.binding.setTimeout(getTimeOut());
	      		 if (log.isDebug())  log.logDebug(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.SettingTimeout",""+this.timeout));
	      	}
	        
	      	
	      	// Set URL
	      	this.binding._setProperty(SoapBindingStub.ENDPOINT_ADDRESS_PROPERTY, getURL());
	      	
	      	// Do we need compression?
	      	if(isUsingCompression()) {
	      		this.binding._setProperty(HTTPConstants.MC_ACCEPT_GZIP, useCompression);
	      		this.binding._setProperty(HTTPConstants.MC_GZIP_REQUEST, useCompression);
	      	}
	       	if(isRollbackAllChangesOnError()) {
	      	    // Set the SOAP header to rollback all changes
	      		// unless all records are processed successfully.
	      	    AllOrNoneHeader allOrNoneHeader = new AllOrNoneHeader();
	      	    allOrNoneHeader.setAllOrNone(true);
	      	    this.binding.setHeader(new SforceServiceLocator().getServiceName().getNamespaceURI(), "AllOrNoneHeader", allOrNoneHeader);
	      	}
	        // Attempt the login giving the user feedback
	        if (log.isDetailed()) {
	        	log.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.LoginNow"));
	        	log.logDetailed("----------------------------------------->");
	        	log.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.LoginURL",getURL()));
	        	log.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.LoginUsername",getUsername()));
	        	if(getModule()!=null) log.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.LoginModule", getModule()));
	        	if(getCondition()!=null) log.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.LoginCondition",getCondition()));
	        	log.logDetailed("<-----------------------------------------");
	        }
	        
	        // Login
	        this.loginResult = getBinding().login(getUsername(), getPassword());
	        
	        if (log.isDebug()) {
	        	log.logDebug(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.SessionId") + " : " + this.loginResult.getSessionId());
	        	log.logDebug(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.NewServerURL") + " : " + this.loginResult.getServerUrl());
	        }
	        
	        // set the session header for subsequent call authentication
	        this.binding._setProperty(SoapBindingStub.ENDPOINT_ADDRESS_PROPERTY,this.loginResult.getServerUrl());
	
	        // Create a new session header object and set the session id to that
	        // returned by the login
	        SessionHeader sh = new SessionHeader();
	        sh.setSessionId(loginResult.getSessionId());
	        this.binding.setHeader(new SforceServiceLocator().getServiceName().getNamespaceURI(), "SessionHeader", sh);
	       
	        // Return the user Infos
	        this.userInfo = this.binding.getUserInfo();
	        if (log.isDebug()) {
	        	log.logDebug(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.UserInfos") + " : " + this.userInfo.getUserFullName());
	        	log.logDebug("----------------------------------------->");
	        	log.logDebug(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.UserName") + " : " + this.userInfo.getUserFullName());
	        	log.logDebug(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.UserEmail") + " : " + this.userInfo.getUserEmail());
	        	log.logDebug(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.UserLanguage") + " : " + this.userInfo.getUserLanguage());
	        	log.logDebug(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.UserOrganization") + " : " + this.userInfo.getOrganizationName());    
	        	log.logDebug("<-----------------------------------------");
	        }
	        
	    	this.serverTimestamp= getBinding().getServerTimestamp().getTimestamp().getTime();
	 		if(log.isDebug()) BaseMessages.getString(PKG, "SalesforceBulkInput.Log.ServerTimestamp",getServerTimestamp());
	 		
	 		// Create bulk connection
	 		ConnectorConfig config = new ConnectorConfig();
	 		config.setSessionId(this.loginResult.getSessionId());
	 		String serverUrl = this.loginResult.getServerUrl();
	 		String restEndpoint = serverUrl.substring(0, serverUrl.indexOf("Soap/")) + "async/" + SalesforceBulkConnectionUtils.LIB_VERSION;
	 		config.setRestEndpoint(restEndpoint);
	 		config.setCompression(this.useCompression);
	 		this.bulkConnection = new BulkConnection(config);
	 		
	 		if(log.isDetailed()) log.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.Connected"));
		
		}catch (LoginFault ex) {
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
            	throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.Error.InvalidUsernameOrPassword"));
            }
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.Error.Connection"), ex);
		}catch(Exception e){
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.Error.Connection"), e);
		}
	}

	 public void query() throws KettleException {
		 
		if(getBinding()==null)  throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.Exception.CanNotGetBiding"));
		
	    try {
			// check if we can query this Object
			DescribeSObjectResult describeSObjectResult = getBinding().describeSObject(getModule());
			if (describeSObjectResult == null) throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.ErrorGettingObject"));  
			if(!describeSObjectResult.isQueryable()) throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.ObjectNotQueryable",module));
		    			        
		    if (getSQL()!=null && log.isDetailed()) log.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.SQLString") + " : " +  getSQL());        
		  
		    this.job = new JobInfo();
		    this.job.setObject(getModule());
		      
		    this.job.setOperation(OperationEnum.query);
		    this.job.setConcurrencyMode(ConcurrencyMode.Parallel);
		    this.job.setContentType(ContentType.CSV);
		      
		    this.job = this.bulkConnection.createJob(job);
		    assert this.job.getId() != null;
		    
		    if (log.isDetailed()) log.logDetailed("Job ID : "+this.job.getId());
		    
		    this.job = bulkConnection.getJobStatus(job.getId());
		    
		    if (log.isDetailed()) log.logDetailed("Job Status : "+this.job.getState().toString());
		      
		    ByteArrayInputStream bout = new ByteArrayInputStream(getSQL().getBytes());
		    this.info = this.bulkConnection.createBatchFromStream(this.job, bout);
		    
		    String[] queryResults = null;
		      
		    for(int i=0; i<10000; i++) {
		    	Thread.sleep(i==0 ? 30 * 1000 : 30 * 1000); //30 sec
		        
		    	this.info = bulkConnection.getBatchInfo(this.job.getId(), this.info.getId());
		    	
		    	if (log.isDetailed()) log.logDetailed("Batch Status : "+this.info.getState());
		        
		    	if (this.info.getState() == BatchStateEnum.Completed) {
		    		QueryResultList qrl = this.bulkConnection.getQueryResultList(this.job.getId(), this.info.getId());
		    		queryResults = qrl.getResult();
		    		break;
		    	} else if (this.info.getState() == BatchStateEnum.Failed) {
		    		log.logError("Batch failed " + this.info);
		    		break;
		    	} else {
		    		if (log.isDetailed()) log.logDetailed("Job processing time : " + this.info.getTotalProcessingTime());
		    	}
		    }
		    
		    if (queryResults != null) {
		        for (String resultId : queryResults) {
		        	if (log.isDetailed()) log.logDetailed("Result Id : "+resultId);
		    	    
		        	this.csvr = new CSVReader(this.bulkConnection.getQueryResultStream(job.getId(), info.getId(), resultId), "UTF-8");
		        	this.resultHeader = csvr.nextRecord();
		        }
		    }

	    } catch (AsyncApiException aae) {
	    	log.logError(Const.getStackTracker(aae));
	    	aae.printStackTrace();
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Exception.Query"),aae);
	    } catch (InterruptedException ie) {
	    	log.logError(Const.getStackTracker(ie));
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Exception.Query"),ie);
		} catch(Exception e){
			log.logError(Const.getStackTracker(e));
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkConnection.Exception.Query"),e);
		}
	 }
	 public void close() throws KettleException
	 {
		 try {
			 this.job.setState(JobStateEnum.Closed);
		     this.bulkConnection.updateJob(this.job);
		     if(log.isDetailed()) log.logDetailed(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.ConnectionClosed"));
		 }
	     catch (AsyncApiException aae) {
	    	 
	     }
		 catch(Exception e){
			 throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.Error.ClosingConnection"),e);
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
	 public MessageElement[] getElements() throws Exception {
		 // Query first
		 QueryResult qr = getBinding().query(getSQL());
		 // and then return records
		 SObject con=qr.getRecords()[0];
		 if(con==null) return null;
		 return con.get_any();
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
	    	  DescribeGlobalSObjectResult o= dgr.getSobjects(i);
	    	  if((OnlyQueryableObjects && o.isQueryable()) || !OnlyQueryableObjects) {
	    		  objects.add(o.getName());
	    	  }
	      }
	      return  (String[]) objects.toArray(new String[objects.size()]);
	   } catch(Exception e){
		   throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.Error.GettingModules"),e);
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
				throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.ObjectNotQueryable",this.module));
			}else{
				// we can query this object
				return  describeSObjectResult.getFields();
			}
		} catch(Exception e){
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.Error.GettingModuleFields", this.module),e);
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
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.ErrorUpsert"), e);
		}
	}
	public SaveResult[] insert(SObject[] sfBuffer) throws KettleException
	{
		try {
			return getBinding().create(sfBuffer);
		}catch(Exception e) {
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.ErrorInsert"), e);
		}
	}
	public SaveResult[] update(SObject[] sfBuffer) throws KettleException
	{
		try {
			return getBinding().update(sfBuffer);
		}catch(Exception e) {
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.ErrorUpdate"), e);
		}
	}
	public DeleteResult[] delete(String[] id) throws KettleException
	{
		try {
			return getBinding().delete(id);
		}catch(Exception e) {
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.ErrorDelete"), e);
		}
	}
	public static MessageElement createMessageElement(String name, Object value, boolean useExternalKey) 
			throws Exception {

		MessageElement me  = null;
		
		if(useExternalKey) {
			// We use an external key
			// the structure should be like this :
			// object:externalId/lookupField
			// where
			// object is the type of the object
			// externalId is the name of the field in the object to resolve the value
			// lookupField is the name of the field in the current object to update (is the "__r" version)

			int indexOfType = name.indexOf(":");
			if(indexOfType>0) {
				String type = name.substring(0, indexOfType);
				String extIdName=null;
				String lookupField=null;
				
				String rest = name.substring(indexOfType+1, name.length());
				int indexOfExtId = rest.indexOf("/");
				if(indexOfExtId>0) {
					extIdName = rest.substring(0, indexOfExtId);
					lookupField = rest.substring(indexOfExtId+1, rest.length());
				}else {
					extIdName=rest;
					lookupField=extIdName;
				}
				me= createForeignKeyElement(type, lookupField ,extIdName, value);
			}else {
				throw new KettleException(BaseMessages.getString(PKG, "SalesforceConnection.UnableToFindObjectType"));
			}
		}else {
			me= fromTemplateElement(name, value, true);
		}

		return me;
	}
	private static MessageElement createForeignKeyElement(String type, String lookupField ,String extIdName, 
			Object extIdValue) throws Exception {

        // Foreign key relationship to the object
		MessageElement me = fromTemplateElement(lookupField, null, false);
		me.addChild(new MessageElement(new QName("type"), type));
        me.addChild(new MessageElement(new QName(extIdName),  extIdValue));
        
        return me;
    }
	
	private static MessageElement TEMPLATE_MESSAGE_ELEMENT = new MessageElement("", "temp");
	
	// The Template org.w3c.dom.Element instance
	private static Element TEMPLATE_XML_ELEMENT;
	
	static {
		try {
			// Create and cache this org.w3c.dom.Element instance for once here.
			TEMPLATE_XML_ELEMENT = TEMPLATE_MESSAGE_ELEMENT.getAsDOM();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		TEMPLATE_XML_ELEMENT.removeAttribute("xsi:type");
		TEMPLATE_XML_ELEMENT.removeAttribute("xmlns:ns1");
		TEMPLATE_XML_ELEMENT.removeAttribute("xmlns:xsd");
		TEMPLATE_XML_ELEMENT.removeAttribute("xmlns:xsi");
	}
	
	
	public static MessageElement fromTemplateElement(String name, Object value, boolean setValue)
			throws SOAPException {
		// Use the TEMPLATE org.w3c.dom.Element to create new Message Elements
		MessageElement me = new MessageElement(TEMPLATE_XML_ELEMENT);
		if(setValue) me.setObjectValue(value);
		me.setName(name);
		return me;
	}

}
