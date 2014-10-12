/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Created by Benoit COLAS - 08/10/2014
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.sforcebulkinput;

import java.util.Map;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.salesforceinput.SalesforceInputField;

/**
 * Read data from Salesforce module in batch mode, convert them to rows and writes these to one or more output streams.
 * 
 * @author Benoit COLAS
 * @since 08-10-2014
 */
public class SalesforceBulkInput extends BaseStep implements StepInterface
{
	private static Class<?> PKG = SalesforceBulkInputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private SalesforceBulkInputMeta meta;
	private SalesforceBulkInputData data;
	
	public SalesforceBulkInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
	{
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}	
	
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
	{
		if(first){
			first=false;
		
		    // Create the output row meta-data
            data.outputRowMeta = new RowMeta();

		    meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
			
            // For String to <type> conversions, we allocate a conversion meta data row as well...
		    //
			 data.convertRowMeta = data.outputRowMeta.clone();
			 for (int i=0;i<data.convertRowMeta.size();i++) {
				data.convertRowMeta.getValueMeta(i).setType(ValueMetaInterface.TYPE_STRING);            
			 }
			
		    // Let's query Salesforce
		    data.connection.query();
	        
		}
		
		Object[] outputRowData=null;

		try  {	
			// get one row ...
			outputRowData = getOneRow();
			
			if(outputRowData==null) {
				setOutputDone();
				return false;
			}
		
			putRow(data.outputRowMeta, outputRowData);  // copy row to output rowset(s);
		    
		    if (checkFeedback(getLinesInput())) {
		    	if(log.isDetailed()) logDetailed(BaseMessages.getString(PKG, "SalesforceBulkInput.log.LineRow",""+ getLinesInput()));
		    }
	          
            data.rownr++;
            data.recordIndex++;
            
		    return true; 
		} 
		catch(KettleException e) {
	        boolean sendToErrorRow=false;
			String errorMessage = null;
			if (getStepMeta().isDoingErrorHandling()) {
		         sendToErrorRow = true;
		         errorMessage = e.toString();
			} else {
				logError(BaseMessages.getString(PKG, "SalesforceBulkInput.log.Exception", e.getMessage()));
                logError(Const.getStackTracker(e));
				setErrors(1);
				stopAll();
				setOutputDone();  // signal end to receiver(s)
				return false;				
			}
			if (sendToErrorRow) {
			   // Simply add this row to the error row
			   putError(getInputRowMeta(), outputRowData, 1, errorMessage, null, "SalesforceBulkInput001");
			}
		} 
		return true;
	}		
	private Object[] getOneRow()  throws KettleException {
		/*if (data.limitReached || data.rownr>=data.recordcount) {
	      return null;
		} */

		// Build an empty row based on the meta-data		  
		Object[] outputRowData=buildEmptyRow();

		try{
			
			// check for limit rows
            if (data.limit>0 && data.rownr>=data.limit) {
            	// User specified limit and we reached it 
            	// We end here 
            	data.limitReached = true;
            	return null;
            }
			
            // Return a record
			Map<String, String> row=data.connection.getNextRecord();
			if (row == null) return null;
			//data.finishedRecord=srvalue.isAllRecordsProcessed();
			
			for (int i=0;i<data.nrfields;i++) {
				String value=data.connection.getRecordValue(row, meta.getInputFields()[i].getField());
				
				// DO Trimming!
				switch (meta.getInputFields()[i].getTrimType()) {
					case SalesforceInputField.TYPE_TRIM_LEFT:
						value = Const.ltrim(value);
						break;
					case SalesforceInputField.TYPE_TRIM_RIGHT:
						value = Const.rtrim(value);
						break;
					case SalesforceInputField.TYPE_TRIM_BOTH:
						value = Const.trim(value);
						break;
					default:
						break;
				}
				      
				// DO CONVERSIONS...
				//
			    ValueMetaInterface targetValueMeta = data.outputRowMeta.getValueMeta(i);
				ValueMetaInterface sourceValueMeta = data.convertRowMeta.getValueMeta(i);
				outputRowData[i] = targetValueMeta.convertData(sourceValueMeta, value);
				
				// Do we need to repeat this field if it is null?
				if (meta.getInputFields()[i].isRepeated()) {
					if (data.previousRow!=null && Const.isEmpty(value)) {
						outputRowData[i] = data.previousRow[i];
					}
				}
		
			}  // End of loop over fields...
			
			int rowIndex = data.nrfields;
			
			// See if we need to add the url to the row...  
			if (meta.includeSoapURL() && !Const.isEmpty(meta.getSoapURLField())) {
				outputRowData[rowIndex++]= data.connection.getURL();
			}
			
			// See if we need to add the module to the row...  
			if (meta.includeModule() && !Const.isEmpty(meta.getModuleField())) {
				outputRowData[rowIndex++]=data.connection.getModule();
			}
	        
			// See if we need to add the generated SQL to the row...  
			if (meta.includeSQL() && !Const.isEmpty(meta.getSQLField())) {
				outputRowData[rowIndex++]=data.connection.getSQL();
			}
	        
			// See if we need to add the server timestamp to the row...  
			if (meta.includeTimestamp() && !Const.isEmpty(meta.getTimestampField())) {
				outputRowData[rowIndex++]=data.connection.getServerTimestamp();
			}
			
			// See if we need to add the row number to the row...  
	        if (meta.includeRowNumber() && !Const.isEmpty(meta.getRowNumberField())) {
	            outputRowData[rowIndex++] = new Long(data.rownr);
	        }

	        if (meta.includeDeletionDate()&& !Const.isEmpty(meta.getDeletionDateField())) {
	            //outputRowData[rowIndex++] = srvalue.getDeletionDate();
	        }
	        
			RowMetaInterface irow = getInputRowMeta();
			
			data.previousRow = irow==null?outputRowData:(Object[])irow.cloneRow(outputRowData); // copy it to make
		 }
		 catch (Exception e) {
			throw new KettleException(BaseMessages.getString(PKG, "SalesforceBulkInput.Exception.CanNotReadFromSalesforce"), e);
		 }
		
		return outputRowData;
	}
	 /* build the SQL statement to send to Salesforce
	  */ 
	 private String BuiltSOQl() {
		String sql="";
		SalesforceInputField fields[] = meta.getInputFields();
		
		sql+="SELECT ";
		for (int i=0;i<data.nrfields;i++){
			SalesforceInputField field = fields[i];    
			sql+= environmentSubstitute(field.getField());
			if(i<data.nrfields-1) sql+= ",";
		}
		sql = sql + " FROM " + environmentSubstitute(meta.getModule());
		if (!Const.isEmpty(environmentSubstitute(meta.getCondition()))){
			sql+= " WHERE " + environmentSubstitute(meta.getCondition().replace("\n\r", "").replace("\n", ""));
		}
		if (Const.toLong(environmentSubstitute(meta.getRowLimit()),0)>0){
			sql+= " LIMIT " + Const.toLong(environmentSubstitute(meta.getRowLimit()),0);
		}

		return sql;
	 }
	 
	/**
	 * Build an empty row based on the meta-data.
	 * 
	 * @return
	 */
	private Object[] buildEmptyRow(){
       Object[] rowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
	    return rowData;
	}
	
	public boolean init(StepMetaInterface smi, StepDataInterface sdi){
		meta=(SalesforceBulkInputMeta)smi;
		data=(SalesforceBulkInputData)sdi;
		
		if (super.init(smi, sdi))
		{
			// get total fields in the grid
			data.nrfields = meta.getInputFields().length;
			
			 // Check if field list is filled 
			 if (data.nrfields==0) {
				 log.logError(BaseMessages.getString(PKG, "SalesforceInputDialog.FieldsMissing.DialogMessage"));
				 return false;
			 }
			 
			// check soap URL
			String soapUrl=environmentSubstitute(meta.getSoapURL());
			if(Const.isEmpty(soapUrl)) {
				log.logError(BaseMessages.getString(PKG, "SalesforceBulkInput.TargetURLMissing.Error"));
				return false;
			}
			// check username
			String realUser=environmentSubstitute(meta.getUserName());
			if(Const.isEmpty(realUser)) {
				log.logError(BaseMessages.getString(PKG, "SalesforceBulkInput.UsernameMissing.Error"));
				return false;
			}
			try  
			{
				
				data.Module=environmentSubstitute(meta.getModule());
				// Check if module is specified 
				if (Const.isEmpty(data.Module)) {
					log.logError(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.ModuleMissing.DialogMessage"));
					return false;
				}
				 
				data.limit=Const.toLong(environmentSubstitute(meta.getRowLimit()),0);
				
				// create a Salesforce bulk connection
				data.connection= new SalesforceBulkConnection(log, soapUrl, realUser,environmentSubstitute(meta.getPassword()));
				// set timeout
				data.connection.setTimeOut(Const.toInt(environmentSubstitute(meta.getTimeOut()),0));
				// Do we use compression?
				data.connection.setUsingCompression(meta.isUsingCompression());
				
			    // retrieve data from a module
			    // Set condition if needed
			    String realCondition=environmentSubstitute(meta.getCondition());
			    if(!Const.isEmpty(realCondition)) data.connection.setCondition(realCondition);
			    // Set module
			    data.connection.setModule(data.Module); 	

			    // Return fields list
			    //data.connection.setFieldsList(BuiltSOQl());
				// Build now SOQL
				data.connection.setSQL(BuiltSOQl());
			    
			    // Now connect ...
			    data.connection.connect();	

				return true;
			} 
			catch(KettleException ke) 
			{ 
				logError(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.ErrorOccurredDuringStepInitialize")+ke.getMessage()); //$NON-NLS-1$
			}

			return true;
		}
		return false;
	}
	
	public void dispose(StepMetaInterface smi, StepDataInterface sdi){
		meta=(SalesforceBulkInputMeta)smi;
		data=(SalesforceBulkInputData)sdi;
		try{
			if(data.connection!=null) data.connection.close();
			if(data.outputRowMeta!=null) data.outputRowMeta=null;
			if(data.convertRowMeta!=null) data.convertRowMeta=null;
			if(data.previousRow!=null) data.previousRow=null;
		}
		catch(Exception e){};
		super.dispose(smi, sdi);
	}
}