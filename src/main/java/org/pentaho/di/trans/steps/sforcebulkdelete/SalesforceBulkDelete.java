/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Created by Benoit COLAS - 17/10/2018
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.sforcebulkdelete;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import com.sforce.async.OperationEnum;
import org.pentaho.di.trans.steps.sforcebulkutils.SalesforceBulkConnection;
import org.pentaho.di.trans.steps.sforcebulkutils.SalesforceBulkConnectionUtils;

/**
* Delete data from Salesforce module.
*
* @author Benoit COLAS
* @since 17-10-2018
*/
public class SalesforceBulkDelete extends BaseStep implements StepInterface {
 private static Class<?> PKG = SalesforceBulkConnectionUtils.class; // for i18n purposes, needed by Translator2!!

 private SalesforceBulkDeleteMeta meta;
 private SalesforceBulkDeleteData data;

 public SalesforceBulkDelete( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
   TransMeta transMeta, Trans trans ) {
   super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
 }

 public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

   // get one row ... This does some basic initialization of the objects, including loading the info coming in
   Object[] outputRowData = getRow();

   if ( outputRowData == null ) {
     if ( data.iBufferPos > 0 ) {
       executeBatch();
     }
     try {
       data.connection.closeJob();
     } catch ( Exception e ) {
       throw new KettleStepException( BaseMessages.getString( PKG, "SalesforceBulkConnection.Error.ClosingJob" ), e );
     }
     setOutputDone();
     return false;
   }

   // If we haven't looked at a row before then do some basic setup.
   if ( first ) {
     first = false;
     
     try {
       data.connection.initJob(meta.getModule(), OperationEnum.delete);
     } catch ( Exception e ) {
       throw new KettleStepException( BaseMessages.getString( PKG, "SalesforceBulkConnection.Error.InitJob" ), e );
     }

     data.deleteId = new String[meta.getBatchSizeInt()];
     data.outputBuffer = new HashMap<String, Object[]>();

     // Create the output row meta-data
     data.outputRowMeta = getInputRowMeta().clone();
     meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );

     // Check deleteKeyField
     String realFieldName = environmentSubstitute( meta.getDeleteField() );
     if ( Const.isEmpty( realFieldName ) ) {
       throw new KettleException( BaseMessages.getString( PKG, "SalesforceBulkDelete.Error.DeleteKeyFieldMissing" ) );
     }

     // return the index of the field in the input stream
     data.indexOfKeyField = getInputRowMeta().indexOfValue( realFieldName );
     if ( data.indexOfKeyField < 0 ) {
       // the field is unreachable!
       throw new KettleException( BaseMessages.getString(
         PKG, "SalesforceBulkDelete.Error.CanNotFindDeleteKeyField", realFieldName ) );
     }
   }

   try {
     writeToSalesForce( outputRowData );
   } catch ( Exception e ) {
     throw new KettleStepException( BaseMessages.getString( PKG, "SalesforceBulkDelete.Error.ErrorWritingToSFDC" ), e );
   }
   return true;
 }

 private void writeToSalesForce( Object[] rowData ) throws KettleException {
   try {
     if ( log.isDetailed() ) {
       logDetailed( BaseMessages.getString( PKG, "SalesforceBulkDelete.Log.WriteToSalesforce", data.iBufferPos, meta.getBatchSizeInt() ) );
     }

     // if there is room in the buffer
     if ( data.iBufferPos < meta.getBatchSizeInt() ) {

       // Load the buffer array
       String deleteId = getInputRowMeta().getString( rowData, data.indexOfKeyField );
       data.deleteId[data.iBufferPos] = deleteId;
       if ( log.isDebug() ) {
         logDebug( "delete: "+ deleteId );
       }
       data.outputBuffer.put(deleteId, rowData);
       data.iBufferPos++;
     }

     if ( data.iBufferPos >= meta.getBatchSizeInt() ) {
       if ( log.isDetailed() ) {
         logDetailed( BaseMessages.getString( PKG, "SalesforceBulkDelete.Log.CallingFlush" ) );
       }
       executeBatch();
     }
   } catch ( Exception e ) {
     throw new KettleException( BaseMessages.getString( PKG, "SalesforceBulkDelete.Error.WriteToSalesforce", e
       .getMessage() ) );
   }
 }

 private void executeBatch() throws KettleException {
	if ( log.isDetailed() ) logDetailed( "SalesforceBulkDelete.executeBatch()");
	String s = "id\n";
	for (int i = 0; i < data.deleteId.length; i++)
	  s += data.deleteId[i] + '\n';
    String batchId = data.connection.createBatch(new ByteArrayInputStream(s.getBytes()));
    data.connection.awaitCompletion();
    checkResults(batchId);
 }
 
 private void checkResults(String batchId) throws KettleException {
   try {
	 List<Map> resultInfos = data.connection.checkResults(batchId);
	 
	 for (Map<String, String> resultInfo : resultInfos) {
	   boolean success = Boolean.valueOf(resultInfo.get("Success"));
	   if (success) {
		 putRow( data.outputRowMeta, data.outputBuffer.get(resultInfo.get("Id")) ); // copy row to output rowset(s);
	     incrementLinesOutput();
	       
	     if ( checkFeedback( getLinesInput() ) ) {
	       if ( log.isDetailed() ) {
	         logDetailed( BaseMessages.getString( PKG, "SalesforceBulk.Log.LineRow", String.valueOf( getLinesInput() ) ) );
	       }
	     }
	   }
	   else {
	     if ( !getStepMeta().isDoingErrorHandling() ) {
	       if ( log.isDetailed() ) {
	         logDetailed( BaseMessages.getString( PKG, "SalesforceBulkDelete.Log.ErrorFound" ) );
	       }
	       throw new KettleException( BaseMessages.getString( PKG, "SalesforceBulkDelete.Error.CheckResult"
	    		                                                 , resultInfo.get("Id"), resultInfo.get("Error") ) );
	     }
	     else {
	       // Simply add this row to the error row
	       if ( log.isDebug() ) {
	         logDebug( BaseMessages.getString( PKG, "SalesforceBulkDelete.Log.PassingRowToErrorStep", resultInfo.get("Id"), resultInfo.get("Error") ) );
	       }
	       putError( getInputRowMeta(), data.outputBuffer.get(resultInfo.get("Id")), 1, resultInfo.get("Error"), null, "SalesforceBulkDelete001" );
	     }
	   }
	 }

     // reset the buffers
     data.deleteId = new String[meta.getBatchSizeInt()];
     data.outputBuffer = new HashMap<String, Object[]>();
     data.iBufferPos = 0;

   } catch ( Exception e ) {
     if ( !getStepMeta().isDoingErrorHandling() ) {
       throw new KettleException( BaseMessages.getString( PKG, "SalesforceBulkDelete.Error.FailedToDeleted", e.getMessage() ) );
     }
     // Simply add this row to the error row
     if ( log.isDebug() ) {
       logDebug( "Passing row to error step" );
     }

     for ( String key : data.outputBuffer.keySet() ) {
       putError( data.inputRowMeta, data.outputBuffer.get(key), 1, e.getMessage(), null, "SalesforceBulkDelete002" );
     }
   }
 }

 public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
   meta = (SalesforceBulkDeleteMeta) smi;
   data = (SalesforceBulkDeleteData) sdi;

   if ( super.init( smi, sdi ) ) {

     try {
       data.realModule = environmentSubstitute( meta.getModule() );
       // Check if module is specified
       if ( Const.isEmpty( data.realModule ) ) {
         log.logError( BaseMessages.getString( PKG, "SalesforceBulkDeleteDialog.ModuleMissing.DialogMessage" ) );
         return false;
       }

       String realUser = environmentSubstitute( meta.getUserName() );
       // Check if username is specified
       if ( Const.isEmpty( realUser ) ) {
         log.logError( BaseMessages.getString( PKG, "SalesforceBulkDeleteDialog.UsernameMissing.DialogMessage" ) );
         return false;
       }

       // initialize variables
       data.realURL = environmentSubstitute( meta.getTargetURL() );
       String batchSize = environmentSubstitute( meta.getBatchSize() );
       meta.setBatchSize(batchSize);
       
       // create a Salesforce bulk connection
       data.connection= new SalesforceBulkConnection(log, data.realURL, realUser, environmentSubstitute(meta.getPassword()));
       // set timeout
       data.connection.setTimeOut(Const.toInt(environmentSubstitute(meta.getTimeOut()),0));
       // set max row expected
       data.connection.setRowMax(Const.toInt(environmentSubstitute(meta.getRowMax()),Integer.MAX_VALUE));
       // Do we use compression?
       data.connection.setUsingCompression(meta.isUsingCompression());
	 
       // Set module
       data.connection.setModule(data.realModule); 	
	    
       // Now connect ...
	   data.connection.connect();	

       return true;
     } catch ( KettleException ke ) {
       logError( BaseMessages.getString( PKG, "SalesforceBulk.Log.ErrorOccurredDuringStepInitialize" )
         + ke.getMessage() );
       return false;
     }
   }
   return false;
 }

 public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
   meta = (SalesforceBulkDeleteMeta) smi;
   data = (SalesforceBulkDeleteData) sdi;
   try {
     if ( data.outputBuffer != null ) {
       data.outputBuffer = null;
     }
     if ( data.deleteId != null ) {
       data.deleteId = null;
     }
     if ( data.connection != null ) {
       data.connection.close();
     }
   } catch ( Exception e ) { /* Ignore */
   }
   super.dispose( smi, sdi );
 }

}
