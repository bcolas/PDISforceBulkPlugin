/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Created by Benoit COLAS - 17/10/2018
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.sforcebulkdelete;

import java.util.Map;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.sforcebulkutils.SalesforceBulkConnection;

/*
* @author Benoit COLAS
* @since 17-10-2018
*/
public class SalesforceBulkDeleteData extends BaseStepData implements StepDataInterface {
 public RowMetaInterface inputRowMeta;
 public RowMetaInterface outputRowMeta;

 public String realURL;
 public String realModule;

 public SalesforceBulkConnection connection;

 public String[] deleteId;
 public Map<String, Object[]> outputBuffer;
 public int iBufferPos;

 public int indexOfKeyField;

 public SalesforceBulkDeleteData() {
   super();

   connection = null;
   realURL = null;
   realModule = null;
   iBufferPos = 0;
   indexOfKeyField = -1;
 }
}
