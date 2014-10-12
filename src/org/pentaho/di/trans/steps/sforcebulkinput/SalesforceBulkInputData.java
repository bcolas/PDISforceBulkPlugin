/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Created by Benoit COLAS - 08/10/2014
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.sforcebulkinput;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/*
 * @author Samatar
 * @since 10-06-2007
 */
public class SalesforceBulkInputData extends BaseStepData implements StepDataInterface 
{
	public int nr_repeats;
    public long rownr;
	public Object[] previousRow;
	public RowMetaInterface inputRowMeta;
	public RowMetaInterface outputRowMeta;
	public RowMetaInterface convertRowMeta;
	public int recordcount;
    public int nrfields;
    public boolean limitReached;
    public long limit;
    public String Module;
	// available before we call query more if needed
	public int nrRecords;
	// We use this variable to query more
	// we initialize it each time we call query more
	public int recordIndex;;
	public SalesforceBulkConnection connection;
	public boolean finishedRecord;

	/**
	 * 
	 */
	public SalesforceBulkInputData()
	{
		super();

		nr_repeats=0;
		nrfields=0;
		recordcount=0;
		limitReached=false;
		limit=0;
		nrRecords=0;
		recordIndex=0;
		rownr = 0;	
		
		connection=null;
	}
}