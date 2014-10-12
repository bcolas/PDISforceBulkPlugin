/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Created by Benoit COLAS - 08/10/2014
 *
 ******************************************************************************/

package org.pentaho.di.ui.trans.steps.sforcebulkinput;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.LabelTextVar;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import com.sforce.soap.partner.Field;

import org.pentaho.di.trans.steps.sforcebulkinput.SalesforceBulkConnection;
import org.pentaho.di.trans.steps.sforcebulkinput.SalesforceBulkConnectionUtils;
import org.pentaho.di.trans.steps.sforcebulkinput.SalesforceBulkInputMeta;

import org.pentaho.di.trans.steps.salesforceinput.SalesforceInputField;
import org.pentaho.di.ui.trans.steps.salesforceinput.SOQLValuesHighlight;

public class SalesforceBulkInputDialog extends BaseStepDialog implements StepDialogInterface {
	
	private static Class<?> PKG = SalesforceBulkInputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private String DEFAULT_DATE_TIME_FORMAT="yyyy-MM-dd'T'HH:mm:ss'.000Z'";
	private String DEFAULT_DATE_FORMAT= "yyyy-MM-dd";
	
	private CTabFolder wTabFolder;
	
	private CTabItem wFileTab, wContentTab, wFieldsTab;

	private Composite wFileComp, wContentComp, wFieldsComp;

	private FormData fdTabFolder,fdFileComp, fdContentComp, fdFieldsComp,fdlInclSoapURLField;
	
	private FormData fdInclSoapURLField,fdlInclModuleField, fdlInclRownumField,  fdlModule, fdModule;
	
	private FormData fdInclModuleField,fdlInclModule,fdlInclSoapURL,fdInclSoapURL,fdlLimit, fdLimit;
	
	private FormData fdlTimeOut,fdTimeOut,fdFields,fdUserName,fdSoapURL,fdPassword,fdCondition;
	
	private FormData fdlCondition,fdlInclRownum,fdRownum,fdInclRownumField, fdUseCompression, fdlUseCompression;

	private Button wInclSoapURL,wInclModule,wInclRownum, wUseCompression;
	
	private FormData fdInclSQLField;
	
	private FormData fdInclTimestampField;

	private Label wlInclSoapURL,wlInclSoapURLField,wlInclModule,wlInclRownum,wlInclRownumField;
	
	private Label wlInclModuleField,wlLimit,wlTimeOut,wlCondition,wlModule,wlInclSQLField,wlInclSQL;
	
	private Group wConnectionGroup, wSettingsGroup;
	
	private Label wlInclTimestampField,wlInclTimestamp, wlUseCompression;
	
	private FormData fdlInclSQL,fdInclSQL,fdlInclSQLField;
	
	private FormData fdlInclTimestamp,fdInclTimestamp,fdlInclTimestampField;

	private Button wInclSQL;
	
	private TextVar wInclSoapURLField,wInclModuleField,wInclRownumField,wInclSQLField;
	
	private Button wInclTimestamp;
	
	private TextVar wInclTimestampField;

	private TableView wFields;

	private SalesforceBulkInputMeta input;

    private LabelTextVar wUserName,wSoapURL,wPassword;
    
    private StyledTextComp  wCondition;
	
	private Label        wlPosition;
	private FormData     fdlPosition;
    
    private TextVar wTimeOut,wLimit;

    private ComboVar  wModule;

	private Group wAdditionalFields, wAdvancedGroup;
	
	private FormData fdAdditionalFields;
	
	/*private Label 		wlRecordsFilter;
	private CCombo 		wRecordsFilter;*/
	
	private Button wTest;
	
	private FormData fdTest;
    private Listener lsTest;
  
    private boolean  gotModule = false;
    
    private boolean  getModulesListError = false;     /* True if error getting modules list */
    
    private ColumnInfo[] colinf;
    
	public SalesforceBulkInputDialog(Shell parent, Object in, TransMeta transMeta,
			String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
		input = (SalesforceBulkInputMeta) in;
	}

	public String open() {
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
		props.setLook(shell);
		setShellImage(shell, input);

		ModifyListener lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				input.setChanged();
			}
		};
		changed = input.hasChanged();

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.DialogTitle"));

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Stepname line
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName"));
		props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.top = new FormAttachment(0, margin);
		fdlStepname.right = new FormAttachment(middle, -margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		wTabFolder = new CTabFolder(shell, SWT.BORDER);
		props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
		
		

		// ////////////////////////
		// START OF FILE TAB ///
		// ////////////////////////
		wFileTab = new CTabItem(wTabFolder, SWT.NONE);
		wFileTab.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.File.Tab"));

		wFileComp = new Composite(wTabFolder, SWT.NONE);
		props.setLook(wFileComp);

		FormLayout fileLayout = new FormLayout();
		fileLayout.marginWidth = 3;
		fileLayout.marginHeight = 3;
		wFileComp.setLayout(fileLayout);
		
		
        //////////////////////////
        // START CONNECTION GROUP

        wConnectionGroup = new Group(wFileComp, SWT.SHADOW_ETCHED_IN);
        wConnectionGroup.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.ConnectionGroup.Label")); //$NON-NLS-1$;
        FormLayout fconnLayout = new FormLayout();
        fconnLayout .marginWidth = 3;
        fconnLayout .marginHeight = 3;
        wConnectionGroup.setLayout(fconnLayout );
        props.setLook(wConnectionGroup);
		
	    // Webservice URL
        wSoapURL = new LabelTextVar(transMeta,wConnectionGroup, BaseMessages.getString(PKG, "SalesforceBulkInputDialog.SoapURL.Label"), 
        		BaseMessages.getString(PKG, "SalesforceBulkInputDialog.SoapURL.Tooltip"));
        props.setLook(wSoapURL);
        wSoapURL.addModifyListener(lsMod);
        fdSoapURL = new FormData();
        fdSoapURL.left = new FormAttachment(0, 0);
        fdSoapURL.top = new FormAttachment(0, margin);
        fdSoapURL.right = new FormAttachment(100, 0);
        wSoapURL.setLayoutData(fdSoapURL);     

	      // UserName line
        wUserName = new LabelTextVar(transMeta,wConnectionGroup, BaseMessages.getString(PKG, "SalesforceBulkInputDialog.User.Label"), 
        		BaseMessages.getString(PKG, "SalesforceBulkInputDialog.User.Tooltip"));
        props.setLook(wUserName);
        wUserName.addModifyListener(lsMod);
        fdUserName = new FormData();
        fdUserName.left = new FormAttachment(0, 0);
        fdUserName.top = new FormAttachment(wSoapURL, margin);
        fdUserName.right = new FormAttachment(100, 0);
        wUserName.setLayoutData(fdUserName);
		
        // Password line
        wPassword = new LabelTextVar(transMeta,wConnectionGroup, BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Password.Label"), 
        		BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Password.Tooltip"));
        props.setLook(wPassword);
        wPassword.setEchoChar('*');
        wPassword.addModifyListener(lsMod);
        fdPassword = new FormData();
        fdPassword.left = new FormAttachment(0, 0);
        fdPassword.top = new FormAttachment(wUserName, margin);
        fdPassword.right = new FormAttachment(100, 0);
        wPassword.setLayoutData(fdPassword);

        // OK, if the password contains a variable, we don't want to have the password hidden...
        wPassword.getTextWidget().addModifyListener(new ModifyListener()
        {
            public void modifyText(ModifyEvent e)
            {
                checkPasswordVisible();
            }
        });

		// Test Salesforce connection button
		wTest=new Button(wConnectionGroup,SWT.PUSH);
		wTest.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.TestConnection.Label"));
 		props.setLook(wTest);
		fdTest=new FormData();
		wTest.setToolTipText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.TestConnection.Tooltip"));
		//fdTest.left = new FormAttachment(middle, 0);
		fdTest.top  = new FormAttachment(wPassword, margin);
		fdTest.right= new FormAttachment(100, 0);
		wTest.setLayoutData(fdTest);
		
        FormData fdConnectionGroup= new FormData();
        fdConnectionGroup.left = new FormAttachment(0, 0);
        fdConnectionGroup.right = new FormAttachment(100, 0);
        fdConnectionGroup.top = new FormAttachment(0, margin);
        wConnectionGroup.setLayoutData(fdConnectionGroup);

        // END CONNECTION  GROUP
        //////////////////////////
		
        //////////////////////////
        // START SETTINGS GROUP

        wSettingsGroup = new Group(wFileComp, SWT.SHADOW_ETCHED_IN);
        wSettingsGroup.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.HttpAuthGroup.Label")); //$NON-NLS-1$;
        FormLayout fsettingsLayout = new FormLayout();
        fsettingsLayout .marginWidth = 3;
        fsettingsLayout .marginHeight = 3;
        wSettingsGroup.setLayout(fsettingsLayout );
        props.setLook(wSettingsGroup);
        
 		// Module
		wlModule=new Label(wSettingsGroup, SWT.RIGHT);
        wlModule.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Module.Label"));
        props.setLook(wlModule);
        fdlModule=new FormData();
        fdlModule.left = new FormAttachment(0, 0);
        fdlModule.top = new FormAttachment(wConnectionGroup, 2*margin);
        //fdlModule.top  = new FormAttachment(wspecifyQuery, margin);
        fdlModule.right= new FormAttachment(middle, -margin);
        wlModule.setLayoutData(fdlModule);
        wModule=new ComboVar(transMeta,wSettingsGroup, SWT.BORDER | SWT.READ_ONLY);
        wModule.setEditable(true);
        props.setLook(wModule);
        wModule.addModifyListener(lsMod);
        fdModule=new FormData();
        fdModule.left = new FormAttachment(middle, margin);
        //fdModule.top  = new FormAttachment(wspecifyQuery, margin);
        fdModule.top  = new FormAttachment(wConnectionGroup, 2*margin);
        fdModule.right= new FormAttachment(100, -margin);
        wModule.setLayoutData(fdModule);
        wModule.addFocusListener(new FocusListener() {
            public void focusLost(org.eclipse.swt.events.FocusEvent e)
            {
            	getModulesListError = false;
            }
        
            public void focusGained(org.eclipse.swt.events.FocusEvent e)
            {
                // check if the URL and login credentials passed and not just had error 
            	if (Const.isEmpty(wSoapURL.getText()) || 
               		Const.isEmpty(wUserName.getText()) ||
            		Const.isEmpty(wPassword.getText()) ||
            		(getModulesListError )) return; 

                getModulesList();
            }
        } );

		
		wlPosition=new Label(wSettingsGroup, SWT.NONE); 
		props.setLook(wlPosition);
		fdlPosition=new FormData();
		fdlPosition.left  = new FormAttachment(middle,0);
		fdlPosition.right = new FormAttachment(100, 0);
		fdlPosition.bottom = new FormAttachment(100, -margin);
		wlPosition.setLayoutData(fdlPosition);
		
	    // condition
        wlCondition = new Label(wSettingsGroup, SWT.RIGHT);
        wlCondition.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Condition.Label"));
        props.setLook(wlCondition);
        fdlCondition = new FormData();
        fdlCondition.left = new FormAttachment(0, -margin);
        fdlCondition.top = new FormAttachment(wModule, margin);
        fdlCondition.right = new FormAttachment(middle, -margin);
        wlCondition.setLayoutData(fdlCondition);

        wCondition=new StyledTextComp(transMeta, wSettingsGroup, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "");
        wCondition.setToolTipText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Condition.Tooltip"));
        props.setLook(wCondition, Props.WIDGET_STYLE_FIXED);
        wCondition.addModifyListener(lsMod);
        fdCondition = new FormData();
        fdCondition.left = new FormAttachment(middle, margin);
        fdCondition.top = new FormAttachment(wModule, margin);
        fdCondition.right = new FormAttachment(100, -2*margin);
        fdCondition.bottom = new FormAttachment(wlPosition, -margin);
        wCondition.setLayoutData(fdCondition);
        wCondition.addModifyListener(new ModifyListener() {
            public void modifyText(ModifyEvent arg0)
            {
            	setQueryToolTip();
                setPosition(); 
            }
        });


		wCondition.addKeyListener(new KeyAdapter(){
			public void keyPressed(KeyEvent e) { setPosition(); }
			public void keyReleased(KeyEvent e) { setPosition(); }
			} 
		);
		wCondition.addFocusListener(new FocusAdapter(){
			public void focusGained(FocusEvent e) { setPosition(); }
			public void focusLost(FocusEvent e) { setPosition(); }
			}
		);
		wCondition.addMouseListener(new MouseAdapter(){
			public void mouseDoubleClick(MouseEvent e) { setPosition(); }
			public void mouseDown(MouseEvent e) { setPosition(); }
			public void mouseUp(MouseEvent e) { setPosition(); }
			}
		);	
		
		// Text Higlighting
		wCondition.addLineStyleListener(new SOQLValuesHighlight());
		
        FormData fdSettingsGroup= new FormData();
        fdSettingsGroup.left = new FormAttachment(0, 0);
        fdSettingsGroup.right = new FormAttachment(100, 0);
        fdSettingsGroup.bottom = new FormAttachment(100, 0);
        fdSettingsGroup.top = new FormAttachment(wConnectionGroup, margin);
        wSettingsGroup.setLayoutData(fdSettingsGroup);

        // END SETTINGS GROUP
        //////////////////////////
        
        
		fdFileComp = new FormData();
		fdFileComp.left = new FormAttachment(0, 0);
		fdFileComp.top = new FormAttachment(0, 0);
		fdFileComp.right = new FormAttachment(100, 0);
		fdFileComp.bottom = new FormAttachment(100, 0);
		wFileComp.setLayoutData(fdFileComp);

		wFileComp.layout();
		wFileTab.setControl(wFileComp);

		// ///////////////////////////////////////////////////////////
		// / END OF FILE TAB
		// ///////////////////////////////////////////////////////////

		// ////////////////////////
		// START OF CONTENT TAB///
		// /
		wContentTab = new CTabItem(wTabFolder, SWT.NONE);
		wContentTab.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Content.Tab"));

		FormLayout contentLayout = new FormLayout();
		contentLayout.marginWidth = 3;
		contentLayout.marginHeight = 3;

		wContentComp = new Composite(wTabFolder, SWT.NONE);
		props.setLook(wContentComp);
		wContentComp.setLayout(contentLayout);
		
		// ///////////////////////////////
		// START OF Additional Fields GROUP  //
		///////////////////////////////// 

		wAdditionalFields = new Group(wContentComp, SWT.SHADOW_NONE);
		props.setLook(wAdditionalFields);
		wAdditionalFields.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.wAdditionalFields.Label"));
		
		FormLayout AdditionalFieldsgroupLayout = new FormLayout();
		AdditionalFieldsgroupLayout.marginWidth = 10;
		AdditionalFieldsgroupLayout.marginHeight = 10;
		wAdditionalFields.setLayout(AdditionalFieldsgroupLayout);
		
		// Add Salesforce Soap URL in the output stream ?
		wlInclSoapURL = new Label(wAdditionalFields, SWT.RIGHT);
		wlInclSoapURL.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclSoapURL.Label"));
		props.setLook(wlInclSoapURL);
		fdlInclSoapURL = new FormData();
		fdlInclSoapURL.left = new FormAttachment(0, 0);
		fdlInclSoapURL.top = new FormAttachment(wAdvancedGroup, margin);
		fdlInclSoapURL.right = new FormAttachment(middle, -margin);
		wlInclSoapURL.setLayoutData(fdlInclSoapURL);
		wInclSoapURL = new Button(wAdditionalFields, SWT.CHECK);
		props.setLook(wInclSoapURL);
		wInclSoapURL.setToolTipText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclSoapURL.Tooltip"));
		fdInclSoapURL = new FormData();
		fdInclSoapURL.left = new FormAttachment(middle, 0);
		fdInclSoapURL.top = new FormAttachment(wAdvancedGroup, margin);
		wInclSoapURL.setLayoutData(fdInclSoapURL);
		wInclSoapURL.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) 
			{
				setEnableInclSoapURL();
			}
		});

		wlInclSoapURLField = new Label(wAdditionalFields, SWT.LEFT);
		wlInclSoapURLField.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclURLField.Label"));
		props.setLook(wlInclSoapURLField);
		fdlInclSoapURLField = new FormData();
		fdlInclSoapURLField.left = new FormAttachment(wInclSoapURL, margin);
		fdlInclSoapURLField.top = new FormAttachment(wAdvancedGroup, margin);
		wlInclSoapURLField.setLayoutData(fdlInclSoapURLField);
		wInclSoapURLField = new TextVar(transMeta,wAdditionalFields, SWT.SINGLE | SWT.LEFT	| SWT.BORDER);
		props.setLook(wlInclSoapURLField);
		wInclSoapURLField.addModifyListener(lsMod);
		fdInclSoapURLField = new FormData();
		fdInclSoapURLField.left = new FormAttachment(wlInclSoapURLField,margin);
		fdInclSoapURLField.top = new FormAttachment(wAdvancedGroup,  margin);
		fdInclSoapURLField.right = new FormAttachment(100, 0);
		wInclSoapURLField.setLayoutData(fdInclSoapURLField);
		
		//	Add module in the output stream ?
		wlInclModule = new Label(wAdditionalFields, SWT.RIGHT);
		wlInclModule.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclModule.Label"));
		props.setLook(wlInclModule);
		fdlInclModule = new FormData();
		fdlInclModule.left = new FormAttachment(0, 0);
		fdlInclModule.top = new FormAttachment(wInclSoapURLField, margin);
		fdlInclModule.right = new FormAttachment(middle, -margin);
		wlInclModule.setLayoutData(fdlInclModule);
		wInclModule = new Button(wAdditionalFields, SWT.CHECK);
		props.setLook(wInclModule);
		wInclModule.setToolTipText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclModule.Tooltip"));
		fdModule = new FormData();
		fdModule.left = new FormAttachment(middle, 0);
		fdModule.top = new FormAttachment(wInclSoapURLField, margin);
		wInclModule.setLayoutData(fdModule);

		wInclModule.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) 
			{
				setEnableInclModule();
			}
		});
		
		wlInclModuleField = new Label(wAdditionalFields, SWT.RIGHT);
		wlInclModuleField.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclModuleField.Label"));
		props.setLook(wlInclModuleField);
		fdlInclModuleField = new FormData();
		fdlInclModuleField.left = new FormAttachment(wInclModule, margin);
		fdlInclModuleField.top = new FormAttachment(wInclSoapURLField, margin);
		wlInclModuleField.setLayoutData(fdlInclModuleField);
		wInclModuleField = new TextVar(transMeta,wAdditionalFields, SWT.SINGLE | SWT.LEFT
				| SWT.BORDER);
		props.setLook(wInclModuleField);
		wInclModuleField.addModifyListener(lsMod);
		fdInclModuleField = new FormData();
		fdInclModuleField.left = new FormAttachment(wlInclModuleField, margin);
		fdInclModuleField.top = new FormAttachment(wInclSoapURLField, margin);
		fdInclModuleField.right = new FormAttachment(100, 0);
		wInclModuleField.setLayoutData(fdInclModuleField);


		// Add SQL in the output stream ?
		wlInclSQL = new Label(wAdditionalFields, SWT.RIGHT);
		wlInclSQL.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclSQL.Label"));
		props.setLook(wlInclSQL);
		fdlInclSQL = new FormData();
		fdlInclSQL.left = new FormAttachment(0, 0);
		fdlInclSQL.top = new FormAttachment(wInclModuleField, margin);
		fdlInclSQL.right = new FormAttachment(middle, -margin);
		wlInclSQL.setLayoutData(fdlInclSQL);
		wInclSQL = new Button(wAdditionalFields, SWT.CHECK);
		props.setLook(wInclSQL);
		wInclSQL.setToolTipText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclSQL.Tooltip"));
		fdInclSQL = new FormData();
		fdInclSQL.left = new FormAttachment(middle, 0);
		fdInclSQL.top = new FormAttachment(wInclModuleField, margin);
		wInclSQL.setLayoutData(fdInclSQL);
		wInclSQL.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) 
			{
				setEnableInclSQL();
			}
		});

		wlInclSQLField = new Label(wAdditionalFields, SWT.LEFT);
		wlInclSQLField.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclSQLField.Label"));
		props.setLook(wlInclSQLField);
		fdlInclSQLField = new FormData();
		fdlInclSQLField.left = new FormAttachment(wInclSQL, margin);
		fdlInclSQLField.top = new FormAttachment(wInclModuleField, margin);
		wlInclSQLField.setLayoutData(fdlInclSQLField);
		wInclSQLField = new TextVar(transMeta,wAdditionalFields, SWT.SINGLE | SWT.LEFT	| SWT.BORDER);
		props.setLook(wlInclSQLField);
		wInclSQLField.addModifyListener(lsMod);
		fdInclSQLField = new FormData();
		fdInclSQLField.left = new FormAttachment(wlInclSQLField,margin);
		fdInclSQLField.top = new FormAttachment(wInclModuleField,  margin);
		fdInclSQLField.right = new FormAttachment(100, 0);
		wInclSQLField.setLayoutData(fdInclSQLField);
		
		// Add Timestamp in the output stream ?
		wlInclTimestamp = new Label(wAdditionalFields, SWT.RIGHT);
		wlInclTimestamp.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclTimestamp.Label"));
		props.setLook(wlInclTimestamp);
		fdlInclTimestamp = new FormData();
		fdlInclTimestamp.left = new FormAttachment(0, 0);
		fdlInclTimestamp.top = new FormAttachment(wInclSQLField, margin);
		fdlInclTimestamp.right = new FormAttachment(middle, -margin);
		wlInclTimestamp.setLayoutData(fdlInclTimestamp);
		wInclTimestamp = new Button(wAdditionalFields, SWT.CHECK);
		props.setLook(wInclTimestamp);
		wInclTimestamp.setToolTipText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclTimestamp.Tooltip"));
		fdInclTimestamp = new FormData();
		fdInclTimestamp.left = new FormAttachment(middle, 0);
		fdInclTimestamp.top = new FormAttachment(wInclSQLField, margin);
		wInclTimestamp.setLayoutData(fdInclTimestamp);
		wInclTimestamp.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) 
			{
				setEnableInclTimestamp();
			}
		});

		wlInclTimestampField = new Label(wAdditionalFields, SWT.LEFT);
		wlInclTimestampField.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclTimestampField.Label"));
		props.setLook(wlInclTimestampField);
		fdlInclTimestampField = new FormData();
		fdlInclTimestampField.left = new FormAttachment(wInclTimestamp, margin);
		fdlInclTimestampField.top = new FormAttachment(wInclSQLField, margin);
		wlInclTimestampField.setLayoutData(fdlInclTimestampField);
		wInclTimestampField = new TextVar(transMeta,wAdditionalFields, SWT.SINGLE | SWT.LEFT	| SWT.BORDER);
		props.setLook(wlInclTimestampField);
		wInclTimestampField.addModifyListener(lsMod);
		fdInclTimestampField = new FormData();
		fdInclTimestampField.left = new FormAttachment(wlInclTimestampField,margin);
		fdInclTimestampField.top = new FormAttachment(wInclSQLField,  margin);
		fdInclTimestampField.right = new FormAttachment(100, 0);
		wInclTimestampField.setLayoutData(fdInclTimestampField);
		
		
		// Include Rownum in output stream?
		wlInclRownum=new Label(wAdditionalFields, SWT.RIGHT);
		wlInclRownum.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclRownum.Label"));
 		props.setLook(wlInclRownum);
		fdlInclRownum=new FormData();
		fdlInclRownum.left = new FormAttachment(0, 0);
		fdlInclRownum.top  = new FormAttachment(wInclTimestampField, margin);
		fdlInclRownum.right= new FormAttachment(middle, -margin);
		wlInclRownum.setLayoutData(fdlInclRownum);
		wInclRownum=new Button(wAdditionalFields, SWT.CHECK );
 		props.setLook(wInclRownum);
		wInclRownum.setToolTipText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclRownum.Tooltip"));
		fdRownum=new FormData();
		fdRownum.left = new FormAttachment(middle, 0);
		fdRownum.top  = new FormAttachment(wInclTimestampField, margin);
		wInclRownum.setLayoutData(fdRownum);

		wInclRownum.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) 
			{
				setEnableInclRownum();
			}
		});
		
		wlInclRownumField=new Label(wAdditionalFields, SWT.RIGHT);
		wlInclRownumField.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.InclRownumField.Label"));
 		props.setLook(wlInclRownumField);
		fdlInclRownumField=new FormData();
		fdlInclRownumField.left = new FormAttachment(wInclRownum, margin);
		fdlInclRownumField.top  = new FormAttachment(wInclTimestampField, margin);
		wlInclRownumField.setLayoutData(fdlInclRownumField);
		wInclRownumField=new TextVar(transMeta,wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wInclRownumField);
		wInclRownumField.addModifyListener(lsMod);
		fdInclRownumField=new FormData();
		fdInclRownumField.left = new FormAttachment(wlInclRownumField, margin);
		fdInclRownumField.top  = new FormAttachment(wInclTimestampField, margin);
		fdInclRownumField.right= new FormAttachment(100, 0);
		wInclRownumField.setLayoutData(fdInclRownumField);

		
		fdAdditionalFields = new FormData();
		fdAdditionalFields.left = new FormAttachment(0, margin);
		fdAdditionalFields.top = new FormAttachment(0, 2*margin);
		fdAdditionalFields.right = new FormAttachment(100, -margin);
		wAdditionalFields.setLayoutData(fdAdditionalFields);
		
		// ///////////////////////////////////////////////////////////
		// / END OF Additional Fields GROUP
		// ///////////////////////////////////////////////////////////	
	
		// Timeout
		wlTimeOut = new Label(wContentComp, SWT.RIGHT);
		wlTimeOut.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.TimeOut.Label"));
		props.setLook(wlTimeOut);
		fdlTimeOut = new FormData();
		fdlTimeOut.left = new FormAttachment(0, 0);
		fdlTimeOut.top = new FormAttachment(wAdditionalFields, 2*margin);
		fdlTimeOut.right = new FormAttachment(middle, -margin);
		wlTimeOut.setLayoutData(fdlTimeOut);
		wTimeOut = new TextVar(transMeta,wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wTimeOut);
		wTimeOut.addModifyListener(lsMod);
		fdTimeOut = new FormData();
		fdTimeOut.left = new FormAttachment(middle, 0);
		fdTimeOut.top = new FormAttachment(wAdditionalFields, 2*margin);
		fdTimeOut.right = new FormAttachment(100, 0);
		wTimeOut.setLayoutData(fdTimeOut);
		
		
		// Use compression?
		wlUseCompression=new Label(wContentComp, SWT.RIGHT);
		wlUseCompression.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.UseCompression.Label"));
 		props.setLook(wlUseCompression);
		fdlUseCompression=new FormData();
		fdlUseCompression.left = new FormAttachment(0, 0);
		fdlUseCompression.top  = new FormAttachment(wTimeOut, margin);
		fdlUseCompression.right= new FormAttachment(middle, -margin);
		wlUseCompression.setLayoutData(fdlUseCompression);
		wUseCompression=new Button(wContentComp, SWT.CHECK );
 		props.setLook(wUseCompression);
		wUseCompression.setToolTipText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.UseCompression.Tooltip"));
		fdUseCompression=new FormData();
		fdUseCompression.left = new FormAttachment(middle, 0);
		fdUseCompression.top  = new FormAttachment(wTimeOut, margin);
		wUseCompression.setLayoutData(fdUseCompression);

		
		// Limit rows
		wlLimit = new Label(wContentComp, SWT.RIGHT);
		wlLimit.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Limit.Label"));
		props.setLook(wlLimit);
		fdlLimit = new FormData();
		fdlLimit.left = new FormAttachment(0, 0);
		fdlLimit.top = new FormAttachment(wUseCompression, margin);
		fdlLimit.right = new FormAttachment(middle, -margin);
		wlLimit.setLayoutData(fdlLimit);
		wLimit = new TextVar(transMeta,wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wLimit);
		wLimit.addModifyListener(lsMod);
		fdLimit = new FormData();
		fdLimit.left = new FormAttachment(middle, 0);
		fdLimit.top = new FormAttachment(wUseCompression, margin);
		fdLimit.right = new FormAttachment(100, 0);
		wLimit.setLayoutData(fdLimit);

		fdContentComp = new FormData();
		fdContentComp.left = new FormAttachment(0, 0);
		fdContentComp.top = new FormAttachment(0, 0);
		fdContentComp.right = new FormAttachment(100, 0);
		fdContentComp.bottom = new FormAttachment(100, 0);
		wContentComp.setLayoutData(fdContentComp);

		wContentComp.layout();
		wContentTab.setControl(wContentComp);

		// ///////////////////////////////////////////////////////////
		// / END OF CONTENT TAB
		// ///////////////////////////////////////////////////////////

		// Fields tab...
		//
		wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
		wFieldsTab.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Fields.Tab"));

		FormLayout fieldsLayout = new FormLayout();
		fieldsLayout.marginWidth = Const.FORM_MARGIN;
		fieldsLayout.marginHeight = Const.FORM_MARGIN;

		wFieldsComp = new Composite(wTabFolder, SWT.NONE);
		wFieldsComp.setLayout(fieldsLayout);
		props.setLook(wFieldsComp);

		wGet = new Button(wFieldsComp, SWT.PUSH);
		wGet.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.GetFields.Button"));
		fdGet = new FormData();
		fdGet.left = new FormAttachment(50, 0);
		fdGet.bottom = new FormAttachment(100, 0);
		wGet.setLayoutData(fdGet);


		final int FieldsRows = input.getInputFields().length;

		colinf = new ColumnInfo[] {
				new ColumnInfo(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.Name.Column"),
						ColumnInfo.COLUMN_TYPE_TEXT, false),
				new ColumnInfo(
						BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.Field.Column"),
						ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false),
				new ColumnInfo(
						BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.IsIdLookup.Column"),
						ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {
								BaseMessages.getString(PKG, "System.Combo.Yes"),
								BaseMessages.getString(PKG, "System.Combo.No") }, true),
				new ColumnInfo(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.Type.Column"),
						ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMeta.getTypes(), true),
				new ColumnInfo(
						BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.Format.Column"),
						ColumnInfo.COLUMN_TYPE_FORMAT, 3),
				new ColumnInfo(
						BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.Length.Column"),
						ColumnInfo.COLUMN_TYPE_TEXT, false),
				new ColumnInfo(
						BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.Precision.Column"),
						ColumnInfo.COLUMN_TYPE_TEXT, false),
				new ColumnInfo(
						BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.Currency.Column"),
						ColumnInfo.COLUMN_TYPE_TEXT, false),
				new ColumnInfo(
						BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.Decimal.Column"),
						ColumnInfo.COLUMN_TYPE_TEXT, false),
				new ColumnInfo(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.Group.Column"),
						ColumnInfo.COLUMN_TYPE_TEXT, false),
				new ColumnInfo(
						BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.TrimType.Column"),
						ColumnInfo.COLUMN_TYPE_CCOMBO,
						SalesforceInputField.trimTypeDesc, true),
				new ColumnInfo(
						BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.Repeat.Column"),
						ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {
								BaseMessages.getString(PKG, "System.Combo.Yes"),
								BaseMessages.getString(PKG, "System.Combo.No") }, true),

		};

		colinf[0].setUsingVariables(true);
		colinf[0].setToolTip(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.Name.Column.Tooltip"));
		colinf[1].setUsingVariables(true);
		colinf[1].setToolTip(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.FieldsTable.Field.Column.Tooltip"));
		colinf[2].setReadOnly(true);
		wFields=new TableView(transMeta,wFieldsComp, 
			      SWT.FULL_SELECTION | SWT.MULTI, 
			      colinf, 
			      FieldsRows,  
			      lsMod,
				  props
			      );
		
		fdFields = new FormData();
		fdFields.left = new FormAttachment(0, 0);
		fdFields.top = new FormAttachment(0, 0);
		fdFields.right = new FormAttachment(100, 0);
		fdFields.bottom = new FormAttachment(wGet, -margin);
		wFields.setLayoutData(fdFields);

		fdFieldsComp = new FormData();
		fdFieldsComp.left = new FormAttachment(0, 0);
		fdFieldsComp.top = new FormAttachment(0, 0);
		fdFieldsComp.right = new FormAttachment(100, 0);
		fdFieldsComp.bottom = new FormAttachment(100, 0);
		wFieldsComp.setLayoutData(fdFieldsComp);

		wFieldsComp.layout();
		wFieldsTab.setControl(wFieldsComp);

		fdTabFolder = new FormData();
		fdTabFolder.left = new FormAttachment(0, 0);
		fdTabFolder.top = new FormAttachment(wStepname, margin);
		fdTabFolder.right = new FormAttachment(100, 0);
		fdTabFolder.bottom = new FormAttachment(100, -50);
		wTabFolder.setLayoutData(fdTabFolder);
		
		

		// THE BUTTONS
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));

		wPreview = new Button(shell, SWT.PUSH);
		wPreview.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Button.PreviewRows"));

		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

		setButtonPositions(new Button[] { wOK, wPreview, wCancel }, margin, wTabFolder);

		// Add listeners
		lsOK = new Listener() {
			public void handleEvent(Event e) {
				ok();
			}
		};
		lsTest     = new Listener() { public void handleEvent(Event e) { test(); } };
		lsGet = new Listener() {
			public void handleEvent(Event e) {
		        Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
		        shell.setCursor(busy);
				get();
		        shell.setCursor(null);
		        busy.dispose();
			}
		};
		lsPreview = new Listener() {
			public void handleEvent(Event e) {
				preview();
			}
		};
		lsCancel = new Listener() {
			public void handleEvent(Event e) {
				cancel();
			}
		};

		wOK.addListener(SWT.Selection, lsOK);
		wGet.addListener(SWT.Selection, lsGet);
		wTest.addListener    (SWT.Selection, lsTest    );	
		wPreview.addListener(SWT.Selection, lsPreview);
		wCancel.addListener(SWT.Selection, lsCancel);

		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent e) {
				ok();
			}
		};

		wStepname.addSelectionListener(lsDef);
		wLimit.addSelectionListener(lsDef);
		wInclModuleField.addSelectionListener(lsDef);
		wInclSoapURLField.addSelectionListener(lsDef);


		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {
				cancel();
			}
		});

		wTabFolder.setSelection(0);

		// Set the shell size, based upon previous time...
		setSize();
		getData(input);
		setEnableInclSoapURL();
		setEnableInclSQL();
		setEnableInclTimestamp();
		setEnableInclModule();
		setEnableInclRownum();
	
		input.setChanged(changed);

		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch())
				display.sleep();
		}
		return stepname;
	}
	public void checkPasswordVisible()
    {
        String password = wPassword.getText();
        List<String> list = new ArrayList<String>();
        StringUtil.getUsedVariables(password, list, true);
        if (list.size() == 0)
            wPassword.setEchoChar('*');
        else
            wPassword.setEchoChar('\0'); // Show it all...
    }
	  
 	private void setEnableInclSoapURL()
 	{
 		wInclSoapURLField.setEnabled(wInclSoapURL.getSelection());
 		wlInclSoapURLField.setEnabled(wInclSoapURL.getSelection());
 	}

 	private void setEnableInclSQL()
 	{
 		wInclSQLField.setEnabled(wInclSQL.getSelection());
 		wlInclSQLField.setEnabled(wInclSQL.getSelection());
 	}
 	private void setEnableInclTimestamp()
 	{
 		wInclTimestampField.setEnabled(wInclTimestamp.getSelection());
 		wlInclTimestampField.setEnabled(wInclTimestamp.getSelection());
 	}
 
 	private void setEnableInclModule()
 	{
 		wInclModuleField.setEnabled(wInclModule.getSelection() /*&& !wspecifyQuery.getSelection()*/);
		wlInclModuleField.setEnabled(wInclModule.getSelection() /*&& !wspecifyQuery.getSelection()*/);
 	}
 	private void setEnableInclRownum()
 	{
 		wInclRownumField.setEnabled(wInclRownum.getSelection());
 		wlInclRownumField.setEnabled(wInclRownum.getSelection());
 	}
 
 	private void test(){
	 
	 boolean successConnection=true;
	 String msgError=null;
	 SalesforceBulkConnection connection=null;
	 try
     {
			SalesforceBulkInputMeta meta = new SalesforceBulkInputMeta();
			getInfo(meta);
			
			// get real values
			String soapURL=transMeta.environmentSubstitute(meta.getSoapURL());
			String realUsername=transMeta.environmentSubstitute(meta.getUserName());
			String realPassword=transMeta.environmentSubstitute(meta.getPassword());
			int realTimeOut=Const.toInt(transMeta.environmentSubstitute(meta.getTimeOut()),0);

			connection=new SalesforceBulkConnection(log, soapURL,realUsername,realPassword); 
			connection.setTimeOut(realTimeOut);
			connection.connect();
			
		}
		catch(Exception e) {
			successConnection=false;
			msgError=e.getMessage();
		} finally{
			if(connection!=null) {
				try {connection.close();}catch(Exception e){};
			}
		}
		if(successConnection) {
			
			MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION );
			mb.setMessage(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Connected.OK",wUserName.getText()) +Const.CR);
			mb.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Connected.Title.Ok")); 
			mb.open();
		}else{
			new ErrorDialog(shell,BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Connected.Title.Error"),
					BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Connected.NOK",wUserName.getText()),new Exception(msgError));
		}
	}


 	private void get() {
 		SalesforceBulkConnection connection = null;
 		try {

 			SalesforceBulkInputMeta meta = new SalesforceBulkInputMeta();
 			getInfo(meta);

 			// Clear Fields Grid
 			wFields.removeAll();

 			// get real values
 			String realModule = transMeta.environmentSubstitute(meta.getModule());
 			String soapURL = transMeta.environmentSubstitute(meta.getSoapURL());
 			String realUsername = transMeta.environmentSubstitute(meta.getUserName());
 			String realPassword = transMeta.environmentSubstitute(meta.getPassword());
 			int realTimeOut = Const.toInt(transMeta.environmentSubstitute(meta.getTimeOut()), 0);

 			connection = new SalesforceBulkConnection(log, soapURL, realUsername, realPassword);
 			connection.setTimeOut(realTimeOut);
 			String[] fieldsName = null;
 			
 			connection.connect();

 			Field[] fields = connection.getObjectFields(realModule);
 			fieldsName = new String[fields.length];
 			for (int i = 0; i < fields.length; i++) {
 				Field field = fields[i];
 				fieldsName[i] = field.getName();
 				addField(field);
 			}
 			if (fieldsName != null)
 				colinf[1].setComboValues(fieldsName);
 			wFields.removeEmptyRows();
 			wFields.setRowNums();
 			wFields.optWidth(true);
 		} catch (KettleException e) {
 			new ErrorDialog(shell, BaseMessages.getString(PKG, "SalesforceBulkInputMeta.ErrorRetrieveData.DialogTitle"),
 					BaseMessages.getString(PKG, "SalesforceBulkInputMeta.ErrorRetrieveData.DialogMessage"), e);
 		} catch (Exception e) {
 			new ErrorDialog(shell, BaseMessages.getString(PKG, "SalesforceBulkInputMeta.ErrorRetrieveData.DialogTitle"),
 					BaseMessages.getString(PKG, "SalesforceBulkInputMeta.ErrorRetrieveData.DialogMessage"), e);
 		} finally {
 			if (connection != null) {
 				try {
 					connection.close();
 				} catch (Exception e) {
 				}
 			}
 		}
 	}

 	private void addField(Field field) {
 		String fieldType = field.getType().getValue();

 		String fieldLength = null;
 		String fieldPrecision = null;
 		if (!fieldType.equals("boolean") && !fieldType.equals("datetime") && !fieldType.equals("date")) {
 			fieldLength = Integer.toString(field.getLength());
 			fieldPrecision = Integer.toString(field.getPrecision());
 		}

 		addField(field.getLabel(), field.getName(), field.isIdLookup(), field.getType().getValue(), fieldLength,
 				fieldPrecision);
 	}


 	private void addField(String fieldLabel, String fieldName, boolean fieldIdIsLookup, String fieldType,
 			String fieldLength, String fieldPrecision) {
 		TableItem item = new TableItem(wFields.table, SWT.NONE);
 		item.setText(1, fieldLabel);
 		item.setText(2, fieldName);
 		item.setText(3, fieldIdIsLookup ? BaseMessages.getString(PKG, "System.Combo.Yes") : 
 										  BaseMessages.getString(PKG,"System.Combo.No"));

 		// Try to get the Type
 		if (fieldType.equals("boolean")) {
 			item.setText(4, "Boolean");
 		} else if (fieldType.equals("date")) {
 			item.setText(4, "Date");
 			item.setText(5, DEFAULT_DATE_FORMAT);
 		} else if (fieldType.equals("datetime")) {
 			item.setText(4, "Date");
 			item.setText(5, DEFAULT_DATE_TIME_FORMAT);
 		} else if (fieldType.equals("double")) {
 			item.setText(4, "Number");
 		} else if (fieldType.equals("int")) {
 			item.setText(4, "Integer");
 		} else {
 			item.setText(4, "String");
 		}

 		if (fieldLength != null) {
 			item.setText(6, fieldLength);
 		}
 		// Get precision
 		if (fieldPrecision != null) {
 			item.setText(7, fieldPrecision);
 		}
 	}

	/**
	 * Read the data from the TextFileInputMeta object and show it in this dialog.
	 * 
	 * @param in The SalesforceBulkInputMeta object to obtain the data from.
	 */
	public void getData(SalesforceBulkInputMeta in) 
	{
		wSoapURL.setText(Const.NVL(in.getSoapURL(),""));
		wUserName.setText(Const.NVL(in.getUserName(),""));
		wPassword.setText(Const.NVL(in.getPassword(),""));
		wModule.setText(Const.NVL(in.getModule(), "Account"));
		wCondition.setText(Const.NVL(in.getCondition(),""));
		
		//wspecifyQuery.setSelection(in.isSpecifyQuery());
		//wQuery.setText(Const.NVL(in.getQuery(),""));
		wInclSoapURLField.setText(Const.NVL(in.getSoapURLField(),""));
		wInclSoapURL.setSelection(in.includeSoapURL());
		
		wInclSQLField.setText(Const.NVL(in.getSQLField(),""));
		wInclSQL.setSelection(in.includeSQL());
		
		wInclTimestampField.setText(Const.NVL(in.getTimestampField(),""));
		wInclTimestamp.setSelection(in.includeTimestamp());
		
		wInclModuleField.setText(Const.NVL(in.getModuleField(),""));
		wInclModule.setSelection(in.includeModule());
		
		wInclRownumField.setText(Const.NVL(in.getRowNumberField(),""));
		wInclRownum.setSelection(in.includeRowNumber());
		
		wTimeOut.setText(Const.NVL(in.getTimeOut(), SalesforceBulkConnectionUtils.DEFAULT_TIMEOUT));
		wUseCompression.setSelection(in.isUsingCompression());
		wLimit.setText("" + in.getRowLimit());
		
		if(log.isDebug()) logDebug(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Log.GettingFieldsInfo"));
		for (int i = 0; i < in.getInputFields().length; i++) 
		{
			SalesforceInputField field = in.getInputFields()[i];

			if (field != null) {
				TableItem item = wFields.table.getItem(i);
				String name = field.getName();
				String path = field.getField();
				String isidlookup = field.isIdLookup() ? 
						BaseMessages.getString(PKG, "System.Combo.Yes") : 
						BaseMessages.getString(PKG, "System.Combo.No");
				String type = field.getTypeDesc();
				String format = field.getFormat();
				String length = "" + field.getLength();
				String prec = "" + field.getPrecision();
				String curr = field.getCurrencySymbol();
				String group = field.getGroupSymbol();
				String decim = field.getDecimalSymbol();
				String trim = field.getTrimTypeDesc();
				String rep = field.isRepeated() ? 
						BaseMessages.getString(PKG, "System.Combo.Yes") : 
						BaseMessages.getString(PKG, "System.Combo.No");

				if (name != null)
					item.setText(1, name);
				if (path != null)
					item.setText(2, path);
				if (isidlookup != null)
					item.setText(3, isidlookup);
				if (type != null)
					item.setText(4, type);
				if (format != null)
					item.setText(5, format);
				if (length != null && !"-1".equals(length))
					item.setText(6, length);
				if (prec != null && !"-1".equals(prec))
					item.setText(7, prec);
				if (curr != null)
					item.setText(8, curr);
				if (decim != null)
					item.setText(9, decim);
				if (group != null)
					item.setText(10, group);
				if (trim != null)
					item.setText(11, trim);
				if (rep != null)
					item.setText(12, rep);
			}
		}

		wFields.removeEmptyRows();
		wFields.setRowNums();
		wFields.optWidth(true);

		wStepname.selectAll();
	}

	private void cancel() {
		stepname = null;
		input.setChanged(changed);
		dispose();
	}

	private void ok() {
		if (Const.isEmpty(wStepname.getText())) return;

		stepname = wStepname.getText();
		try {
			getInfo(input);
		} catch (KettleException e) {
			new ErrorDialog(
					shell,BaseMessages.getString(PKG, "SalesforceBulkInputDialog.ErrorValidateData.DialogTitle"),
					BaseMessages.getString(PKG, "SalesforceBulkInputDialog.ErrorValidateData.DialogMessage"),	e);
		}
		dispose();
	}

	private void getInfo(SalesforceBulkInputMeta in) throws KettleException {
		stepname = wStepname.getText(); // return value

		// copy info to SalesforceInputMeta class (input)
		in.setSoapURL(Const.NVL(wSoapURL.getText(),SalesforceBulkConnectionUtils.SOAP_DEFAULT_URL));
		in.setUserName(Const.NVL(wUserName.getText(),""));
		in.setPassword(Const.NVL(wPassword.getText(),""));
		in.setModule(Const.NVL(wModule.getText(),"Account"));
		in.setCondition(Const.NVL(wCondition.getText(),""));
		
		//in.setSpecifyQuery(wspecifyQuery.getSelection());
		//in.setQuery(Const.NVL(wQuery.getText(),""));
		in.setUseCompression(wUseCompression.getSelection());
		in.setTimeOut(Const.NVL(wTimeOut.getText(),"0"));
		in.setRowLimit(Const.NVL(wLimit.getText(),"0"));
		in.setSoapURLField(Const.NVL(wInclSoapURLField.getText(),""));
		in.setSQLField(Const.NVL(wInclSQLField.getText(),""));
		in.setTimestampField(Const.NVL(wInclTimestampField.getText(),""));
		in.setModuleField(Const.NVL(wInclModuleField.getText(),""));
		in.setRowNumberField(Const.NVL(wInclRownumField.getText(),""));
		in.setIncludeSoapURL(wInclSoapURL.getSelection());
		in.setIncludeSQL(wInclSQL.getSelection());
		in.setIncludeTimestamp(wInclTimestamp.getSelection());
		in.setIncludeModule(wInclModule.getSelection());
		in.setIncludeRowNumber(wInclRownum.getSelection());
		int nrFields = wFields.nrNonEmpty();

		in.allocate(nrFields);

		for (int i = 0; i < nrFields; i++) {
			SalesforceInputField field = new SalesforceInputField();

			TableItem item = wFields.getNonEmpty(i);

			field.setName(item.getText(1));
			field.setField(item.getText(2));
			field.setIdLookup(BaseMessages.getString(PKG, "System.Combo.Yes").equalsIgnoreCase(item.getText(3)));
			field.setType(ValueMeta.getType(item.getText(4)));
			field.setFormat(item.getText(5));
			field.setLength(Const.toInt(item.getText(6), -1));
			field.setPrecision(Const.toInt(item.getText(7), -1));
			field.setCurrencySymbol(item.getText(8));
			field.setDecimalSymbol(item.getText(9));
			field.setGroupSymbol(item.getText(10));
			field.setTrimType(SalesforceInputField.getTrimTypeByDesc(item.getText(11)));
			field.setRepeated(BaseMessages.getString(PKG, "System.Combo.Yes").equalsIgnoreCase(item.getText(12)));

			in.getInputFields()[i] = field;
		}
	}

	// Preview the data
	private void preview() {
		try {
			SalesforceBulkInputMeta oneMeta = new SalesforceBulkInputMeta();
			getInfo(oneMeta);

			// check if the path is given
			
			 TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation(transMeta, oneMeta, wStepname.getText());
	            
			EnterNumberDialog numberDialog = new EnterNumberDialog(
					shell,
					props.getDefaultPreviewSize(),
					BaseMessages.getString(PKG, "SalesforceBulkInputDialog.NumberRows.DialogTitle"),
					BaseMessages.getString(PKG, "SalesforceBulkInputDialog.NumberRows.DialogMessage"));
			int previewSize = numberDialog.open();
			if (previewSize > 0) {
				TransPreviewProgressDialog progressDialog = new TransPreviewProgressDialog(
						shell, previewMeta,
						new String[] { wStepname.getText() },
						new int[] { previewSize });
				progressDialog.open();

				if (!progressDialog.isCancelled()) {
					Trans trans = progressDialog.getTrans();
					String loggingText = progressDialog.getLoggingText();

					if (trans.getResult() != null
							&& trans.getResult().getNrErrors() > 0) {
						EnterTextDialog etd = new EnterTextDialog(
								shell,BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
								BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"),loggingText, true);
						etd.setReadOnly();
						etd.open();
					}

                    PreviewRowsDialog prd = new PreviewRowsDialog(shell, transMeta, SWT.NONE, wStepname.getText(),
							progressDialog.getPreviewRowsMeta(wStepname.getText()), progressDialog
									.getPreviewRows(wStepname.getText()), loggingText);
					prd.open();
				}
			}
		} catch (KettleException e) {
			new ErrorDialog(
					shell,
					BaseMessages.getString(PKG, "SalesforceBulkInputDialog.ErrorPreviewingData.DialogTitle"),
					BaseMessages.getString(PKG, "SalesforceBulkInputDialog.ErrorPreviewingData.DialogMessage"),
					e);
		}
	}
	private void getModulesList()
	{
		if (!gotModule){
			SalesforceBulkConnection connection=null;
			String selectedField=wModule.getText();
			wModule.removeAll();

			try{
				SalesforceBulkInputMeta meta = new SalesforceBulkInputMeta();
				getInfo(meta);
				String soapurl = transMeta.environmentSubstitute(meta.getSoapURL());
				  
				// Define a new Salesforce Bulk connection
				connection=new SalesforceBulkConnection(log, soapurl, transMeta.environmentSubstitute(meta.getUserName()),transMeta.environmentSubstitute(meta.getPassword())); 
				// connect to Salesforce
				connection.connect();
				  
				// retrieve modules list
				String[] modules = connection.getAllAvailableObjects(true);
				if(modules!=null && modules.length>0) {
					// populate Combo
					wModule.setItems(modules);	
				}
				  
			    gotModule = true;
	        	getModulesListError = false;
			}catch(Exception e) {
				new ErrorDialog(shell,BaseMessages.getString(PKG, "SalesforceBulkInputDialog.ErrorRetrieveModules.DialogTitle"),
								BaseMessages.getString(PKG, "SalesforceBulkInputDialog.ErrorRetrieveData.ErrorRetrieveModules"),e);
				getModulesListError = true;
			} finally{
				if(!Const.isEmpty(selectedField)) wModule.setText(selectedField);
				if(connection!=null) {
					try {connection.close();}catch(Exception e){};
				}
		 	}
		}
	}
	protected void setQueryToolTip()
    {
		StyledTextComp control = wCondition;
		/*if(wspecifyQuery.getSelection()) control = wQuery;*/
		control.setToolTipText(transMeta.environmentSubstitute(control.getText()));
    }
	public void setPosition(){
		StyledTextComp control = wCondition;
		/*if(wspecifyQuery.getSelection()) control = wQuery;*/
		
		String scr = control.getText();
		int linenr = control.getLineAtOffset(control.getCaretOffset())+1;
		int posnr  = control.getCaretOffset();
				
		// Go back from position to last CR: how many positions?
		int colnr=0;
		while (posnr>0 && scr.charAt(posnr-1)!='\n' && scr.charAt(posnr-1)!='\r')
		{
			posnr--;
			colnr++;
		}
		wlPosition.setText(BaseMessages.getString(PKG, "SalesforceBulkInputDialog.Position.Label",""+linenr,""+colnr));

	}
}
