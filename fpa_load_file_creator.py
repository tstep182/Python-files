
from ayx import Alteryx
import pandas as pd
import numpy as np
import os
import datetime
import glob
import re
import sys
import json
import logging
from IPython.display import display
from pandas.api.types import CategoricalDtype
from IPython.core.interactiveshell import InteractiveShell


##########################################################################################
# NOTE: This code runs in a single Python tool within an Alteryx workflow                    
# It can optionally be split into multiple cells in the tool's Jupyter notebook
# However, the entire DataLoader class should be confined to a single cell
##########################################################################################

# Enable logging
log_file = r'\\disk23\fin_plan-shared\Automation-FPA\Load_Files\Logs\fpa_load_files.log'

logging.basicConfig(
    filename=log_file,
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


print('Python version running on the Alteryx server:')
print(sys.version_info)

print('Pandas version running on the Alteryx server:')
print(pd.__version__) # Get the pandas version running on the Alteryx server


# Get the current date and time to append to the output file names
now = datetime.datetime.now()
current_datetime = str(now.year) + '-' + str(now.month).zfill(2) + '-' + str(now.day).zfill(2) + '-' + str(now.hour).zfill(2) + str(now.minute).zfill(2)


class DataLoader:

    print('Creating the DataLoader object...')
    
    def __init__(self, input_files, summary_info):
        
        self.user_id = summary_info['user_id']
        self.user_email = summary_info['user_email']
        self.workbook_name = summary_info['workbook_name']
        self.load_sheet_name = summary_info['load_sheet_name']
        self.enhanced_file_name = self.user_id + '_' + self.workbook_name + '_' + self.load_sheet_name + '.txt'
        
        self.summary_info = summary_info
        self.input_files = input_files
        
        self.df = self.input_files['LOADSHEET']
        self.finstmt_backup = self.input_files['BACKUP']
        
        print('DataLoader object created.')
        
        
        
    def validate_loadfile_name(self):
    
        print('Running validate_loadfile_name...')

        print('Workbook name: ' + self.workbook_name)
        print('Load Sheet name: ' + self.load_sheet_name)

        # Ensure the combined workbook and load sheet names are not too long (more than 80 characters)
        if len(self.workbook_name) + len(self.load_sheet_name) > 80:
            
            # Set error email info
            error_email_info = {}
            error_email_info['error_email_subject'] = 'WARNING: Load process failed - see attachment for details'
            error_email_info['error_email_filepath'] = r'\\disk23\fin_plan-shared\Automation-FPA\Load_Files\Validation_Errors\Validation_Errors_' + self.enhanced_file_name
            error_email_info['error_email_body'] = 'The load failed because the combined length of the workbook and load sheet names exceeds 80 characters. See the attachment for details. NOTE: No data on your sheet has been loaded.'
            
            error_log_entry = "Validation failed because the combined names of the workbook and load sheet exceed 80 characters. For details see Validation_Errors_" + self.enhanced_file_name + r" at \\disk23\fin_plan-shared\Automation-FPA\Load_Files\Validation_Errors"
            
            print('Combined length of names = ' + str(len(self.workbook_name) + len(self.load_sheet_name)))
            validation_errors = {}
            validation_errors['workbook_name_error'] = 'The combined workbook and load sheet names exceed 80 characters'
            validation_errors['workbook_name_length'] = 'Workbook name length: ' + str(len(self.workbook_name))
            validation_errors['load_sheet_name_length'] = 'Load sheet name length: ' + str(len(self.load_sheet_name))
            validation_errors_df = pd.DataFrame.from_dict(validation_errors, orient='index', columns=['Details'])
            ret = self.create_error_file(validation_errors_df, error_email_info, error_log_entry)
            print('The combined workbook and load sheet names exceed 80 characters')
            return False
        else:
            print('The workbook and load sheet names are within their limits')
            return True
            
            
            
    def validate_dimension_files(self):
        
        print("Running validate_dimension_files...")
    
        invalid_dim_files = {}

        # Check the number of lines in each dataframe
        # The files that populate the dataframes are from an external source and occassionally they are incomplete
        # The numbers shown for each dimension reflect a fully complete file (in August 2022)
        # If any of them are not complete, we can't use them to validate the members on the load sheet, and the load must be aborted
        if len(self.input_files['ACCT'].index) < 2796:
            invalid_dim_files['ACCT'] = 'The Account dimension file contains only ' + str(len(self.input_files['ACCT'].index)) + ' lines'

        if len(self.input_files['CC'].index) < 1743:
            invalid_dim_files['CC'] = 'The Cost Center dimension file contains only ' + str(len(self.input_files['CC'].index)) + ' lines'

        if len(self.input_files['IO'].index) < 257:
            invalid_dim_files['IO'] = 'The Internal Order dimension file contains only ' + str(len(self.input_files['IO'].index)) + ' lines'

        if len(self.input_files['CO'].index) < 14:
            invalid_dim_files['CO'] = 'The Company Code dimension file contains only ' + str(len(self.input_files['CO'].index)) + ' lines'

        if len(self.input_files['PC'].index) < 535:
            invalid_dim_files['PC'] = 'The Profit Center dimension file contains only ' + str(len(self.input_files['PC'].index)) + ' lines'

        if len(self.input_files['ET'].index) < 11:
            invalid_dim_files['ET'] = 'The Equipment Type dimension file contains only ' + str(len(self.input_files['ET'].index)) + ' lines'

        if len(self.input_files['SCEN'].index) < 29:
            invalid_dim_files['SCEN'] = 'The Scenario dimension file contains only ' + str(len(self.input_files['SCEN'].index)) + ' lines'

        if len(self.input_files['VER'].index) < 26:
            invalid_dim_files['VER'] = 'The Version dimension file contains only ' + str(len(self.input_files['VER'].index)) + ' lines'

        if len(self.input_files['TYPE'].index) < 8:
            invalid_dim_files['TYPE'] = 'The Type dimension file contains only ' + str(len(self.input_files['TYPE'].index)) + ' lines'

        if len(self.input_files['YEAR'].index) < 29:
            invalid_dim_files['YEAR'] = 'The Year dimension file contains only ' + str(len(self.input_files['YEAR'].index)) + ' lines'

        if len(self.input_files['PERIOD'].index) < 112:
            invalid_dim_files['PERIOD'] = 'The Period dimension file contains only ' + str(len(self.input_files['PERIOD'].index)) + ' lines'


        # Create an error file if any dimension files are incomplete
        if invalid_dim_files:

            # Set error email info
            error_email_info = {}
            error_email_info['error_email_subject'] = 'WARNING: Load process failed - see attachment for details'
            error_email_info['error_email_filepath'] = r'\\disk23\fin_plan-shared\Automation-FPA\Load_Files\Validation_Errors\Validation_Errors_' + self.enhanced_file_name
            error_email_info['error_email_body'] = 'The load failed because at least one dimension validation file is missing or incomplete. See the attachment for details.  NOTE: No data on your sheet has been loaded.'

            error_log_entry = "Validation failed because one or more dimension files are either missing or incomplete. For details see Validation_Errors_" + self.enhanced_file_name + r" at \\disk23\fin_plan-shared\Automation-FPA\Load_Files\Validation_Errors"

            invalid_dim_files_df = pd.DataFrame.from_dict(invalid_dim_files, orient='index', columns=['Details'])
            ret = self.create_error_file(invalid_dim_files_df, error_email_info, error_log_entry)
            print('At least one dimension file is incomplete')
            return False
        else:
            logging.info("Dimension files validated.")
            print("Dimension files validated.")
            return True

        
    
    def get_time_labels(self):
        
        print("Running get_time_labels...")
    
        if self.df.iloc[1,0].startswith('ET:'):
            # Put the month headers into a new dataframe (exclude the FileName label at the end of the headers)
            months_labels = self.df.columns[10:-1].to_frame().dropna().reset_index()
            months_labels.rename(columns={months_labels.columns[1]:'PERIOD'}, inplace=True)

            #return the new dataframe
            return months_labels
        else:
            # Capture the first row of the dataframe (not the headers) and convert it into a new dataframe
            # The year labels are the index
            # Month labels are in the first column; add a new column for the year labels (use the index to populate it)
            months_years_labels = self.df.iloc[0,:].to_frame().dropna().reset_index()
            months_years_labels.rename(columns={months_years_labels.columns[1]:'PERIOD'}, inplace=True)
            months_years_labels['YEAR'] = months_years_labels['index']
            cond1 = months_years_labels['PERIOD'] != 0
            cond2 = months_years_labels['PERIOD'] != ''
            cond3 = months_years_labels['YEAR'].str.upper().isin(['FILENAME','USEREMAIL'])
            months_years_labels = months_years_labels[cond1 & cond2 & ~ cond3]

            # Drop the _## suffixes from the year labels
            months_years_labels['YEAR'] = months_years_labels['YEAR'].str.replace(r'_.*','', regex=True)

            #return the new dataframe
            return months_years_labels

    
    
    def create_capacity_flag_file(self, df, version, load_flag_value):
        
        print("Running create_capacity_flag_file...")
    
        df = df.join(pd.DataFrame(
        {
            'ACCT':'CL:09962',
            'CC':'CC:None',
            'IO':'IO:None',
            'CO': 'CO:9001',
            'PC':'PC:1000',
            'ET':'ET:None',
            'SCEN':'Forecast',
            'VER':version,
            'TYPE':'Amount',
            'DATA':load_flag_value,
            'FileName':'_' + str(current_datetime)
        }, index = df.index
        ))

        # Reorder the columns to match the load rule
        df = df[['ACCT','CC','IO','CO','PC','ET','SCEN','VER','TYPE','YEAR','PERIOD','DATA','FileName']]

        return df


    
    def process_backup_file(self, load_sheet_members):
    
        print('Running process_backup_file...')

        # Create the column headers
        self.finstmt_backup.columns = ['ET','PC','CO','TYPE','IO','CC','YEAR','VER','SCEN','ACCT','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','FileName']

        print('Backup file size before processing:')
        print(self.finstmt_backup.shape)
        print(self.finstmt_backup.info())

        # Create a dataframe from the month headers
        periods = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        df_period = pd.DataFrame (periods, columns = ['PERIOD'])

        # Limit the data to the members on the load sheet
        cond1 = self.finstmt_backup['ACCT'].isin(load_sheet_members['acct'].tolist())
        cond2 = self.finstmt_backup['CC'].isin(load_sheet_members['cc'].tolist())
        cond3 = self.finstmt_backup['IO'].isin(load_sheet_members['io'].tolist())
        cond4 = self.finstmt_backup['CO'].isin(load_sheet_members['co'].tolist())
        cond5 = self.finstmt_backup['PC'].isin(load_sheet_members['pc'].tolist())
        cond6 = self.finstmt_backup['ET'].isin(load_sheet_members['et'].tolist())
        cond7 = self.finstmt_backup['SCEN'].isin(load_sheet_members['scen'].tolist())
        cond8 = self.finstmt_backup['VER'].isin(load_sheet_members['ver'].tolist())
        cond9 = self.finstmt_backup['TYPE'].isin(load_sheet_members['type'].tolist())
        cond10 = self.finstmt_backup['YEAR'].isin(load_sheet_members['year'].tolist())

        self.finstmt_backup = self.finstmt_backup[cond1 & cond2 & cond3 & cond4 & cond5 & cond6 & cond7 & cond8 & cond9 & cond10]

        print('Backup file size after filtering:')
        print(self.finstmt_backup.shape)
        print(self.finstmt_backup.info())

        # Melt all of the year_month headers into a new column named Combo_Period
        # Intentionally omit the "value_vars" parameter here, which causes all of the remaining columns to be namelessly melted
        # This is perfect because you never know how many months columns there will be, or which year(s) are being loaded
        self.finstmt_backup = pd.melt(self.finstmt_backup, id_vars=['ACCT','CC','IO','CO','PC','ET','SCEN','VER','TYPE','YEAR','FileName'], var_name='PERIOD',value_name='DATA')

        # Reorder the columns
        self.finstmt_backup = self.finstmt_backup[['ACCT','CC','IO','CO','PC','ET','SCEN','VER','TYPE','YEAR','PERIOD','DATA','FileName']]

        print('Backup file size after melting:')
        print(self.finstmt_backup.shape)
        print(self.finstmt_backup.info())

        return self.finstmt_backup

    
    
    def process_date_month_labels(self, column_header):
        
        print('Running process_date_month_labels...')

        print(type(column_header))
        print('Incoming header column:')
        print(column_header)

        match_pattern = r'(FY)\s\d\d(\d\d_\w\w\w)'

        z = re.match(match_pattern, column_header)
        if not z == None:
            print('group 0: ' + ''.join(z.groups(0)))
            repl = ''.join(z.groups(0))  # Convert the tuple to a string
            print('Replacement string: ' + repl)
            column_header = re.sub(r'FY .*', repl, column_header)
            return column_header
        else:
            return column_header
    
    

    def add_email_columns(self, df, user_email):
        
        print('Running add_email_columns...')
        
        # Note: This function is called by the 'create_error_file' function only
        # The email field in the dataframe for the load sheet was created by Alteryx when the sheet was imported
    
        # Create new columns in the dataframe for email addresses (one column for the primary recipient, and one for the cc:)
        # If the user's eID was found in the name of the load file, add the user's email address as the primary recipient
        # If the user's eID was not found, make me the primary recipient
        # If the user is the primary recipient, put my address in the cc column; if I'm the primary, leave the cc: column empty

        if not user_email is None:
            if not 'UserEmail' in df.columns:
                df['UserEmail'] = user_email
            df['ccEmail'] = 'e79230@wnco.com'
        else:
            if not 'UserEmail' in df.columns:
                df['UserEmail'] = 'e79230@wnco.com'
            df['ccEmail'] = None

        return df   
    


    def cleanup_load_sheet(self):
    
        print("Running cleanup_load_sheet...")

        #user_id = self.user_id
        #workbook_name = self.workbook_name
        #load_sheet_name = self.load_sheet_name
        
        print("Before cleanup:")
        print(self.df.head())

        # Drop any empty rows and columns; this was retested on 4/26/22
        # NOTE: The FileName and UserEmail columns will always be auto-populated by Alteryx when it creates the datafame
        print('df shape before dropna rows and columns: ' + str(self.df.shape))
        self.df = self.df.dropna(axis=0, thresh=3) # Drop all rows having fewer than 3 *non*-NA values (two being FileName and UserEmail)
        self.df = self.df.dropna(axis=1, how='all') # Drop all columns that are entirely empty
        print('df shape after dropna rows and columns: ' + str(self.df.shape))

        ##################################################################################################################
        # Fill all empty cells if AND ONLY IF the initial F1 - F9 and FY* column headers are correct
        # If the headers are incorrect, leave the sheet alone and let it fail the validation process
        #if list(pd.Series(df.columns))[8] == 'F9' and list(pd.Series(df.columns))[9] != 'F10':
        print('Year column headers:')
        print(list(pd.Series(self.df.columns)))
        if list(pd.Series(self.df.columns))[0] == 'F1' and list(pd.Series(self.df.columns))[8] == 'F9' and list(pd.Series(self.df.columns))[9].startswith('FY'):
            print('Writing empty strings in empty header cells...')
            replacement_values = {'F1':'','F2':'','F3':'','F4':'','F5':'','F6':'','F7':'','F8':'','F9':''}
            self.df = self.df.fillna(value=replacement_values)  # Fill the empty member cells (Columns 1 through 9) with empty strings
            self.df = self.df.fillna(0) # Only numeric cells (Column 10 and beyond) will be empty at this point; fill them with zeroes
            print('Dataframe after filling empty cells:')
            print(self.df.head())
            print('')
        ##################################################################################################################

        # Verify the user entered their eID; if not, use mine
        print('Incoming user email: ' + self.df.loc[1,'UserEmail'])
        if len(self.df.loc[1,'UserEmail']) == 9:
            self.df['UserEmail'] = 'e79230@wnco.com'
            logging.warning("UserEmail was FORCED to e79230@wnco.com")
            print('WARNING: UserEmail was FORCED to e79230@wnco.com')
        
        # Convert the year and month headers to be member names, not aliases
        # This is done to ensure proper index matching with the CORPPLN_Forecast_CY (FIN_STMT) backup file
        x = range(len(self.df.columns))
        for n in x:
            year_label = re.search(r'\s{0,}FY\s{0,}((?:\d{4}|\d{2}))\s{0,}(_?\d{0,})$', self.df.columns[n])
            if not year_label is None:
                self.df.rename(columns={self.df.columns[n]:'FY' + str(year_label.group(1)[-2:])}, inplace=True)
                # If the year label had a suffix, reappend it (because it makes the label unique in the index)
                if not len(year_label.group(2)) == 0:
                    self.df.rename(columns={self.df.columns[n]:self.df.columns[n] + str(year_label.group(2))}, inplace=True)
                else:
                    self.df.rename(columns={self.df.columns[n]:self.df.columns[n] + '_1'}, inplace=True)
                
                month_label = re.search(r'(?:^|(?<= ))(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:(?= )|$)', self.df.iloc[0,n])
                if not month_label is None:
                    self.df.iloc[0,n] = month_label.group(1)[:3]
                else:
                    print('No month label match on ' + self.df.iloc[0,n])
            else:
                print('No year label match on ' + self.df.columns[n])

        print("After cleanup:")
        print(self.df.head())

        return True

    
    
    def validation_and_cleanup(self):
        
        print('Running validation_and_cleanup...')
        
        if self.validate_loadfile_name() == False:
            return False

        if self.validate_dimension_files() == False:
            return False

        if self.cleanup_load_sheet() == False:
            return False

        

    def process_load_sheet(self):
    
        print('Running process_load_sheet...')

        #this_function_name = sys._getframe(  ).f_code.co_name  <=== This works, but there's currently no need for it

        print("Before processing:")
        print(self.df.head())
        

        if self.preliminary_validation() == False:
            return False

        # Check the load sheet for duplicate rows
        # Note: If duplicate rows are found, a partial load file will NOT be created
        if self.duplicate_rows() == True:
            return False

        # Check the load sheet for invalid members
        validation_result = self.validate_dimensions() # if validation_result is a df, the validation was successful
        if isinstance(validation_result, pd.DataFrame):
            self.df = validation_result
        else:
            return False

        # Create the load file(s)
        # Note: Loading Current Capacity creates a second text load file, which contains flag values 
        if self.create_load_file() == True:
            return True
        else:
            return False

    
    
    def preliminary_validation(self):
        
        # This is a *high-level* eye test of whether the load sheet generally follows the layout/format requirements
        # The purpose of this validation is to catch any obvious problems that need to be fixed by the sheet's creator
        # Even if this validation is successful, additional validations that are more thorough will be done in subsequent steps

        invalid_dims = {}

        print('Running preliminary_validation...')
        print('')
        print('Preliminary dataframe:')
        print(self.df.head())
        
        # The first nine cells on the month labels row should contain empty strings (written there after the df was created)
        z = str(list(self.df.iloc[0,0:9]).count(''))
        print(f'The number of empty strings before the first column header is {z}')

        z = str(self.df.iloc[0,0:9].isna().sum())
        print(f'The number of na cells before the first column header is {z}')
        
        # Get the count of columns containing forecast values
        # Next, we'll make sure the number of year and month labels matches this count
        # Note: The first 9 columns are the non-time-based dimensions; the last 2 are filename and user email
        forecast_columns = self.df.shape[1] - 9 - 2  
        
        
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # WARNING: When evaluating the df values, empty cells and those containing zeroes will always be evaluated as TRUE
        #          by Python's 'any' and 'all' functions.
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        # Check whether any values in Column 1 resemble account members or aliases
        if not any(self.df.iloc[:,0].str.contains(r'\D\D:\d{4,7}|.*\([HFS]?\d{4,7}\)$', regex=True)):
            invalid_dims['Account'] = 'The Account dimension has at least one invalid and/or missing member in Column A'

        # Check whether any values in Column 2 resemble cost center members or aliases
        if not any(self.df.iloc[:,1].str.contains(r'CC:\d{5}|.*\(\d{5}\)$', regex=True)):
            invalid_dims['Cost Center'] = 'The Cost Center dimension has at least one invalid and/or missing member in Column B'

        # Check whether any values in Column 3 resemble internal order members or aliases
        if not any(self.df.iloc[:,2].str.contains(r'IO:\d{6}|.*\(\d{6}\)$|IO:None', regex=True)):
            invalid_dims['Internal Order'] = 'The Internal Order dimension has at least one invalid and/or missing member in Column C'

        # Check whether any values in Column 4 resemble company code members or aliases
        if not any(self.df.iloc[:,3].str.contains(r'CO:\d{4}|.*\(\d{4}\)$', regex=True)):
            invalid_dims['Company Code'] = 'The Company Code dimension has at least one invalid and/or missing member in Column D'

        # Check whether any values in Column 5 resemble profit center members or aliases
        if not any(self.df.iloc[:,4].str.contains(r'PC:\d{4}|.*\(\d{4}\)$', regex=True)):
            invalid_dims['Profit Center'] = 'The Profit Center dimension has at least one invalid and/or missing member in Column E'

        # Check whether any values in Column 6 resemble equipment type members or aliases
        if not any(self.df.iloc[:,5].str.contains(r'ET:\d{3}|.*\(\d{3}M?X?\)$|ET:None', regex=True)):
            invalid_dims['Equipment Type'] = 'The Equipment Type dimension has at least one invalid and/or missing member in Column F'

        # Check whether Column 7 contains either Forecast or Actual (Actual is for ExTO loads only)
        if not any(self.df.iloc[:,6].isin(['Forecast','Actual'])):
            invalid_dims['Scenario'] = 'The Scenario dimension has at least one invalid and/or missing member in Column G'

        # Check whether Column 8 contains either Working or Locked
        if not any(self.df.iloc[:,7].isin(['Working','Locked','Final','Current Capacity'])):
            invalid_dims['Version'] = 'The Version dimension has at least one invalid and/or missing member in Column H'

        # Check whether Column 9 contains either Amount or Adjustment
        if not any(self.df.iloc[:,8].isin(['Amount','Adjustment'])):
            invalid_dims['Type'] = 'The Type dimension has at least one invalid and/or missing member in Column I'
        
        # Check the critical cells on the year header row, and get the count of columns containing a year header (member name or alias)
        # A column labeled as "Total" will cause a failure here
        if list(pd.Series(self.df.columns))[0] != 'F1' or \
        list(pd.Series(self.df.columns))[8] != 'F9' or \
        not list(pd.Series(self.df.columns))[9].startswith('FY') or \
        list(pd.Series(self.df.columns))[-1] != 'UserEmail' or \
        list(pd.Series(self.df.columns))[-2] != 'FileName':
            invalid_dims['Year'] = 'The Year dimension has at least one invalid and/or missing member in Row 1.'

        # Check the month headers row; the first nine cells must contain an empty string
        # Any header other than a month name will cause the validation to fail; a "Total" label will cause a failure
        if list(self.df.iloc[0,0:9]).count('') != 9 or \
        not self.df.iloc[0,9].startswith(('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')):
            invalid_dims['Month'] = 'The Month dimension has at least one invalid and/or missing member in Row 2.'

        
        # Check whether any dimensions have been flagged as invalid
        if not invalid_dims:
            # All of the dimensions are in the correct position on the load sheet
            # Now check for other issues...  
            
            # NOTE: In these validations, the Forecast columns are collectively treated as a "dimension"
            
            # 1. Check for missing column headers in the Forecast values region (which begins in Column 10 on a properly formatted sheet):
            # We're looking for any Year column header that follows the pattern "F##" and/or a Month header of 0 (*not* an empty cell)
            # Example: Year headers "F1" through "F9" are ok (at this point in the process); "F10", "F11", "F12" etc. are NOT ok
            if any(pd.Series(self.df.columns).str.contains(r'F\d{2}', regex=True)) or any(pd.Series(self.df.iloc[0]) == 0):
                invalid_dims['ForecastColumns'] = 'Forecast values were found in one or more columns that do not have column headers.'        
            
            # 2. Check for non-numeric characters in the Forecast values region:
            #    Create a new dataframe for the load sheet and force all cells in the Forecast region to be numeric
            #    Any cells containing non-numeric characters will be converted by pandas to the value 'NaN' ('Not a Number')
            #    Count the resulting 'NaN' cells in the new dataframe; if greater than zero, we will notify the user
            #    Note: After the number of 'NaN' cells is captured, this new 'df2' dataframe will not be used or referenced again
            df2 = self.df.iloc[1:,9:self.df.shape[1]-2]  # Limit the new dataframe to the numeric region of the load sheet
            df2 = df2.apply(lambda s: pd.to_numeric(s, errors='coerce'))
            df2_nulls = df2.isnull().sum().sum()  # 'NaN' is considered a null value
            if df2_nulls != 0:
                invalid_dims['ForecastColumns'] = df2_nulls
                if df2_nulls > 1:
                    invalid_dims['ForecastColumns'] = 'A non-numeric character was found in ' + str(df2_nulls) + ' cells in the Forecast values region of the sheet'
                else:
                    invalid_dims['ForecastColumns'] = 'A non-numeric character was found in ' + str(df2_nulls) + ' cell in the Forecast values region of the sheet'
              
                
        # Create an error file if any dimensions were flagged
        if invalid_dims:
            # Set error email info
            error_email_info = {}
            error_email_info['error_email_subject'] = 'WARNING: Load process failed - see attachment for details'
            error_email_info['error_email_filepath'] = r'\\disk23\fin_plan-shared\Automation-FPA\Load_Files\Validation_Errors\Validation_Errors_' + self.enhanced_file_name
            error_email_info['error_email_body'] = 'The load process failed. Please see the attachment for details. \n\n NOTE: No data on your sheet has been loaded.'

            error_log_entry = "The preliminary validation failed. For details see Validation_Errors_" + self.enhanced_file_name + r" at \\disk23\fin_plan-shared\Automation-FPA\Load_Files\Validation_Errors"

            invalid_dims_df = pd.DataFrame.from_dict(invalid_dims, orient='index', columns=['Details'])
            ret = self.create_error_file(invalid_dims_df, error_email_info, error_log_entry)
            return False
        else:
            logging.info("Worksheet meets layout rules.")
            print("Worksheet meets layout rules.")
            return True      

        
        
    def duplicate_rows(self):
    
        print('Running duplicate_rows...')
        
        # Determine if there are any duplicate rows (based on combining the values in the first 9 columns as a key)
        cond1 = self.df.duplicated(subset=['F1','F2','F3','F4','F5','F6','F7','F8','F9'], keep=False)
        duplicate_rows = self.df.copy()[cond1]  # Create a COPY of the df to prevent the SettingWithCopyWarning issue

        # Add a column for row number and move it to the first position
        duplicate_rows['RowNumber'] = duplicate_rows.index + 2
        rn = duplicate_rows.pop('RowNumber') # remove column and store it
        duplicate_rows.insert(0, 'RowNumber', rn)

        # Sort
        duplicate_rows = duplicate_rows.sort_values(by=['F1','F2','F3','F4','F5','F6','F7','F8','F9','RowNumber'])

        print('Duplicate rows df:')
        print(duplicate_rows.head())

        # If any duplicates were found, create a file containing all of the duplicate rows for the user to fix
        # Do NOT create a load file
        if len(duplicate_rows.index) > 0:

            # Set error email info
            error_email_info = {}
            error_email_info['error_email_subject'] = 'WARNING: Load process failed - duplicate rows found on the load sheet'
            error_email_info['error_email_filepath'] = r'\\disk23\fin_plan-shared\Automation-FPA\Load_Files\Validation_Errors\Validation_Errors_' + self.enhanced_file_name
            error_email_info['error_email_body'] = """Duplicate account/cost center/internal order combinations were found on the load sheet.  
            The first column in the attachment shows the row numbers on the sheet where the duplicates were found.  
            Please delete the appropriate rows from the sheet and repeat the load process.
            NOTE: No data on your sheet has been loaded."""

            error_log_entry = "Validation failed because of duplicate rows on the load sheet. For details see Validation_Errors_" + self.enhanced_file_name + r" at \\disk23\fin_plan-shared\Automation-FPA\Load_Files\Validation_Errors"

            ret = self.create_error_file(duplicate_rows, error_email_info, error_log_entry)
            return True
        else:
            logging.info("No duplicate rows found.")
            print("No duplicate rows found.")
            return False
    
    
    
    def validate_dimensions(self):
    
        print('Running validate_dimensions...')

        month_labels = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC',  \
        'JANUARY','FEBRUARY','MARCH','APRIL','MAY','JUNE','JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER']

        # Get the year and month labels from the headers
        # NOTE: For ExTO load files, get the month labels only
        time_labels = self.get_time_labels()

        # For all load files other than ExTO, get the year and month labels and validate them; 
        # All other dimensions are validated below
        if self.df.iloc[1,0].startswith('ET:'):
            # Validate the Period members
            # The Year members will be validated below (in ExTO load files they're in the rows rather than the headers)
            PERIOD_validated = self.validate_members(time_labels['PERIOD'],'Period')
        else:
            # Validate the Year and Period members
            YEAR_validated = self.validate_members(time_labels['YEAR'],'Years')
            PERIOD_validated = self.validate_members(time_labels['PERIOD'],'Period')

        x = range(len(self.df.columns))
        for n in x:
            if self.df.columns[n].upper() in (month_labels) or  self.df.columns[n].upper() in ['FILENAME', 'USEREMAIL']:
                pass # Leave the column header as-is
            # On all of these evaluations, skip Row 0 because it contains either None or the month labels
            elif any(self.df.iloc[1:,n].isin(self.input_files['ET']['Member Name'].tolist())) or any(self.df.iloc[1:,n].isin(self.input_files['ET']['Alias: Default'].tolist())):
                self.df.rename(columns={self.df.columns[n]:'ET'}, inplace=True)
                ET_validated = self.validate_members(self.df['ET'][self.df['ET']!=''],'Equipment Type') #Filter out the rows containing zeros
                if ET_validated.empty == True:
                    if self.df.iloc[1,n].find(':') != 2:
                        # The dimension contains aliases on the sheet, so add a new column for member names
                        ET_MemberNames = self.get_member_names(self.df['ET'],'Equipment Type')
                        self.df = self.df.merge(ET_MemberNames, how='left', left_index=True, right_index=True, suffixes=(None, '_y'))
                        self.df = self.df.drop(columns=['ET_y','ET_Alias'])
                        self.df = self.df.rename(columns={'ET':'ET_Alias','ET_MemberName':'ET'})
            elif any(self.df.iloc[1:,n].isin(self.input_files['PC']['Member Name'].tolist())) or any(self.df.iloc[1:,n].isin(self.input_files['PC']['Alias: Default'].tolist())):
                self.df.rename(columns={self.df.columns[n]:'PC'}, inplace=True)
                PC_validated = self.validate_members(self.df['PC'][self.df['PC']!=''],'Profit Center') #Filter out the rows containing zeros
                if PC_validated.empty == True:
                    if self.df.iloc[1,n].find(':') != 2:
                        # The dimension contains aliases on the sheet, so add a new column for member names
                        PC_MemberNames = self.get_member_names(self.df['PC'],'Profit Center')
                        self.df = self.df.merge(PC_MemberNames, how='left', left_index=True, right_index=True, suffixes=(None, '_y'))
                        self.df = self.df.drop(columns=['PC_y','PC_Alias'])
                        self.df = self.df.rename(columns={'PC':'PC_Alias','PC_MemberName':'PC'})
            elif any(self.df.iloc[1:,n].isin(self.input_files['CO']['Member Name'].tolist())) or any(self.df.iloc[1:,n].isin(self.input_files['CO']['Alias: Default'].tolist())):
                self.df.rename(columns={self.df.columns[n]:'CO'}, inplace=True)
                CO_validated = self.validate_members(self.df['CO'][self.df['CO']!=''],'Company Code') #Filter out the rows containing zeros
                if CO_validated.empty == True:
                    if self.df.iloc[1,n].find(':') != 2:
                        # The dimension contains aliases on the sheet, so add a new column for member names
                        CO_MemberNames = self.get_member_names(self.df['CO'],'Company Code')
                        self.df = self.df.merge(CO_MemberNames, how='left', left_index=True, right_index=True, suffixes=(None, '_y'))
                        self.df = self.df.drop(columns=['CO_y','CO_Alias'])
                        self.df = self.df.rename(columns={'CO':'CO_Alias','CO_MemberName':'CO'})
            elif any(self.df.iloc[1:,n].isin(self.input_files['IO']['Member Name'].tolist())) or any(self.df.iloc[1:,n].isin(self.input_files['IO']['Alias: Default'].tolist())):
                self.df.rename(columns={self.df.columns[n]:'IO'}, inplace=True)
                IO_validated = self.validate_members(self.df['IO'][self.df['IO']!=''],'Internal Order') #Filter out the rows containing zeros
                if IO_validated.empty == True:
                    if self.df.iloc[1,n].find(':') != 2:
                        # The dimension contains aliases on the sheet, so add a new column for member names
                        IO_MemberNames = self.get_member_names(self.df['IO'],'Internal Order')
                        self.df = self.df.merge(IO_MemberNames, how='left', left_index=True, right_index=True, suffixes=(None, '_y'))
                        self.df = self.df.drop(columns=['IO_y','IO_Alias'])
                        self.df = self.df.rename(columns={'IO':'IO_Alias','IO_MemberName':'IO'})
            elif any(self.df.iloc[1:,n].isin(self.input_files['CC']['Member Name'].tolist())) or any(self.df.iloc[1:,n].isin(self.input_files['CC']['Alias: Default'].tolist())):
                self.df.rename(columns={self.df.columns[n]:'CC'}, inplace=True)
                CC_validated = self.validate_members(self.df['CC'][self.df['CC']!=''],'Cost Center') #Filter out the rows containing zeros
                if CC_validated.empty == True:
                    if self.df.iloc[1,n].find(':') != 2:
                        # The dimension contains aliases on the sheet, so add a new column for member names
                        CC_MemberNames = self.get_member_names(self.df['CC'],'Cost Center')
                        self.df = self.df.merge(CC_MemberNames, how='left', left_index=True, right_index=True, suffixes=(None, '_y'))
                        self.df = self.df.drop(columns=['CC_y','CC_Alias'])
                        self.df = self.df.rename(columns={'CC':'CC_Alias','CC_MemberName':'CC'})
            elif any(self.df.iloc[1:,n].isin(self.input_files['ACCT']['Member Name'].tolist())) or any(self.df.iloc[1:,n].isin(self.input_files['ACCT']['Alias: Default'].tolist())):
                self.df.rename(columns={self.df.columns[n]:'ACCT'}, inplace=True)
                ACCT_validated = self.validate_members(self.df['ACCT'][self.df['ACCT']!=''],'Account')
                if ACCT_validated.empty == True:
                    if self.df.iloc[1,n].find(':') != 2:
                        # The dimension contains aliases on the sheet, so add a new column for member names
                        ACCT_MemberNames = self.get_member_names(self.df['ACCT'],'Account')
                        self.df = self.df.merge(ACCT_MemberNames, how='left', left_index=True, right_index=True, suffixes=(None, '_y'))
                        self.df = self.df.drop(columns=['ACCT_y','ACCT_Alias'])
                        self.df = self.df.rename(columns={'ACCT':'ACCT_Alias','ACCT_MemberName':'ACCT'})
            elif any(self.df.iloc[1:,n].isin(self.input_files['YEAR']['Member Name'].tolist())) or any(self.df.iloc[1:,n].isin(self.input_files['YEAR']['Alias: Default'].tolist())):
                self.df.rename(columns={self.df.columns[n]:'YEAR'}, inplace=True)
                YEAR_validated = self.validate_members(self.df['YEAR'][self.df['YEAR']!=''],'Years') #Filter out the rows containing zeros
                if YEAR_validated.empty == True:
                    if self.df.iloc[1,n].find(':') != 2:
                        # The dimension contains aliases on the sheet, so add a new column for member names
                        YEAR_MemberNames = self.get_member_names(self.df['YEAR'],'Years')
                        self.df = self.df.merge(YEAR_MemberNames, how='left', left_index=True, right_index=True, suffixes=(None, '_y'))
                        self.df = self.df.drop(columns=['YEAR_y','YEAR_Alias'])
                        self.df = self.df.rename(columns={'YEAR':'YEAR_Alias','YEAR_MemberName':'YEAR'})
            elif any(self.df.iloc[1:,n].isin(['Amount','Adjustment','Rate','Units'])):
                self.df.rename(columns={self.df.columns[n]:'TYPE'}, inplace=True)
                TYPE_validated = self.validate_members(self.df['TYPE'][self.df['TYPE']!=''],'Type') #Filter out the rows containing zeros
            elif any(self.df.iloc[1:,n].isin(['Working','Final','Current Capacity'])):
                self.df.rename(columns={self.df.columns[n]:'VER'}, inplace=True)
                VER_validated = self.validate_members(self.df['VER'][self.df['VER']!=''],'Version') #Filter out the rows containing zeros
            elif any(self.df.iloc[1:,n].isin(['Forecast','Actual','Flash_Base','Flash_GAAP','Flash_NonGAAP','Flash_Eco'])):
                self.df.rename(columns={self.df.columns[n]:'SCEN'}, inplace=True)
                SCEN_validated = self.validate_members(self.df['SCEN'][self.df['SCEN']!=''],'Scenario') #Filter out the rows containing zeros
            elif YEAR_validated.empty == True and PERIOD_validated.empty == True:
                if str(self.df.iloc[0,n]).upper() in (month_labels):
                    # WARNING: The placement of this test for month labels *on Row 0* is crucial - it must be here at the bottom
                    # Concatenate the year and month as the new header (ex: FY 2021_Jan)
                    # It will be split after it's melted into the rows
                    year = re.search(r'F?Y?\s?[0-9]{0,4}', self.df.columns[n]).group(0)
                    self.df.rename(columns={self.df.columns[n]:year + '_' + self.df.iloc[0,n]}, inplace=True)
            else:
                pass # Leave the header as-is (even if it appears to be wrong - it will be flagged during validation)

        # For *NON-ExTO* data only, drop the first row of the dataframe (i.e., the second header row in the source file)
        # The ExTO data set is the only one with Equipment Type in the first column (since it was exported directly from FIN_STMT)
        if not self.df.iloc[1,0].startswith('ET:'):
            self.df = self.df.drop([0])

        
        print('Dataframe with new headers:')       
        print(self.df.head())

        # Sort all columns to speed up the check for duplicates
        # This will fail if any of the columns don't contain at least one valid member (and thus the column header will be missing)
        self.df = self.df.sort_values(by=['ACCT','CC','IO','CO','PC','ET','SCEN','VER','TYPE'])


        # If ANY of the dimension validations failed, create a file containing all of the invalid members for the user to fix
        # Do NOT create a load file
        if any([ACCT_validated.empty == False, CC_validated.empty == False, IO_validated.empty == False, \
                CO_validated.empty == False, PC_validated.empty == False, ET_validated.empty == False, \
                SCEN_validated.empty == False, VER_validated.empty == False, TYPE_validated.empty == False, \
                YEAR_validated.empty == False, PERIOD_validated.empty == False]):

            # Concatenate all of the validation dataframes that contain invalid members
            # This way the user will be able to see all invalid members across all dimensions in a single file
            validations = [ACCT_validated, CC_validated, IO_validated, CO_validated, PC_validated, ET_validated, \
                           SCEN_validated, VER_validated, TYPE_validated, YEAR_validated, PERIOD_validated]
            invalid_members = {}
            for df_index, df_dim in enumerate(validations):
                if df_dim.empty == False:
                    invalid_members[df_index] = df_dim  # Create a dictionary of dataframes
                    
            # Set error email info
            error_email_info = {}
            error_email_info['error_email_subject'] = 'WARNING: Load process failed - invalid Essbase members found on the load sheet'
            error_email_info['error_email_filepath'] = r'\\disk23\fin_plan-shared\Automation-FPA\Load_Files\Validation_Errors\Validation_Errors_' + self.enhanced_file_name
            error_email_info['error_email_body'] = """The attached file contains a list of the invalid Essbase members found on the load sheet.
            Invalid members are those that do not have an exact match in SWA_RPT.
            Discrepancies are often caused by capitalization, special characters, and space characters.
            Please correct the load sheet and repeat the load process. \n \n
            NOTE: No data on your sheet has been loaded."""

            error_log_entry = "Validation failed because of invalid members on the load sheet. For details see Validation_Errors_" + self.enhanced_file_name + r" at \\disk23\fin_plan-shared\Automation-FPA\Load_Files\Validation_Errors"
                    
            invalid_members_df = pd.concat(df_dim for df_dim in invalid_members.values())  # Concatenate the dataframes into a single one
            ret = self.create_error_file(invalid_members_df, error_email_info, error_log_entry)
            print('At least one dimension file is incomplete')
            return False

        else:
            # The df was altered during the successful validation process, so send it back to the ProcessSourceFile def
            # The presence of the dataframe *implies* a return value of "True", but we will not return "True" explicitly
            logging.info("All members validated.")
            print("All members validated.")
            return self.df
    
   

    def validate_members(self, s_load_sheet_members, dimension):
        
        print('Running validate_members...')
    
        # Get the dimension's Outline Extractor doc file (these were imported as input files below)
        if dimension == 'Account':
            dim_members = self.input_files['ACCT']
            # RESTRICTION: In the doc file, keep only accounts that store data
            dim_members = dim_members[dim_members['Data Storage'].str.upper().isin(['STORE DATA','NEVER SHARE'])]
            sheet_dim_header = 'ACCT'
        elif dimension == 'Cost Center':
            dim_members = self.input_files['CC']
            sheet_dim_header = 'CC'
        elif dimension == 'Internal Order':
            dim_members = self.input_files['IO']
            sheet_dim_header = 'IO'
        elif dimension == 'Company Code':
            dim_members = self.input_files['CO']
            dim_members = dim_members[dim_members['Member Name'].str.upper().isin(['CO:9001'])]
            sheet_dim_header = 'CO'
        elif dimension == 'Profit Center':
            dim_members = self.input_files['PC']
            dim_members = dim_members[dim_members['Member Name'].str.upper().isin(['PC:1000','HDQ (1000)'])]
            sheet_dim_header = 'PC'
        elif dimension == 'Equipment Type':
            dim_members = self.input_files['ET']
            dim_members = dim_members[dim_members['Member Name'].str.upper().isin(['ET:NONE'])]
            sheet_dim_header = 'ET'
        elif dimension == 'Scenario':
            dim_members = self.input_files['SCEN']
            dim_members = dim_members[dim_members['Member Name'].str.upper().isin(['ACTUAL','FORECAST','FLASH_BASE'])]
            sheet_dim_header = 'SCEN'
        elif dimension == 'Version':
            dim_members = self.input_files['VER']
            dim_members = dim_members[dim_members['Member Name'].str.upper().isin(['FINAL','WORKING','CURRENT CAPACITY','CURRENT CAPACITY2'])]
            sheet_dim_header = 'VER'
        elif dimension == 'Type':
            dim_members = self.input_files['TYPE']
            dim_members = dim_members[dim_members['Member Name'].str.upper().isin(['AMOUNT','ADJUSTMENT'])]
            sheet_dim_header = 'TYPE'
        elif dimension == 'Years':
            # RESTRICTION: Keep current year and future years only
            dim_members = self.input_files['YEAR']
            # Split the alias on the space character to create two new columns
            dim_members[['FY Prefix','Year Number']] = dim_members['Alias: Default'].str.split(expand=True)
            # Convert the new Year Number column from string to numeric
            dim_members['Year Number'] = pd.to_numeric(dim_members['Year Number'])
            # Keep only the years that are equal to or greater than the current year
            dim_members = dim_members[dim_members['Year Number'] >= now.year - 1] 
            sheet_dim_header = 'YEAR'
        elif dimension == 'Period': 
            dim_members = self.input_files['PERIOD']
            sheet_dim_header = 'PERIOD'

        # In all doc files/dimensions, keep only the level-zero members
        dim_members = dim_members[dim_members['Level'] == '0']

        f_load_sheet_members = s_load_sheet_members.to_frame()

        # This command: print(f_load_sheet.iloc[0,0].contains('FY[2-9][0-9]',regex=True))
        # Creates this error: "AttributeError: 'str' object has no attribute 'contains' ""
        # NOTE REGARDING THE ERROR: 
        # The str method/object is for a pandas Series BUT NOT FOR A SINGLE ELEMENT OF A SERIES (even though that element is a string)
        # By subscripting a series with [0] you are getting an element of the series. 

        f_dim_members = dim_members[['Member Name','Alias: Default']]

        # Look at the first member on the load sheet to see if it's a member name or an alias
        if any(f_load_sheet_members[sheet_dim_header].isin(['Forecast','Working','Locked','Current Capacity','Amount','Adjustment'])):
            dim_members_join_field = 'Member Name'
        elif any(f_load_sheet_members[sheet_dim_header].str.find(':') == 2): # This catches GL:*, CC:*, etc.
            dim_members_join_field = 'Member Name'
        elif any(f_load_sheet_members[sheet_dim_header].isin(['Jan','Feb','Mar','Apr','Jun','Jul','Aug','Sep','Oct','Nov','Dec'])):
            # Note: May was intentionally omitted from the list since it doesn't have an alias defined in the dim_members file
            dim_members_join_field = 'Member Name'
        elif any(f_load_sheet_members[sheet_dim_header].str.contains('FY[2-9][0-9]',regex=True)):
            dim_members_join_field = 'Member Name'
        else:
            dim_members_join_field = 'Alias: Default'

        # Join the list of members from the load sheet to the members in the dim file
        f_load_sheet_members = f_load_sheet_members.merge(f_dim_members, how='left', left_on=sheet_dim_header, right_on=dim_members_join_field)

        # Rows where the member name AND alias are both NaN indicate invalid members on the load sheet
        cond1 = f_load_sheet_members['Member Name'].isnull()
        cond2 = f_load_sheet_members['Alias: Default'].isnull()
        cond3 = f_load_sheet_members[sheet_dim_header].str.upper().isin(['MAY']) # Filter out the anomalies
        invalid_members = f_load_sheet_members[(cond1 & cond2) & ~ cond3]

        # Drop the Member Name and Alias columns, and all duplicate invalid members
        # Create a standard column name for all dimensions
        invalid_members = invalid_members.drop(columns=['Member Name','Alias: Default']).drop_duplicates()   
        invalid_members.columns = ['Invalid Members']
        invalid_members = invalid_members.sort_values(by=['Invalid Members'])

        return invalid_members

    
    
    def get_member_names(self, s_load_file_members, dimension):
        
        print('Running get_member_names...')
    
        # Get the dimension's Outline Extractor doc file (these were imported as input files below)
        if dimension == 'Account':
            dim_members = self.input_files['ACCT']
            # RESTRICTION: In the doc file, keep only accounts that store data
            dim_members = dim_members[dim_members['Data Storage'].str.upper().isin(['STORE DATA','NEVER SHARE'])]
            backup_file_dim_header = 'ACCT'
        elif dimension == 'Cost Center':
            dim_members = self.input_files['CC']
            backup_file_dim_header = 'CC'
        elif dimension == 'Internal Order':
            dim_members = self.input_files['IO']
            backup_file_dim_header = 'IO'
        elif dimension == 'Company Code':
            dim_members = self.input_files['CO']
            dim_members = dim_members[dim_members['Member Name'].str.upper().isin(['CO:9001'])]
            backup_file_dim_header = 'CO'
        elif dimension == 'Profit Center':
            dim_members = self.input_files['PC']
            dim_members = dim_members[dim_members['Member Name'].str.upper().isin(['PC:1000','HDQ (1000)'])]
            backup_file_dim_header = 'PC'
        elif dimension == 'Equipment Type':
            dim_members = self.input_files['ET']
            dim_members = dim_members[dim_members['Member Name'].str.upper().isin(['ET:NONE'])]
            backup_file_dim_header = 'ET'
        elif dimension == 'Scenario':
            dim_members = self.input_files['SCEN']
            dim_members = dim_members[dim_members['Member Name'].str.upper().isin(['ACTUAL','FORECAST','FLASH_BASE'])]
            backup_file_dim_header = 'SCEN'
        elif dimension == 'Version':
            dim_members = self.input_files['VER']
            dim_members = dim_members[dim_members['Member Name'].str.upper().isin(['FINAL','WORKING','CURRENT CAPACITY','CURRENT CAPACITY2'])]
            backup_file_dim_header = 'VER'
        elif dimension == 'Type':
            dim_members = self.input_files['TYPE']
            dim_members = dim_members[dim_members['Member Name'].str.upper().isin(['AMOUNT','ADJUSTMENT'])]
            backup_file_dim_header = 'TYPE'
        elif dimension == 'Years':
            # RESTRICTION: Keep current year and future years only
            dim_members = self.input_files['YEAR']
            # Split the alias on the space character to create two new columns
            dim_members[['FY Prefix','Year Number']] = dim_members['Alias: Default'].str.split(expand=True)
            # Convert the new Year Number column from string to numeric
            dim_members['Year Number'] = pd.to_numeric(dim_members['Year Number'])
            # Keep only the years that are equal to or greater than the current year
            dim_members = dim_members[dim_members['Year Number'] >= now.year - 1] 
            backup_file_dim_header = 'YEAR'
        elif dimension == 'Period': 
            dim_members = self.input_files['PERIOD']
            backup_file_dim_header = 'PERIOD'

        # In all doc files/dimensions, keep only the level-zero members
        dim_members = dim_members[dim_members['Level'] == '0']

        f_load_file_members = s_load_file_members.to_frame()

        f_dim_members = dim_members[['Member Name','Alias: Default']]
        f_dim_members = f_dim_members.drop_duplicates(subset=['Member Name'])

        # Join the list of members from the backup file to the members in the dim file
        f_load_file_members = f_load_file_members.merge(f_dim_members, how='left', left_on=backup_file_dim_header, right_on='Alias: Default')

        # Rename the alias field
        f_load_file_members = f_load_file_members.rename(columns={'Member Name':backup_file_dim_header + '_MemberName','Alias: Default': backup_file_dim_header + '_Alias'})

        return f_load_file_members
    
    
    
    def create_load_file(self):
    
        # This function produces one or more text files that will be loaded into FIN_STMT

        print('Running create_load_file...')
        
        # Test the handling of a runtime error outside of a try/except block
        #z = 1/0

        # Continue with the steps to create the load file...
        # The following line was updated in v5.0.4; previously it was: if self.df.iloc[1,0].startswith('ET:'):
        if self.df.iloc[0,0].startswith('ET:'):
            # This is the ExTO adjustments data (it is the only source file with Equipment Type in the first column)
            # NOTE: Unlike the load files coming from users, this one already has Year in the rows

            # Melt the months into the rows and then reorder the columns
            self.df = pd.melt(self.df, id_vars=['ACCT','CC','IO','CO','PC','ET','SCEN','VER','TYPE','YEAR','FileName','UserEmail'], var_name='PERIOD',value_name='DATA')
            self.df = self.df[['ACCT','CC','IO','CO','PC','ET','SCEN','VER','TYPE','YEAR','PERIOD','DATA','FileName','UserEmail']]
            self.df['SCEN'] = 'Actual'  # Replace Flash_Base with Actual in the ExTO file
            self.df['VER'] = 'Final'    # Replace Working with Final in the ExTO file
        else:
            # Melt all of the year_month headers into a new column named Combo_Period
            # Intentionally omit the "value_vars" parameter here, which causes all of the remaining columns to be namelessly melted
            # This is perfect because you never know how many months columns there will be, or which year(s) are being loaded

            print('Load file df before melting:')
            print(self.df.head())

            # Drop all alias columns before melting
            for dim in self.df.columns:
                if dim.endswith('_Alias'):
                    self.df = self.df.drop(columns=[dim])

            self.df = pd.melt(self.df, id_vars=['ACCT','CC','IO','CO','PC','ET','SCEN','VER','TYPE','FileName','UserEmail'], var_name='COMBO_PERIOD',value_name='DATA')

            print('Load file df after melting:')
            print(self.df.head())

            # Split the concatenated year_month labels into separate columns (in a new dataframe)
            month_year = self.df['COMBO_PERIOD'].str.split(pat="_",expand=True)
            month_year = month_year.rename(columns={0:'YEAR',1:'PERIOD'})
            unique_periods = month_year.drop_duplicates()

            # Bring the new Year and Period columns into the original dataframe, then delete the Combo_Period column
            self.df = self.df.merge(month_year,left_index=True,right_index=True,copy=False)
            self.df = self.df.drop(columns=['COMBO_PERIOD'])

            # Reorder the new columns
            self.df = self.df[['ACCT','CC','IO','CO','PC','ET','SCEN','VER','TYPE','YEAR','PERIOD','DATA','FileName','UserEmail']]

            # Fill blank cells in the DATA column with zeroes
            self.df = self.df.fillna(value={'DATA':0})

        # Reorder the new columns
        self.df = self.df[['ACCT','CC','IO','CO','PC','ET','SCEN','VER','TYPE','YEAR','PERIOD','DATA','FileName','UserEmail']]

        # The dataframe already includes a column named 'FileName'
        # Append the file type to the front of the filename and a timestamp to the end
        if all(self.df['VER'].isin(['Current Capacity'])):
            if all(self.df['CC'].isin(['CC:40001','Non Operating (40001)'])):
                self.df['FileName'] = 'CurrentCapacity_Load_FleetOnly_' + self.workbook_name + '_' + str(current_datetime)
            else:
                self.df['FileName'] = 'CurrentCapacity_Load_' + self.workbook_name + '_' + str(current_datetime)
        elif all(self.df['SCEN'].isin(['Actual'])):
            # Actual_Load_ (these are the monthly ExTO adjustments)
            self.df['FileName'] = 'Actual_Load_' + self.workbook_name + '_' + str(current_datetime)
        else:
            # Working_Load_
            self.df['FileName'] = 'Working_Load_' + self.user_id + '_' + self.workbook_name + '_' + self.load_sheet_name + '_' + str(current_datetime)

        # Embed the backup data into the load file (as a new column that will be ignored by the load rule)
        #if not all(self.df['VER'].isin(['Current Capacity'])):  As of 4/26/22, capacity load files will have the new column too
        print('Load file df before adding backup data:')
        print(self.df.head())

        # Drop the duplicates in all dimensions; these series will be used as filters for the backup data
        load_sheet_acct = self.df['ACCT'].drop_duplicates()
        load_sheet_cc = self.df['CC'].drop_duplicates()
        load_sheet_io = self.df['IO'].drop_duplicates()
        load_sheet_co = self.df['CO'].drop_duplicates()
        load_sheet_pc = self.df['PC'].drop_duplicates()
        load_sheet_et = self.df['ET'].drop_duplicates()
        load_sheet_scen = self.df['SCEN'].drop_duplicates()
        load_sheet_ver = self.df['VER'].drop_duplicates()
        load_sheet_type = self.df['TYPE'].drop_duplicates()
        load_sheet_year  = self.df['YEAR'].drop_duplicates()

        load_sheet_members = {'acct':load_sheet_acct,'cc':load_sheet_cc,'io':load_sheet_io,'co':load_sheet_co, \
                              'pc':load_sheet_pc,'et':load_sheet_et,'scen':load_sheet_scen,'ver':load_sheet_ver, \
                              'type':load_sheet_type,'year':load_sheet_year}

        # Get the associated pre-load data from the latest FIN_STMT backup file on the shared drive
        df_backup_file = self.process_backup_file(load_sheet_members)

        # Merge the backup file and the load file
        print('Adding backup data to the load file df...')
        left_key = ['ACCT','CC','IO','CO','PC','ET','SCEN','VER','TYPE','YEAR','PERIOD']
        right_key = ['ACCT','CC','IO','CO','PC','ET','SCEN','VER','TYPE','YEAR','PERIOD']
        self.df = self.df.merge(df_backup_file, how='left', left_on=left_key, right_on=right_key)

        # Rename the new columns
        self.df.rename(columns={'DATA_x':'DATA','FileName_x':'FileName', 'DATA_y':'DATA_Backup','FileName_y':'FileName_Backup'},inplace=True)

        # Drop the backup filename column
        self.df = self.df.drop(columns=['FileName_Backup'])

        #Fill all empty cells in the backup data column with zeros
        self.df = self.df.fillna({'DATA_Backup':0})

        # Add the email columns to the dataframe    
        self.df = self.add_email_columns(self.df, self.user_email)

        # Output the load file
        Alteryx.write(self.df,1)

        logging.info("Worksheet validation successful. Load file " + str(self.df.loc[0,'FileName']) + r".txt written to \\disk23\fin_plan-shared\Automation-FPA\Load_Files\Output")
        print('Worksheet validation successful. Load file written to Automation-FPA\Load_Files\Output...')

        # For capacity loads only, create a flag file that indicates which months to load
        # It's written to the same directory as the capacity data file and uses the same load rule
        # The values loaded from this flag file will be referenced when the capacity calc scripts are run
        if all(self.df['VER'].isin(['Current Capacity'])):  # Ensure ALL values in the VER column are 'Current Capacity'
            if all(self.df['CC'].isin(['CC:40001','Non Operating (40001)'])):
                load_flag_value = 2 # Fleet-only load
            else:
                load_flag_value = 1 # Stations and fleet load
            capacity_load_flags = self.create_capacity_flag_file(unique_periods,'Current Capacity',load_flag_value)
            Alteryx.write(capacity_load_flags,2)
            logging.info(r"Capacity flag file written to \\disk23\fin_plan-shared\Automation-FPA\Load_Files\Output")
            print('Capacity flag file created')
        elif all(self.df['ACCT'].isin(capacity_accounts)): # Ensure ALL values in the ACCT column are capacity accounts
            if all(self.df['CC'].isin(['CC:40001','Non Operating (40001)'])):
                load_flag_value = 2
            else:
                load_flag_value = 1
            capacity_load_flags = self.create_capacity_flag_file(unique_periods,'Working',load_flag_value)
            Alteryx.write(capacity_load_flags,2)
            logging.info(r"Capacity flag file written to \\disk23\fin_plan-shared\Automation-FPA\Load_Files\Output")
            print('Capacity flag file created')

        return True

    
            
    def create_error_file(self, error_details_df, error_email_info, error_log_entry=None):
        
        # The "error_log_entry" will be received for all error types except runtime errors

        print('Running create_error_file...')
        
        print('Incoming error dataframe:')
        print(error_details_df)
       
        # Add the FileName column to the dataframe
        error_details_df['FileName'] = self.enhanced_file_name

        # Add the recipients' email columns to the dataframe    
        error_details_df = self.add_email_columns(error_details_df, self.user_email)

        # Add columns containing info/details about the error
        error_details_df['ErrorEmailSubject'] = error_email_info['error_email_subject']
        error_details_df['ErrorFilePath'] = error_email_info['error_email_filepath']
        error_details_df['ErrorEmailBody'] = error_email_info['error_email_body']
            
        print('Errors dataframe:')
        print(error_details_df)

        # Ouput the details of the error(s) but do NOT create the load file
        Alteryx.write(error_details_df, 3)

        if error_log_entry:
            logging.error(error_log_entry)
        
        print('Load file validation FAILED. See the log for details.')

# End of the DataLoader class
        


def summary_information(df):
    
    print('Running summary_information...')
    
    # Create empty dictionary
    summary_info = {}
    
    match_pattern_filepath = r'.*\\(.*).xls[xm]?.[|]{0,}\W{0,}(.*)\$'
    match_pattern_email = r'.*([ex]\d{3,7})@wnco.com$'
    
    z1 = re.match(match_pattern_filepath, df.loc[1,'FileName'])  # FileName actually contains the full path to the file
    z2 = re.match(match_pattern_email, df.loc[1,'UserEmail'])  # Email address is formatted as e12345@wnco.com
        
    if not z1 == None:
        summary_info['workbook_name'] = ''.join(z1.group(1)) # Convert tuple to string
    else:
        summary_info['workbook_name'] = None
        
    if not z1 == None:
        summary_info['load_sheet_name'] = ''.join(z1.group(2)) # Convert tuple to string
    else:
        summary_info['load_sheet_name'] = None

    if not z2 == None:
        summary_info['user_id'] = ''.join(z2.group(1)) # Convert tuple to string
    else:
        summary_info['user_id'] = None
    
    summary_info['user_email'] = df.loc[1,'UserEmail']
    
    return summary_info


def get_input_files():
    
    print("Running get_input_files...")
    
    input_files = {}
    
    # Get the user's load workbook, the Outline Extractor doc files, the list of capacity accounts, and the FIN_STMT backup data file

    # Get the user's load workbook
    # The user will be prompted to browse to it and select the load sheet within the workbook
    # They will also be required to enter their eID (including the 'e')
    df = Alteryx.read("#1")
    input_files['LOADSHEET'] = df

    # Get the Outline Extractor doc files for each dimension
    # As of 8/19/22 these file are retrieved by Alteryx from: \\disk23\fin_plan-shared\Automation-FPA\OutlineExtracts
    # Updated files are placed in that folder as needed (ie, when new cost centers, accounts, etc. are created)
    # The location to get the updated files from is: \\disk23\fin_plan-shared\Automation-ALTERYX\OutlineExtracts
    ACCT_dim = Alteryx.read("#2")
    input_files['ACCT'] = ACCT_dim 

    CC_dim = Alteryx.read("#3")
    input_files['CC'] = CC_dim

    IO_dim = Alteryx.read("#4")
    input_files['IO'] = IO_dim

    CO_dim = Alteryx.read("#5")
    input_files['CO'] = CO_dim

    PC_dim = Alteryx.read("#6")
    input_files['PC'] = PC_dim

    ET_dim = Alteryx.read("#7")
    input_files['ET'] = ET_dim

    SCEN_dim = Alteryx.read("#8")
    input_files['SCEN'] = SCEN_dim

    VER_dim = Alteryx.read("#9")
    input_files['VER'] = VER_dim

    TYPE_dim = Alteryx.read("#10")
    input_files['TYPE'] = TYPE_dim

    YEAR_dim = Alteryx.read("#11")
    input_files['YEAR'] = YEAR_dim

    PERIOD_dim = Alteryx.read("#12")
    input_files['PERIOD'] = PERIOD_dim

    # Get the latest FIN_STMT backup file
    finstmt_backup = Alteryx.read("#13")
    input_files['BACKUP'] = finstmt_backup
	
    # Get the file containing the list of capacity accounts
    capacity_accounts = Alteryx.read("#14")
    input_files['CAPACITY_ACCOUNTS'] = capacity_accounts

    print("All input files have been imported")

    return input_files
    


def main():
    
    # This function is the entry point into the entire process of validating the load sheet and creating a load file
    
    try:
    
        print("Running main...")

        input_files = get_input_files()  # A dictionary is returned that contains all 14 files defined in get_input_files()
        df = input_files['LOADSHEET']
        finstmt_backup = input_files['BACKUP']
		capacity_accounts = input_files['CAPACITY_ACCOUNTS']  # This corresponds to the list of accounts generated in the MXL_CurrCapacity2Wkg calc script

        # Get summary information about the load from the FileName and UserEmail fields
        # Info will include the user's eID and email address, the name of the workbook, and the name of the load sheet
        # Note1: The email address is formatted as e12345@wnco.com
        # Note2: The FileName field contains the full path to the analyst's Excel file
        summary_info = summary_information(df)  # The info will be returned in a dictionary
        user_id = summary_info.get('user_id', '')
        user_email = summary_info.get('user_email', '')
        workbook_name = summary_info.get('workbook_name', '')
        load_sheet_name = summary_info.get('load_sheet_name', '')
        summary_info['enhanced_file_name'] = user_id + '_' + workbook_name + '_' + load_sheet_name + '.txt'

        logging.info("Validating the worksheet " + load_sheet_name + " in " + workbook_name + " for user " + user_id + "...")

        # Display high-level info about the data to load
        print('DataFrame before cleanup and processing:')
        print(df.head())
        print(df.shape)
        print(df.info())

        # Add a new column to the FIN_STMT backup file
        finstmt_backup['FileName'] = 'CORPPLN_Forecast_CY'
        print('FIN_STMT backup data:')
        print(finstmt_backup.head())

        print('Creating the dataload object...')
        my_load_obj = DataLoader(input_files, summary_info)
        if not my_load_obj:
            return False
       
        if my_load_obj.validation_and_cleanup() == False:
            return False

        if my_load_obj.process_load_sheet() == False:
            return False

        return True
    
    except Exception as e:
        
        log = logging.getLogger("fpa_log")
        log.exception(e)

        # Attach the log to the error email
        error_messages = []
        this_function_name = sys._getframe(  ).f_code.co_name
        error_messages.append('The following runtime error occurred in the ' + str(this_function_name) + ' function or one of its sub-functions : ' + str(e))
        runtime_error_df = pd.DataFrame(error_messages, columns=['Runtime Error'])

        try:

            print('Running create_error_file...')
        
            print('Incoming runtime error dataframe:')
            print(runtime_error_df)

            # Add the FileName column to the dataframe
            runtime_error_df['FileName'] = user_id + '_' + workbook_name + '_' + load_sheet_name + '.txt'

            # Add columns containing info/details about the error
            if user_email:
                runtime_error_df['UserEmail'] = user_email
            else:
                runtime_error_df['UserEmail'] = 'e79230@wnco.com'
            runtime_error_df['ccEmail'] = 'e79230@wnco.com'
            runtime_error_df['ErrorEmailSubject'] = 'CRITICAL ERROR: Load process failed - see attachment for details'
            runtime_error_df['ErrorFilePath'] = log_file
            runtime_error_df['ErrorEmailBody'] = 'A critical error occured during the load process. Please see the attachment for details.  NOTE: No data on your sheet has been loaded.'

            print('Updated runtime error dataframe:')
            print(runtime_error_df)

            # Ouput the details of the error(s) but do NOT create the load file
            Alteryx.write(runtime_error_df, 3)

            print('Load file validation FAILED. See the log for details.')
            
        except Exception as e2:
            log = logging.getLogger("fpa_log")
            log.exception(e2)
            
        finally:
            return False
    


if main() == True:
    print('Load sheet was processed successfully')
else:
    print('Load sheet was NOT processed successfully')
