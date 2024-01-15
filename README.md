I built an interactive Alteryx workflow for the financial analysts in the FP&A department that runs a Python script that does the following:

• Imports an analyst's Excel workbook containing forecast updates

• Validates the forecast data; if the validation fails, an automated email containing the list of issues is sent to the analyst

• Transforms the data into a custom-formatted text file 

• Loads the text file into Essbase through an automated batch process (outside of Alteryx)
 
• Sends an automated confirmation email to the analyst
