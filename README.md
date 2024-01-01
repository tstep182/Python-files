For the benefit of the financial analysts in the FP&A department, I built an interactive Alteryx workflow that runs a Python script that does the following:

• Imports an analyst's Excel workbook containing forecast updates

• Validates and standardizes the metadata on the worksheet the analyst selects to load

• Transforms the sheet into a custom-formatted text file that's loaded into Essbase through an automated batch process
 
• Sends an automated confirmation email to the analyst upon the creation of the text file

• If the validation fails, an automated email containing the list of issues that must be fixed is sent to the analyst
