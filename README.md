This Python script (which runs within an interactive Alteryx workflow) takes an FP&A analyst's Excel workbook containing forecast updates, validates it, and transforms it into a text file that is loaded into an Essbase cube through an automated batch process. A confirmation email is automatically sent to the analyst upon the creation of the load file.

If the validation fails, the analyst is automatically emailed a list of issues that must be fixed. A partial load file will NOT be created. 
