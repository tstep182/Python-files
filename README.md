This Python script (which runs within an Alteryx workflow) takes an FP&A analyst's Excel workbook containing forecast updates, validates it, and transforms it into a format suitable for loading directly into an Essbase cube with a load rule. Upon the creation of the load file, a confirmation email is automatically sent to the analyst.

If the validation fails, the analyst is automatically emailed a list of issues that must be fixed. A partial load file will NOT be created. 
