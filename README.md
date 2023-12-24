This Python script (which runs within an Alteryx workflow) takes an FP&A analyst's Excel workbook containing forecast updates, validates it, and transforms it into a format suitable for loading directly into an Essbase cube with a load rule.

If the validation fails, the user is automatically emailed a list of issues that must be fixed before the load file is created.
