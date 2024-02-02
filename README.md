# KM_Subscription_ETL_Pipelines
 ETL pipelines for the RMI Knowledge Management Newsroom platform. Code extracts, formats, and stores summary information from a variety of relevant publications. This summary information is used to apply keyword tags to content and is surfaced via Power BI on SharePoint for staff to view. Users can then request full-text access for content based on the available summary and tag information.

# Requirements/Setup
- Clone this repository
- Using preferred code editor, review scripts to update hard coded OneDrive file paths to desired output locations
   - Note: This will be changed to a single parameters file in the future.
- Add an environment file, cred.env, with the following:
   - DBASE_PWD= Database password
   - DBASE_IP= IP address for target database
   - Note: Currently, data are stored in an Azure for MySQL environment, schema name rmi_km_news

# Usage
- This ETL process is typically executed daily, Monday through Friday
- The script, "auto_compile.py" is the only script that should be required for standard data processing
- Several temp files updated by this script are housed in SharePoint and used as a part of various Microsoft Power Automate Flows. Contact Knowledge Management for more information
