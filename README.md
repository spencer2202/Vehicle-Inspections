VehicleInsight Analytics

Welcome to VehicleInsight Analytics! We partner with a vehicle fleet inspection agency to create an analytics dashboard revealing insightful patterns in vehicle inspections among client organizations. Using Python, psycopg, and ETL projects, we process monthly dumps of vehicle inspection report (VIR) records. This allows us to answer vital questions about vehicle ownership, inspections, and pass rates. 


Future Improvements for implementation:
=======================================

- Slowly Changing Dimension Type 2 (SCD Type 2) for dim_org:
If there is any need to track the historal changes in org names, implementing SCD Type 2 for the org dimension might be beneficial.
For instance, adding like "ValidFrom" and "ValidTo" field. 
This allows us to maintain historical records while keeping track of changes to org attributes over time.  Since there is no specific info related to this in the task and it doesn't seem important to track org name changes, I went with simple approach (Type 1).

- Adding vehicle Dimension (if needed): 
Currently, the vehicle information is stored in the fact table. 
Since vehicle doesn't seem to have other info other that vehicle id, I keep this simple by storing in the fact table. If in the future it has name, or other info, creating separate dimension might be beneficial
to maintain data integrity and reduces redundancy.

Additional Notes:

The provided query is designed for MS SQL Server.
