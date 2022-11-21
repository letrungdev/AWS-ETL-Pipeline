Data Pipeline for Data incrementally load (update and insert)
- S3: data lake 
- Lambda: trigger
- Glue: ETL 
- Redshift: data warehouse
Lambda will be triggered when file is added to S3, schedule Glue Job to update multiple tables parallel in Redshift.
