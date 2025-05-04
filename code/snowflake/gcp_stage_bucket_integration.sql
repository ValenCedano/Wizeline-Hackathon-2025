CREATE STORAGE INTEGRATION gcs_integration -- Or your chosen integration name
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://hack_data_fusion/');


  DESC STORAGE INTEGRATION gcs_integration; -- Use the name you chose above


  CREATE OR REPLACE STAGE my_gcs_stage -- Choose a name for your stage
  URL = 'gcs://hack_data_fusion/data/' -- Must be one of the locations in STORAGE_ALLOWED_LOCATIONS
  STORAGE_INTEGRATION = gcs_integration -- Use the integration name created earlier
  FILE_FORMAT = ( TYPE = JSON );