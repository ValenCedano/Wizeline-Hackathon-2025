-- Create an external connection to     

-- Use Admin role
USE ROLE ACCOUNTADMIN;

-- Select the database and schema where you want to create the rule
--USE DATABASE HACK_DATA;
--USE SCHEMA PUBLIC;

CREATE OR REPLACE NETWORK RULE gh_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('raw.githubusercontent.com', 'githubusercontent.com', 'github.com', 'data.gharchive.org');
  COMMENT = 'Github access rule via API to download files';


CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION gh_access_integration
  ALLOWED_NETWORK_RULES = (gh_network_rule)
  ENABLED = true;