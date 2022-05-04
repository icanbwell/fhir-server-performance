# Test project for using the Helix FHIR Client SDK
This project shows how you can use the SDK 
(https://github.com/icanbwell/helix.fhir.client.sdk) 
to download large amounts of data from a FHIR server efficiently.

## Running the project

### Prerequisites
1. Python 3.7
2. Pipenv

### One time setup
1. Run `pipenv update` to download all the packages in Pipfile
2. Set following environment variables to authenticate with the FHIR server:
   1. FHIR_CLIENT_ID: OAuth client id issued by the owner of the FHIR server
   2. FHIR_CLIENT_SECRET: OAuth client secret issued by the owner of the FHIR server
   3. FHIR_CLIENT_TAG: Your client tag issued by the owner of the FHIR server.  
      1. This is used only with an advanced security FHIR server such as the Helix FHIR server (https://github.com/icanbwell/fhir-server/blob/master/security.md)
3. Alternatively, you can copy the `.env.template` file to `.env` and set the values in there.  Github will not upload `.env` file since it is in `.gitignore`.
4. Run `main.py` or type `make tests`

