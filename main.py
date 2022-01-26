# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import os


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
    from helix_fhir_client_sdk.fhir_client import FhirClient
    server_url = "https://fhir.icanbwell.com/4_0_0"
    auth_client_id = os.environ.get("FHIR_CLIENT_ID")
    assert auth_client_id
    auth_client_secret = os.environ.get("FHIR_CLIENT_SECRET")
    assert auth_client_secret
    auth_scopes = ["user/Practitioner.read", "access/medstar.*"]
    fhir_client: FhirClient = FhirClient()
    fhir_client = fhir_client.url(server_url)
    fhir_client = fhir_client.resource("Practitioner")
    fhir_client = fhir_client.client_credentials(auth_client_id, auth_client_secret)
    fhir_client = fhir_client.auth_scopes(auth_scopes)

    result = fhir_client.get()

    import json
    resource_list = json.loads(result.responses)
    for resource in resource_list:
        print(resource['id'])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
