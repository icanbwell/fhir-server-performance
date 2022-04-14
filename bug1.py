import os
import requests

from dotenv import load_dotenv


def call_fhir_server(resource_url):
    assert os.environ.get("FHIR_AUTH_TOKEN"), "FHIR_AUTH_TOKEN environment variable must be set"

    fhir_server_host = "https://fhir.staging.bwell.zone"
    full_url = f"{fhir_server_host}{resource_url}"
    payload = {}
    headers = {
        f'Authorization': f'Bearer {os.environ.get("FHIR_AUTH_TOKEN")}'
    }

    response = requests.request("GET", full_url, headers=headers, data=payload)
    print(response.status_code)
    if response.status_code == 401:
        print("ERROR: ========= Your FHIR_AUTH_TOKEN has expired ==========")
    assert response.status_code == 200
    return response.json()


def load_ids(url, practitioner_ids):
    response_json = call_fhir_server(url)
    # read the next url if it exists and store all the ids from the entry array
    link_array = [x["url"] for x in response_json["link"] if x['relation'] == 'next']
    print(link_array)

    practitioner_ids.extend([x["resource"]["id"] for x in response_json["entry"]])
    print("practitioner ids from single call")
    # print(practitioner_ids)
    if len(link_array) > 0:
        next_url = link_array[0]
        print(f"loading next page: {next_url}")
        print(f"practitioner id count: {len(practitioner_ids)}")
        load_ids(next_url, practitioner_ids)
    else:
        print(response_json)


if __name__ == '__main__':
    load_dotenv()
    page_size = 200
    url = f"/4_0_0/Practitioner?_elements=id&_security=https://www.icanbwell.com/access|unitypoint&_useAtlas=1&_count={page_size}"
    practitioner_ids = []
    load_ids(url, practitioner_ids)
    print(practitioner_ids)
    print(f"total practitioner id count: {len(practitioner_ids)}")
    assert '1003219551' in practitioner_ids, f"practitioner id 1003219551 was not found"
