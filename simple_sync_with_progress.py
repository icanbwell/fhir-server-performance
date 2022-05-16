import asyncio
import base64
import json
import os
from typing import Dict, Any
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
from furl import furl
from requests import Session, Response
from requests.exceptions import ChunkedEncodingError


async def authenticate(client_id, client_secret, fhir_server_url):
    full_uri: furl = furl(furl(fhir_server_url).origin)
    full_uri /= ".well-known/smart-configuration"

    # print(f"Calling {full_uri}")
    with Session() as http:
        response: Response = http.request(
            "GET", str(full_uri)
        )
        response_json = json.loads(response.text)
        auth_server_url = response_json["token_endpoint"]

    auth_scopes = ["user/AuditEvent.read", "access/medstar.*"]

    login_token: str = base64.b64encode(
        f"{client_id}:{client_secret}".encode("ascii")
    ).decode("ascii")

    payload: str = (
        "grant_type=client_credentials&scope=" + "%20".join(auth_scopes)
        if auth_scopes
        else ""
    )
    # noinspection SpellCheckingInspection
    headers: Dict[str, str] = {
        "Accept": "application/json",
        "Authorization": "Basic " + login_token,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    # print(f"Calling {auth_server_url}")
    with Session() as http:
        with http.request(
                "POST", auth_server_url, headers=headers, data=payload
        ) as response:
            token_text: str = response.text
            if not token_text:
                return None
            token_json: Dict[str, Any] = json.loads(token_text)

            if "access_token" not in token_json:
                raise Exception(f"No access token found in {token_json}")
            access_token: str = token_json["access_token"]
        return access_token


async def load_data(fhir_server: str, use_data_streaming: bool, limit: int, use_atlas: bool, retrieve_only_ids: bool,
                    use_access_index: bool = False):
    """
    loads data
    :param use_access_index:
    :param retrieve_only_ids:
    :type retrieve_only_ids:
    :param use_atlas:
    :type use_atlas:
    :param fhir_server:
    :type use_data_streaming:
    :param limit:
    :return: None
    """
    greater_than = "2021-11-23"
    less_than = "2021-11-24"
    fhir_server_url = f"https://{fhir_server}/4_0_0/AuditEvent?_lastUpdated=gt{greater_than}&_lastUpdated=lt{less_than}&_count={limit}&_getpagesoffset=0"
    # fhir_server_url = f"https://{fhir_server}/4_0_0/AuditEvent?_lastUpdated=gt2022-04-20&_lastUpdated=lt2022-04-22&_elements=id&_count={limit}&_getpagesoffset=0"
    if retrieve_only_ids:
        fhir_server_url += "&_elements=id"
    if use_atlas:
        fhir_server_url += "&_useAtlas=1"
    if use_data_streaming:
        fhir_server_url += "&_streamResponse=1"
    if use_access_index:
        fhir_server_url += "&_useAccessIndex=1"
    # _useTwoStepOptimization
    # fhir_server_url += "&_useTwoStepOptimization=1"
    # cursor_batch_size = 1000000
    # if cursor_batch_size:
    #     fhir_server_url += f"&_cursorBatchSize={cursor_batch_size}"
    assert os.environ.get("FHIR_CLIENT_ID"), "FHIR_CLIENT_ID environment variable must be set"
    assert os.environ.get("FHIR_CLIENT_SECRET"), "FHIR_CLIENT_SECRET environment variable must be set"
    client_id = os.environ.get("FHIR_CLIENT_ID")
    client_secret = os.environ.get("FHIR_CLIENT_SECRET")

    access_token = await authenticate(client_id=client_id, client_secret=client_secret, fhir_server_url=fhir_server_url)
    headers = {
        "Accept": "application/fhir+ndjson" if use_data_streaming else "application/fhir+json",
        "Content-Type": "application/fhir+json",
        "Accept-Encoding": "gzip,deflate",
        "Authorization": f"Bearer {access_token}",
        "Connection": "keep-alive",
        "x-no-compression": "1"
    }

    payload = {}

    start_job = time.time()

    dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print(f"{dt_string}: Calling {fhir_server_url} with Atlas={use_atlas}")
    with Session() as http:
        with http.request("GET", fhir_server_url, headers=headers, data=payload, stream=use_data_streaming) as response:
            dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            print(f"{dt_string}: Received response for {fhir_server_url} with Atlas={use_atlas}.")
            print(f"{dt_string}: Headers= {response.headers}")
            with open('output.json', mode='wb') as file:
                chunk_number = 0
                if use_data_streaming:
                    buffer = b""

                    try:
                        # if you want to receive data one line at a time
                        line: bytes
                        for line in response.iter_content(chunk_size=1000):
                            # await asyncio.sleep(0)
                            chunk_number += 1
                            # dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                            chunk_end_time = time.time()
                            file.write(line)
                            # file.write("\n".encode('utf-8'))
                            print(f"[{chunk_number}] {timedelta(seconds=chunk_end_time - start_job)}", end='\r')
                    except ChunkedEncodingError as e:
                        print(response)
                    #     print(f"[{chunk_number}] {dt_string}: {line}", end='\r')
                    # if you want to receive data in a binary buffer
                    # async for data, _ in response.content.iter_chunks():
                    #     chunk_number += 1
                    #     buffer += data
                    #     dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    #     print(f"[{chunk_number}] {dt_string}: {data}")
                else:
                    print(response.status_code)
                    print(response.text)
    end_job = time.time()
    print(f"====== Received {chunk_number} resources in {timedelta(seconds=end_job - start_job)}"
          f" with Atlas={use_atlas} =======")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    load_dotenv()

    prod_fhir_server_external = "fhir.icanbwell.com"
    prod_fhir_server = "fhir.prod-mstarvac.icanbwell.com"
    prod_next_fhir_server = "fhir-next.icanbwell.com"

    # print("--------- Prod FHIR no data streaming -----")
    # asyncio.run(load_data(fhir_server=prod_fhir_server, use_data_streaming=False, limit=10000000,
    #                       use_atlas=False, retrieve_only_ids=True))
    # print("--------- Prod Next FHIR no data streaming -----")
    # asyncio.run(load_data(fhir_server=prod_next_fhir_server, use_data_streaming=False, limit=10000000,
    #                       use_atlas=False, retrieve_only_ids=True))
    # print("--------- Prod Next FHIR with data streaming -----")
    # asyncio.run(load_data(fhir_server=prod_next_fhir_server, use_data_streaming=True, limit=10000000,
    #                       use_atlas=False, retrieve_only_ids=True))
    # print("--------- Prod Next FHIR with data streaming and Atlas -----")
    # asyncio.run(load_data(fhir_server=prod_next_fhir_server, use_data_streaming=True, limit=10000000,
    #                       use_atlas=True, retrieve_only_ids=True))
    # print("--------- Prod Next FHIR with data streaming and Atlas and useAccessIndex -----")
    # asyncio.run(load_data(fhir_server=prod_next_fhir_server, use_data_streaming=True, limit=10000000,
    #                       use_atlas=True, retrieve_only_ids=True, use_access_index=False))
    # print("--------- Prod Next FHIR with data streaming, full resources -----")
    # asyncio.run(load_data(fhir_server=prod_next_fhir_server, use_data_streaming=True, limit=1000,
    #                       use_atlas=False, retrieve_only_ids=False))
    print("--------- Prod Next FHIR with data streaming and Atlas, full resources -----")
    asyncio.run(load_data(fhir_server=prod_next_fhir_server, use_data_streaming=True, limit=10000,
                          use_atlas=True, retrieve_only_ids=False))
    # print("--------- Prod  FHIR external, full resources -----")
    # asyncio.run(load_data(fhir_server=prod_fhir_server_external, use_data_streaming=False, limit=100,
    #                       use_atlas=False, retrieve_only_ids=False))
    # print("--------- Prod FHIR internal, full resources -----")
    # asyncio.run(load_data(fhir_server=prod_fhir_server, use_data_streaming=False, limit=100,
    #                       use_atlas=False, retrieve_only_ids=False))
