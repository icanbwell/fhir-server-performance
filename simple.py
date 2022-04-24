import asyncio
import base64
import json
import os
from typing import Dict, Any

from aiohttp import ClientSession, ClientResponse
from dotenv import load_dotenv
from furl import furl


async def authenticate(client_id, client_secret, fhir_server_url):
    full_uri: furl = furl(furl(fhir_server_url).origin)
    full_uri /= ".well-known/smart-configuration"

    async with ClientSession() as http:
        response: ClientResponse = await http.request(
            "GET", str(full_uri), ssl=False
        )
        response_json = json.loads(await response.text())
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
    async with ClientSession() as http:
        async with http.request(
                "POST", auth_server_url, headers=headers, data=payload
        ) as response:
            token_text: str = await response.text()
            if not token_text:
                return None
            token_json: Dict[str, Any] = json.loads(token_text)

            if "access_token" not in token_json:
                raise Exception(f"No access token found in {token_json}")
            access_token: str = token_json["access_token"]
        return access_token


async def load_data(fhir_server, use_data_streaming):
    fhir_server_url = f"https://{fhir_server}/4_0_0/AuditEvent?_lastUpdated=gt2022-04-20&_lastUpdated=lt2022-04-22&_elements=id&_count=10&_getpagesoffset=0"
    if use_data_streaming:
        fhir_server_url += "&_streamResponse=1"
    # fhir_server_url = "http://localhost:3000/4_0_0/AuditEvent"
    assert os.environ.get("FHIR_CLIENT_ID"), "FHIR_CLIENT_ID environment variable must be set"
    assert os.environ.get("FHIR_CLIENT_SECRET"), "FHIR_CLIENT_SECRET environment variable must be set"
    client_id = os.environ.get("FHIR_CLIENT_ID")
    client_secret = os.environ.get("FHIR_CLIENT_SECRET")

    access_token = await authenticate(client_id=client_id, client_secret=client_secret, fhir_server_url=fhir_server_url)
    headers = {
        # "Accept": "application/fhir+ndjson" if use_data_streaming else "application/fhir+json",
        # "Content-Type": "application/fhir+json",
        # "Accept-Encoding": "gzip,deflate",
        "Authorization": f"Bearer {access_token}"
    }

    payload = {}

    async with ClientSession() as http:
        async with http.request("GET", fhir_server_url, headers=headers, data=payload, ssl=False) as response:
            if use_data_streaming:
                async for data, _ in response.content.iter_chunks():
                    print(data)
            else:
                print(response.status)
                print(await response.text())


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    load_dotenv()

    asyncio.run(load_data(fhir_server="fhir-next.prod-mstarvac.icanbwell.com" ,use_data_streaming=False))
