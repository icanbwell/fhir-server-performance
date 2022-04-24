import asyncio
import json
import os
from typing import Dict, Any

from aiohttp import ClientSession, ClientResponse, ClientPayloadError
from dotenv import load_dotenv
import base64
from furl import furl


async def authenticate(client_id, client_secret, fhir_server_url):
    full_uri: furl = furl(furl(fhir_server_url).origin)
    full_uri /= ".well-known/smart-configuration"

    async with ClientSession() as http:
        response: ClientResponse = await http.request(
            "GET", str(full_uri)
        )
        response_json = json.loads(await response.text())
        auth_server_url = response_json["token_endpoint"]

    auth_scopes = ["user/AuditEvent.*", "access/medstar.*"]

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
        response: ClientResponse = await http.request(
            "POST", auth_server_url, headers=headers, data=payload
        )
        token_text: str = await response.text()
        if not token_text:
            return None
        token_json: Dict[str, Any] = json.loads(token_text)

        if "access_token" not in token_json:
            raise Exception(f"No access token found in {token_json}")
        access_token: str = token_json["access_token"]
        return access_token


async def load_data():
    server_url = "https://fhir.icanbwell.com/4_0_0"
    assert os.environ.get("FHIR_CLIENT_ID"), "FHIR_CLIENT_ID environment variable must be set"
    assert os.environ.get("FHIR_CLIENT_SECRET"), "FHIR_CLIENT_SECRET environment variable must be set"
    client_id = os.environ.get("FHIR_CLIENT_ID")
    client_secret = os.environ.get("FHIR_CLIENT_SECRET")

    login_token = await authenticate(client_id=client_id, client_secret=client_secret, fhir_server_url=server_url)
    use_data_streaming = True
    headers = {
        "Accept": "application/fhir+ndjson" if use_data_streaming else "application/json",
        "Content-Type": "application/fhir+json",
        "Accept-Encoding": "gzip,deflate",
    }

    async with ClientSession() as session:
        async with session.get('http://localhost:3000/4_0_0/Condition/') as resp:
            print(resp.status)
            print(await resp.text())


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    load_dotenv()

    asyncio.run(load_data())
