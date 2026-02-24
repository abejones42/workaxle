import aiohttp
import base64
import json

ROOT_URL                = "https://api.pos.dutchie.com/"
TRANSACTIONS_PATH       = "/reporting/transactions"

def _generate_authorization_header(api_key):
    # Encode api_key to bytes
    key_bytes = api_key.encode("ascii")
    # Encode byte key to base64
    key_base64 = base64.b64encode(key_bytes)
    # Decode to ascii
    key_ascii = key_base64.decode("ascii")
    return {"Authorization": f"Basic {key_ascii}"}

async def dutchie_api_get_transactions(api_key, start_date, end_date, include_item_details="true"):
    full_path = ROOT_URL + TRANSACTIONS_PATH
    params = {
        "FromDateUTC": start_date,
        "ToDateUTC": end_date,
        "includeDetail": include_item_details
    }

    headers = _generate_authorization_header(api_key)
    async with aiohttp.ClientSession() as session:
        response = await fetch(session, full_path, params, headers)
    json_response = None
    try:
        json_response = json.loads(response)
    except:
        print("Transactions - Failed to load json: Retrying")
        json_response = await dutchie_api_get_transactions(api_key, start_date, end_date)
    return json_response


async def fetch(session, full_path, params, headers):
    async with session.get(
            full_path,
            params=params,
            headers=headers) as response:
        return await response.text()
