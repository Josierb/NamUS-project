import os, json, time, grequests, requests

PAGE_SIZE = 10000  # Max results per API request
REQUEST_BATCH_SIZE = 10   # Reduced to avoid rate limiting
BATCH_DELAY = 1           # Seconds to wait between batches
MAX_RETRIES = 3           # How many times to retry a failed case
RETRY_DELAY = 5           # Seconds to wait before retrying
REQUEST_FEEDBACK_INTERVAL = 50

USER_AGENT = "NamUs Scraper / github.com/prepager/namus-scraper"
API_ENDPOINT = "https://www.namus.gov/api"
STATE_ENDPOINT = API_ENDPOINT + "/CaseSets/NamUs/States"
CASE_ENDPOINT = API_ENDPOINT + "/CaseSets/NamUs/MissingPersons/Cases/{case}"
SEARCH_ENDPOINT = API_ENDPOINT + "/CaseSets/NamUs/MissingPersons/Search"
STATE_FIELD = "stateOfLastContact"

DATA_OUTPUT = "./output/MissingPersons/MissingPersons.json"

completedCases = 0


def fetch_case_ids_for_state(state_name):
    """Fetch all case IDs for a state, paginating if the state has >10,000 cases."""
    ids = []
    skip = 0
    while True:
        response = requests.post(
            SEARCH_ENDPOINT,
            headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
            data=json.dumps(
                {
                    "take": PAGE_SIZE,
                    "skip": skip,
                    "projections": ["namus2Number"],
                    "predicates": [
                        {
                            "field": STATE_FIELD,
                            "operator": "IsIn",
                            "values": [state_name],
                        }
                    ],
                }
            ),
        )
        results = response.json().get("results", [])
        ids.extend(results)
        if len(results) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    return ids


def fetch_case_with_retry(namus2Number):
    """Fetch a single case, retrying up to MAX_RETRIES times on failure."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                CASE_ENDPOINT.format(case=namus2Number),
                headers={"User-Agent": USER_AGENT},
                timeout=30,
            )
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY * (attempt + 1))
    return None


def fetch_cases_in_batches(cases):
    """Fetch all cases in small batches with a delay between each batch."""
    results = {}
    total = len(cases)
    for i in range(0, total, REQUEST_BATCH_SIZE):
        batch = cases[i: i + REQUEST_BATCH_SIZE]
        reqs = [
            grequests.get(
                CASE_ENDPOINT.format(case=n),
                headers={"User-Agent": USER_AGENT},
                timeout=30,
            )
            for n in batch
        ]
        responses = grequests.map(reqs, size=REQUEST_BATCH_SIZE)
        for j, response in enumerate(responses):
            namus2Number = batch[j]
            if response and response.status_code == 200:
                results[namus2Number] = response.json()
            else:
                results[namus2Number] = None  # Mark for retry

        completed = min(i + REQUEST_BATCH_SIZE, total)
        if completed % REQUEST_FEEDBACK_INTERVAL == 0 or completed == total:
            print(" > Fetched {completed}/{total} cases".format(completed=completed, total=total))

        time.sleep(BATCH_DELAY)

    return results


def main():
    print("Fetching states\n")
    states = requests.get(STATE_ENDPOINT, headers={"User-Agent": USER_AGENT}).json()

    print("Collecting: MissingPersons")

    print(" > Fetching case identifiers (paginated per state)")
    case_states = {}
    for state in states:
        state_cases = fetch_case_ids_for_state(state["name"])
        print("   - {state}: {count} cases".format(state=state["name"], count=len(state_cases)))
        for c in state_cases:
            case_states.setdefault(c["namus2Number"], []).append(state["name"])

    cases = list(case_states.keys())
    multistate = sum(1 for v in case_states.values() if len(v) > 1)
    print(" > Found %d unique cases (%d multi-state)" % (len(cases), multistate))

    print(" > Fetching case details in batches (batch size: %d, delay: %ds)" % (REQUEST_BATCH_SIZE, BATCH_DELAY))
    case_data = fetch_cases_in_batches(cases)

    # Retry any that failed
    failed = [n for n, d in case_data.items() if d is None]
    if failed:
        print(" > Retrying %d failed cases..." % len(failed))
        for namus2Number in failed:
            data = fetch_case_with_retry(namus2Number)
            if data:
                case_data[namus2Number] = data
            else:
                print(" > Permanently failed: %s" % namus2Number)

    print(" > Writing output file")
    os.makedirs(os.path.dirname(DATA_OUTPUT), exist_ok=True)
    successful = [(n, d) for n, d in case_data.items() if d is not None]
    with open(DATA_OUTPUT, "w") as outputFile:
        outputFile.write("[")
        for i, (namus2Number, data) in enumerate(successful):
            data["searchStates"] = case_states[namus2Number]
            outputFile.write(json.dumps(data) + ("" if i + 1 == len(successful) else ","))
        outputFile.write("]")

    print(" > Saved %d/%d cases" % (len(successful), len(cases)))
    print("\nScraping completed")


main()
