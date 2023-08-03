register_module_line('TheHive Project_Custom', 'start', __line__())

### pack version: 1.0

import urllib3
import json

# Disable insecure warnings
urllib3.disable_warnings()

DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


class Client(BaseClient):
    def __init__(self,
                 base_url=None,
                 verify=False,
                 mirroring=None,
                 headers=None,
                 proxy=None
                 ):
        super().__init__(
            base_url=base_url, verify=verify, headers=headers, proxy=proxy)
        self.mirroring = mirroring
        self.version = self.get_version()

    def get_version(self):
        res = self._http_request(
            'GET', 'status', ok_codes=[200, 201, 404], resp_type='response')
        if "versions" in res.json():
            if "TheHive" in res.json()['versions']:
                return res.json()['versions']['TheHive']
            else:
                return "Unknown"

    def get_cases(self, limit: int=None):
        instance = demisto.integrationInstance()
        cases = list()
        res = self.list_cases()
        for case in res:
            if '_id' in case:
                case['tasks'] = self.get_tasks(case['_id'])
                case['observables'] = self.list_observables(case['_id'])
            case['instance'] = instance
            case['mirroring'] = self.mirroring
            cases.append(case)
        if limit and type(cases) == list:
            if len(cases) > limit:
                return cases[0:limit]
        return cases

    def get_case(self, case_id):
        case = self._http_request(
            'GET',
            f'case/{case_id}',
            params={"headers": {"Content-Type": "application/json"}})

        case['tasks'] = self.get_tasks(case_id)
        case['observables'] = self.list_observables(case_id)
        return case

    def create_case(self, details: dict=None):
        res = self._http_request(
            'POST',
            '/v1/case',
            ok_codes=[200, 201, 404],
            json_data=details,
            resp_type='response',
            params={"headers": {"Content-Type": "application/json"}})

        if res.status_code not in [200, 201]:
            return (res.status_code, res.text)
        else:
            case = res.json()
            return case

    def remove_case(self, case_id):
        url = f'/v1/case/{case_id}'
        res = self._http_request(
            'DELETE',
            url,
            ok_codes=[200, 201, 204, 404],
            resp_type='response',
            timeout=360)
        if res.status_code not in [200, 201, 204]:
            return (res.status_code, res.text)
        else:
            return res.status_code

    def get_tasks(self, case_id):
        query = {
            "query": [
                {
                    "_name": "getCase",
                    "idOrName": case_id
                },
                {
                    "_name": "tasks"
                }
            ]
        }
        res = self._http_request(
            'POST',
            'v1/query',
            json_data=query,
            ok_codes=[200, 201, 204, 404, 503],
            resp_type='response'
        )
        if res.status_code != 200:
            return None
        tasks = [x for x in res.json()]
        if tasks:
            for task in tasks:
                if "id" in task:
                    logs = self.get_task_logs(task['id'])
                elif "_id" in task:
                    logs = self.get_task_logs(task['_id'])
                else:
                    logs = []
                task['logs'] = logs
        return tasks

    def get_task_logs(self, task_id: str=None):
        res = self._http_request(
            'GET',
            f'case/task/{task_id}/log',
            ok_codes=[200, 404],
            resp_type='response')
        if res.status_code != 200:
            return []
        else:
            logs = list()
            for log in res.json():
                log['has_attachments'] = True if log.get('attachment', None) else False
                logs.append(log)
            return logs

    def list_cases(self):
        query = {
            "query": [
                {
                    "_name": "listCase"
                }
            ]
        }
        res = self._http_request(
            'POST', 'v1/query', json_data=query, resp_type='json')
        return res

    def list_observables(self, case_id: str=None):
        query = {
            "query": [
                {
                    "_name": "getCase",
                    "idOrName": case_id
                },
                {
                    "_name": "observables"
                },
                {
                    "_name": "page",
                    "from": 0,
                    "to": 15
                }
            ]
        }

        res = self._http_request(
            'POST', 'v1/query', json_data=query,
            params={"headers": {"Content-Type": "application/json"}})
        return res

    def update_case(self, case_id: str=None, data: dict=None):
        if case_id is None:
            raise DemistoException(f'Error updating case - case_id not found')

        response = self._http_request(
            'PATCH', f'/case/{case_id}', json_data=data, resp_type="response")
        return response.json() if response.status_code == 201 or response.status_code == 200 else None

    def search_cases(self, last_timestamp):
        query = {
            "query": [
                {
                    "_name": "listCase"
                },
                {
                    "_name": "filter",
                    "_gt": {
                        "_field": "_updatedAt",
                        "_value": last_timestamp
                    }
                }
            ]
        }
        res = self._http_request(
            'POST', 'v1/query', json_data=query,
            params={"headers": {"Content-Type": "application/json"}}
            )


#####################################################
            ### COMMAND FUNCTIONS ###
#####################################################


def list_cases_command(client: Client, args: dict):
    limit: int = args.get('limit', None)
    #limit = 10
    res = client.get_cases(limit=int(limit) if limit else None)
    res = sorted(res, key=lambda x: x['_id'])
    if res:
        for case in res:
            if '_createdAt' in case:
                case_date_dt = dateparser.parse(str(case['_createdAt']))
                case['_createdAt'] = case_date_dt.strftime(DATE_FORMAT)
            if '_updatedAt' in case:
                case_update_dt = dateparser.parse(str(case['_updatedAt']))
                case['_updatedAt'] = case_update_dt.strftime(DATE_FORMAT)

        read = tableToMarkdown('TheHive Cases:', res, [
            'id',
            'title',
            'description',
            'createdAt'
            ])
    else:
        read = "No cases to be displayed."

    return CommandResults(
        outputs_prefix='TheHive.Cases',
        outputs_key_field="id",
        outputs=res,
        readable_output=res,
    )


def get_case_command(client: Client, args: dict):
    case_id = args.get('id')
    case = client.get_case(case_id)
    if case:
        case_date_dt = dateparser.parse(str(case['createdAt']))
        case_update_dt = dateparser.parse(str(case['updatedAt']))
        if case_date_dt:
            case['createdAt'] = case_date_dt.strftime(DATE_FORMAT)
        if case_update_dt:
            case['updatedAt'] = case_update_dt.strftime(DATE_FORMAT)

        headers = ['id', 'title', 'description', 'createdAt']
        read = tableToMarkdown(f'TheHive Case ID {case_id}:', case, headers)
    else:
        read = "No case with the given ID."
    return CommandResults(
        outputs_prefix='TheHive.Cases',
        outputs_key_field='id',
        outputs=case,
        readable_output=case,
    )


def updating_case_command(client: Client, args: dict):
    ### Getting the case ID
    case_id = args.get('id')
    if not case_id:
        raise DemistoException('No case ID')

    ### Getting the original case
    original_case_data = client.get_case(case_id)
    if not original_case_data:
        raise DemistoException(f'Could not find case ID {case_id}.')

    ### Prepare the tag field
    args['tags'] = argToList(args.get('tags', []))

    ### The fields available for the update
    available_fields = [
        "title",
        "description",
        "severity",
        "tags",
        "flag",
        "tlp",
        "pap",
        "status",
        "assignee",
        "customFields"
        ]

    ### Cleaning up the data that we got from the user
    cleanedUp_data_to_update = {}

    for k, v in args.items():
        if v:
            cleanedUp_data_to_update[k] = v

    ### Adding the new data to the orginial case
    new_case_data = original_case_data | cleanedUp_data_to_update

    ### Cleaning up the new data
    cleanedUp_new_case_data = {}

    for k, v in new_case_data.items():
        if v and k in available_fields:
            cleanedUp_new_case_data[k] = v

    response = client.update_case(case_id, cleanedUp_new_case_data)

    if response is None:
        raise DemistoException(f'Error updating case ({case_id}) - {response}')

    case = client.get_case(case_id)
    if "_createdAt" in case:
        case_date_dt = dateparser.parse(str(case['_createdAt']))
        case['_createdAt'] = case_date_dt.strftime(DATE_FORMAT)
    if "_updatedAt" in case:
        case_update_dt = dateparser.parse(str(case['updatedAt']))
        case['_updatedAt'] = case_update_dt.strftime(DATE_FORMAT)

    read = tableToMarkdown(f'TheHive Update Case ID {case_id}:', case, [
        'id',
        'title',
        'description',
        'createdAt'
        ])

    return CommandResults(
        outputs_prefix='TheHive.Cases',
        outputs_key_field="id",
        outputs=case,
        readable_output=case
    )


def fix_element(args: dict):
    """
    Fix args to fit API types requirements.

    Args:
        args (dict): args to fix
    """
    types_dict = {
        'title': str,
        'description': str,
        'tlp': arg_to_number,
        'pap': arg_to_number,
        'severity': arg_to_number,
        'flag': argToBoolean,
        'tags': argToList,
        'startDate': dateparser.parse,
        'metrics': argToList,
        'customFields': str,
        'tasks': argToList,
        'template': str
    }
    for k, v in args.items():
        args[k] = types_dict.get(k, str)(v)  # type: ignore
        if k == 'tasks':
            args[k] = [fix_element(task) for task in args[k]]


def creating_case_command(client: Client, args: dict):
    fix_element(args)
    case = client.create_case(args)
    if type(case) == tuple:
        raise DemistoException(f'Error creating case ({case[0]}) - {case[1]}')

    if "_createdAt" in case:
        case_date_dt = dateparser.parse(str(case['_createdAt']))
        case['_createdAt'] = case_date_dt.strftime(DATE_FORMAT)
    if "_updatedAt" in case:
        case_update_dt = dateparser.parse(str(case['_updatedAt']))
        case['_updatedAt'] = case_update_dt.strftime(DATE_FORMAT)

    read = tableToMarkdown('TheHive newly Created Case:', case, [
        'id',
        'title',
        'description',
        'createdAt'
        ])

    return CommandResults(
        outputs_prefix='TheHive.Cases',
        outputs_key_field="id",
        outputs=case,
        readable_output=read,
    )


def removing_case_command(client: Client, args: dict):
    case_id = args.get('id')
    case = client.get_case(case_id)
    if not case:
        raise DemistoException(f'No case found with ID {case_id}')

    res = client.remove_case(case_id)
    if type(res) == tuple:
        raise DemistoException(
            f'Error removing case ID {case_id} ({res[0]}) - {res[1]}'
        )

    return f'Case ID {case_id} permanently removed successfully'


def get_version_command(client: Client, args: dict):
    version = client.get_version()
    return version


def test_module(client: Client):
    res = client._http_request('GET', 'case', resp_type="response")
    if res.status_code == 200:
        return 'ok'
    else:
        return res.text


def get_mapping_fields_command(client: Client, args: dict) -> Dict[str, Any]:
    instance_name = demisto.integrationInstance()
    schema = client.get_cases(limit=1)
    schema = schema[0] if schema and type(schema) == list else {}
    schema_id = schema.get('_id', None)
    schema = client.get_case(schema_id) if schema_id else {
        "Warning": "No cases to pull schema from."
    }
    schema['dbotMirrorDirection'] = client.mirroring
    schema['dbotMirrorInstance'] = instance_name
    return {f"Default Schema {client.version}": schema}


def get_remote_data_command(client: Client, args: dict):
    parsed_args = GetRemoteDataArgs(args)

    parsed_entries = []
    case: dict = client.get_case(parsed_args.remote_incident_id)
    valid_status = ["New", "InProgress", "Open"]
    if not case:
        parsed_entries.append({
            'Type': EntryType.NOTE,
            'Contents': {
                'dbotIncidentClose': True,
                'closeReason': 'Deleted',
                'closeNotes': 'Case no longer exists',
                'casestatus': 'Deleted'
            },
            'ContentsFormat': EntryFormat.JSON
        })
        case = {'caseId': parsed_args.remote_incident_id}
    elif case['status'] not in valid_status:  # Handle closing the case
        parsed_entries.append({
            'Type': EntryType.NOTE,
            'Contents': {
                'dbotIncidentClose': True,
                'closeReason': case.get('resolutionStatus', ''),
                'closeNotes': case.get('summary', '')
            },
            'ContentsFormat': EntryFormat.JSON
        })

    return GetRemoteDataResponse(case, parsed_entries)  # mypy: ignore


def get_modified_remote_data_command(client: Client, args: dict):
    remote_args = GetModifiedRemoteDataArgs(args)
    last_update = remote_args.last_update
    last_update_utc = dateparser.parse(last_update, settings={
        'TIMEZONE': 'UTC'
    })
    assert last_update_utc is not None, f'could not parse {last_update}'
    last_update_utc = last_update_utc.replace(tzinfo=None)
    last_timestamp = int(last_update_utc.timestamp() * 1000)

    cases = client.search_cases(last_timestamp)
    demisto.debug(cases)
    incident_ids = [x['_id'] for x in cases] if cases else []
    return GetModifiedRemoteDataResponse(incident_ids)


def fetch_incidents(client: Client, fetch_closed: bool=False):
    last_run = demisto.getLastRun()
    last_timestamp = int(last_run.get('timestamp', 0))
    res = client.get_cases()
    demisto.debug(f"number of returned cases from the api:{len(res)}")

    ### Add a condition to update newly added observables or tasks
    if fetch_closed:
        res[:] = [x for x in res if x['_createdAt'] > last_timestamp]
    else:
        valid_status = ["New", "InProgress", "Open"]
        res[:] = [
            x for x in res
            if x['_createdAt'] > last_timestamp
            and x['status'] in valid_status
            ]

    res = sorted(res, key=lambda x: x['_createdAt'])
    incidents = list()
    instance_name = demisto.integrationInstance()
    mirror_direction = demisto.params().get('mirror')
    mirror_direction = None if mirror_direction == "Disabled" else mirror_direction

    for case in res:
        case['dbotMirrorDirection'] = mirror_direction
        case['dbotMirrorInstance'] = instance_name
        incident = {
            'name': case['title'],
            'occurred': timestamp_to_datestring(case['_createdAt']),
            'severity': case['severity'],
            'rawJSON': json.dumps(case)
        }
        incidents.append(incident)
        last_timestamp = case['_createdAt'] if case['_createdAt'] > last_timestamp else last_timestamp
    demisto.setLastRun({"timestamp": str(last_timestamp)})
    demisto.debug(f"number of cases after filtering: {len(incidents)}")
    return incidents


def update_remote_system_command(client: Client, args: dict) -> str:
    parsed_args = UpdateRemoteSystemArgs(args)
    changes = {
        k: v for k, v in parsed_args.delta.items() if k in parsed_args.data.keys()
    }
    if parsed_args.remote_incident_id:
        # Apply the updates
        client.update_case(case_id=parsed_args.remote_incident_id, data=changes)
    return parsed_args.remote_incident_id


def debug_mirroring_command(client: Client, args: dict):
    mirror_stats = demisto.executeCommand("getMirrorStatistics", {})
    if mirror_stats and isinstance(mirror_stats[0], dict):
        mirror_stats_output = mirror_stats[0].get('Contents', {})
        demisto.results(mirror_stats_output)
    else:
        demisto.results("Failed to retrieve mirror statistics.")

    return


def main() -> None:
    params = demisto.params()
    args = demisto.args()
    mirroring = params.get('mirror', 'Disabled').title()
    api_key = params.get('credentials', {}).get('password') or params.get('apiKey')
    if not api_key:
        raise DemistoException('API Key must be provided.')
    client = Client(
        base_url=urljoin(params.get('url'), '/api'),
        verify=not params.get('insecure', False),
        headers={'Authorization': f'Bearer {api_key}'},
        proxy=params.get('proxy', False),
        mirroring=None if mirroring == 'Disabled' else mirroring,
    )

    command = demisto.command()

    command_map = {
        'thehive-list-cases': list_cases_command,
        'thehive-get-case': get_case_command,
        'thehive-updating-case': updating_case_command,
        'thehive-creating-case': creating_case_command,
        'thehive-removing-case': removing_case_command,
        'thehive-get-version': get_version_command,
        'get-mapping-fields': get_mapping_fields_command,
        'get-remote-data': get_remote_data_command,
        'get-modified-remote-data': get_modified_remote_data_command,
        'update-remote-system': update_remote_system_command,
        'debug-mirroring': debug_mirroring_command
    }
    demisto.debug(f'Command being called is {command}')
    try:

        if command == 'test-module':
            # This is the call made when pressing the integration Test button.
            result = test_module(client)
            return_results(result)

        elif command == 'fetch-incidents':
            # Set and define the fetch incidents command to run after
            # activated via integration settings.
            incidents = fetch_incidents(client, demisto.params().get(
                'fetch_closed', True))
            demisto.incidents(incidents)

        elif command in command_map:
            return_results(command_map[command](client, args))  # type: ignore

    except Exception as err:
        return_error(
            f'Failed to execute {command} command. \nError: {str(err)}')


if __name__ in ('__main__', '__builtin__', 'builtins'):
    main()

register_module_line('TheHive Project_Custom', 'end', __line__())
