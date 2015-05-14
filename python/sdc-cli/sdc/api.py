# -*- coding: utf-8 -*-
import json
import logging
import requests
from requests.auth import HTTPBasicAuth
from requests.auth import HTTPDigestAuth
import urlparse

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


class DataCollector:
    """Main data collector API"""

    def __init__(self, url, sdc_user=None, sdc_password=None, auth_type='none'):
        self._base_url = urlparse.urljoin(url, '/rest/v1/')
        self._login_url = urlparse.urljoin(url, 'login')

        self._session = requests.Session()

        setup_auth = {
            'none': self._auth_none,
            'basic': self._auth_basic,
            'digest': self._auth_digest,
            'form': self._auth_form,
        }

        # When a user asks for --help we shouldn't require auth.
        if auth_type is not None:
            setup_auth[auth_type](sdc_user, sdc_password)

        self._json_headers = {'content-type': 'application/json'}
        # The trailing slash is required for urljoin to work properly.
        self._pipeline_store = urlparse.urljoin(self._base_url, 'pipeline-library/')
        self._pipeline = urlparse.urljoin(self._base_url, 'pipeline/')
        self._ping = urlparse.urljoin(url, '/rest/ping')

    def _auth_none(self, sdc_user, sdc_password):
        pass

    def _auth_basic(self, sdc_user, sdc_password):
        self._session.auth = HTTPBasicAuth(sdc_user, sdc_password)

    def _auth_digest(self, sdc_user, sdc_password):
        self._session.auth = HTTPDigestAuth(sdc_user, sdc_password)

    def _auth_form(self, sdc_user, sdc_password):
        credentials = {'j_username': sdc_user, 'j_password': sdc_password}
        logger.info('using login url %s' % self._login_url)
        response = self._session.get(self._login_url, params=credentials)

        if response.status_code != requests.codes.ok:
            raise Exception('Failed to authenticate with SDC. Status %s' % response.status_code)

        self._session.auth = (sdc_user, sdc_password)

    def ping(self):
        response = self._session.get(self._ping)
        return response

    def list_pipelines(self):
        """Returns a list of stored pipelines from the SDC."""
        response = self._session.get(self._pipeline_store)
        return response.json()

    def list_pipeline_names(self):
        return list(i['name'] for i in self.list_pipelines())

    def get_pipeline(self, name):
        """Returns the JSON representation of a pipeline of the given name."""
        response = self._session.get(urlparse.urljoin(self._pipeline_store, name))
        return response.json()

    def create_pipeline(self, pipeline_name):
        """Creates a new pipeline given a name and returns the new pipeline."""
        pipeline_url = urlparse.urljoin(self._pipeline_store, pipeline_name)

        # Create the new Pipeline
        response = requests.put(
            pipeline_url,
        )

        if response.status_code != requests.codes.created:
            raise Exception('Failed to create new pipeline. Perhaps the specified pipeline already exists?')

        return response.json()

    def update_pipeline(self, pipeline_config):
        """Updates a pipeline definition for an existing pipeline."""
        assert pipeline_config['info']['name'] is not None, 'Pipeline must have a name! [%s]' % pipeline_config['info']
        response = requests.post(
            urlparse.urljoin(self._pipeline_store, pipeline_config['info']['name']),
            data=json.dumps(pipeline_config),
            headers=self._json_headers,
        )

        if response.status_code != requests.codes.ok:
            raise Exception('Failed to add stages to pipeline. Status %s' % response.status_code)

        return response

    def get_rules(self, pipeline_name):
        """Get rules for a given pipeline."""
        pipeline_url = urlparse.urljoin(self._pipeline_store, pipeline_name)
        response = self._session.get(
            urlparse.urljoin(pipeline_url + '/', 'rules'),
        )

        if response.status_code != requests.codes.ok:
            raise Exception('Failed to retrieve rules for pipeline %s' % pipeline_name)

        return response.json()

    def update_rules(self, pipeline_rules, pipeline_name):
        """Update rules for the given pipeline."""
        pipeline_url = urlparse.urljoin(self._pipeline_store, pipeline_name)
        response = requests.post(
            # Trailing slash needed to continue appending to pipeline name.
            urlparse.urljoin(pipeline_url + '/', 'rules'),
            data=json.dumps(pipeline_rules),
            headers=self._json_headers,
        )

        if response.status_code != requests.codes.ok:
            raise Exception('Failed to add new pipeline rules. [%s]' % json.dumps(pipeline_rules))

        return response

    def import_pipeline(self, pipeline, pipeline_rules=None):
        """Creates a new pipeline provided its JSON representation."""
        if 'pipelineConfig' in pipeline:
            # This means it was saved as an attachment and we should
            # split it into two requests.
            pipeline_config = pipeline['pipelineConfig']
            pipeline_rules = pipeline['pipelineRules']
        else:
            # If it wasn't a pipeline export, also must provide the rules.
            assert pipeline_rules is not None, 'Pipeline rules cannot be None.'
            pipeline_config = pipeline

        pipeline_name = pipeline_config['info']['name']

        pipeline_uuid = self.create_pipeline(pipeline_name)['uuid']

        # Replace the pipeline configuration's UUID with the one on the server.
        pipeline_config['uuid'] = pipeline_uuid
        pipeline_config['info']['uuid'] = pipeline_uuid

        self.update_pipeline(pipeline_config)

        # Import rules
        rules_uuid = self.get_rules(pipeline_name)['uuid']

        pipeline_rules['uuid'] = rules_uuid

        self.update_rules(pipeline_rules, pipeline_name)

        return self.get_pipeline(pipeline_name)

    def delete_pipeline(self, name):
        """Deletes the pipeline of the specified name."""
        response = requests.delete(
            urlparse.urljoin(self._pipeline_store, name)
        )
        return response

    def pipeline_status(self):
        """Returns the status of the currently active pipeline."""
        response = self._session.get(
            urlparse.urljoin(self._pipeline, 'status'),
        )
        try:
            return response.json()
        except ValueError:
            logger.warning('Failed to decode JSON from response: %s' % response.text)
            return response.text

    def pipeline_metrics(self):
        """Returns metrics of the currently active pipeline."""
        response = self._session.get(
            urlparse.urljoin(self._pipeline, 'metrics'),
        )
        return response.json()

    def start_pipeline(self, name, rev=None):
        """Starts a pipeline of the given name."""
        query = {'name': name}
        if rev is not None:
            query['rev'] = rev

        response = requests.post(
            urlparse.urljoin(self._pipeline, 'start'),
            params=query
        )
        return response

    def stop_pipeline(self):
        """Stops the currently running pipeline.
        The response code should be checked for errors. If a pipeline was not
        running when stop was called it will result in a 500.
        """
        response = requests.post(
            urlparse.urljoin(self._pipeline, 'stop')
        )
        return response
