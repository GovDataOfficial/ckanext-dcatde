import unittest

from ckanext.dcatde.validation.shacl_validation import ShaclValidator
from ckantoolkit.tests import helpers
from mock import patch

VALIDATOR_API_URL = "http://foo:8050/shacl/dcat-ap.de/api/"
VALIDATION_PROFILE = "all"
DATASET_TEST_URI = "<https://www.foo.de/bar/1e17f560-2061-4219-a37d-61e2fce75336>"
TEST_ORGANIZATION_ID = "1e17f560-2061-4219-a37d-61e2fce75336"
TEST_QUERY = '''"CONSTRUCT { ?s ?p ?o } WHERE { " +
    "<https://www.foo.de/bar/1e17f560-2061-4219-a37d-61e2fce75336> (<>|!<>)* ?s . " +
    "  ?s ?p ?o ." +
    "}"'''


class TestShaclValidatorClient(unittest.TestCase):
    """
    Test class for the ShaclValidator
    """

    @helpers.change_config('ckanext.dcatde.shacl_validator.api_url', VALIDATOR_API_URL)
    @helpers.change_config('ckanext.dcatde.shacl.validator.profile.type', VALIDATION_PROFILE)
    def test_get_validator_config_success(self):
        """ Tests if _get_validator_config() return the correct values """

        endpoint_base_url, validation_profile = ShaclValidator._get_validator_config()
        self.assertEqual(endpoint_base_url, VALIDATOR_API_URL)


    @helpers.change_config('ckanext.dcatde.shacl.validator.profile.type', VALIDATION_PROFILE)
    def test_get_validator_endpoint_fail(self):
        """ Tests if _get_validator_config() returns None if no valid endpoint is available """

        endpoint_base_url, validation_profile = ShaclValidator._get_validator_config()
        self.assertEqual(endpoint_base_url, None)


    @helpers.change_config('ckanext.dcatde.shacl_validator.api_url', VALIDATOR_API_URL)
    def test_get_validator_profile_fail(self):
        """ Tests if _get_validator_config() returns None if no valid profile is available """

        endpoint_base_url, validation_profile = ShaclValidator._get_validator_config()
        self.assertEqual(validation_profile, None)


    def test_validate_fail_no_url(self):
        """ Tests if validate() returns None if url is not set properly """

        client = ShaclValidator()
        result = client.validate(TEST_QUERY, DATASET_TEST_URI, TEST_ORGANIZATION_ID)
        self.assertEqual(result, None)


    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator._get_validator_config')
    def test_validate_fail_validator_not_available(self, mock_validator_get_config):
        """ Tests if validate() returns None if validator is not available """

        mock_validator_get_config.return_value = VALIDATOR_API_URL, VALIDATION_PROFILE

        client = ShaclValidator()
        result = client.validate(TEST_QUERY, DATASET_TEST_URI, TEST_ORGANIZATION_ID)
        self.assertEqual(result, None)


    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator._get_validator_config')
    @patch('ckanext.dcatde.validation.shacl_validation.requests.post')
    def test_validate_fail_validator_responds_with_bad_status(self, mock_requests_post, mock_validator_get_config):
        """ Tests if validate() returns None if the post request has a bad status response """

        mock_requests_post.return_value.status_code = 404
        mock_validator_get_config.return_value = VALIDATOR_API_URL, VALIDATION_PROFILE

        client = ShaclValidator()
        result = client.validate(TEST_QUERY, DATASET_TEST_URI, TEST_ORGANIZATION_ID)

        self.assertEqual(result, None)


    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator._get_validator_config')
    @patch('ckanext.dcatde.validation.shacl_validation.requests.post')
    def test_validate_successful_request(self, mock_requests_post, mock_validator_get_config):
        """ Tests if validate() returns the expected value if the post request is successful """

        contributor_id = 'http://dcat-ap.de/def/contributors/test'

        expected_repsonse = "SUCCESS"
        mock_requests_post.return_value.status_code = 200
        mock_requests_post.return_value.text = expected_repsonse
        mock_validator_get_config.return_value = VALIDATOR_API_URL, VALIDATION_PROFILE

        client = ShaclValidator()
        result = client.validate(TEST_QUERY, DATASET_TEST_URI, TEST_ORGANIZATION_ID, contributor_id)

        self.assertEqual(result, expected_repsonse)

    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator._get_validator_config')
    @patch('ckanext.dcatde.validation.shacl_validation.requests.post')
    def test_validate_successful_request_without_contributor_id(self, mock_requests_post,
                                                                mock_validator_get_config):
        """ Tests if validate() returns the expected value if the post request is successful 
            without contributorID
        """

        expected_repsonse = "SUCCESS"
        mock_requests_post.return_value.status_code = 200
        mock_requests_post.return_value.text = expected_repsonse
        mock_validator_get_config.return_value = VALIDATOR_API_URL, VALIDATION_PROFILE

        client = ShaclValidator()
        result = client.validate(TEST_QUERY, DATASET_TEST_URI, TEST_ORGANIZATION_ID)

        self.assertEqual(result, expected_repsonse)


    def test_get_report_query(self):
        """ Tests if get_report_query() returns the expected Query """

        expected_value = u"""PREFIX sh: <http://www.w3.org/ns/shacl#>
            PREFIX dqv: <http://www.w3.org/ns/dqv#>
            PREFIX govdata: <http://govdata.de/mqa/#>
            CONSTRUCT {{
                ?report dqv:computedOn <{dataset_uri}> .
                ?report govdata:attributedTo '{owner_org}' .
                ?s ?p ?o .
            }} WHERE {{
                {{ ?report a sh:ValidationReport . }}
                UNION
                {{ ?s ?p ?o . }}
            }}""".format(dataset_uri=DATASET_TEST_URI, owner_org=TEST_ORGANIZATION_ID)

        report_query = ShaclValidator._get_report_query(DATASET_TEST_URI, TEST_ORGANIZATION_ID, None)
        self.assertEqual(report_query, expected_value)

    def test_get_report_query_with_contributor_id(self):
        """ Tests if get_report_query() returns the expected Query with contributorID """

        contributor_id = 'http://dcat-ap.de/def/contributors/test'

        expected_value = u"""PREFIX sh: <http://www.w3.org/ns/shacl#>
            PREFIX dqv: <http://www.w3.org/ns/dqv#>
            PREFIX govdata: <http://govdata.de/mqa/#>
            CONSTRUCT {{
                ?report dqv:computedOn <{dataset_uri}> .
                ?report govdata:attributedTo '{owner_org}' .
                ?report govdata:attributedTo <{contributor_id}> .
                ?s ?p ?o .
            }} WHERE {{
                {{ ?report a sh:ValidationReport . }}
                UNION
                {{ ?s ?p ?o . }}
            }}""".format(dataset_uri=DATASET_TEST_URI, owner_org=TEST_ORGANIZATION_ID,
                         contributor_id=contributor_id)

        report_query = ShaclValidator._get_report_query(
            DATASET_TEST_URI, TEST_ORGANIZATION_ID, contributor_id)
        self.assertEqual(report_query, expected_value)
