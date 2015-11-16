import unittest
import json
import yaml
from populate import site_populate
from populate.site_populate import get_users_with_null_sites, calculate_last_user_events, build_s3_url, get_s3_keys, lookup_site_name, build_update_object, find_user_events

class SitePopulateTest(unittest.TestCase):

    def test_get_users_with_null_sites(self):
        result = yaml.load(get_users_with_null_sites(10))
        self.assertEqual(len(result), 10)

    def test_calculate_last_user_events(self):
        test_user_list = yaml.load('[{"sites": [],"lastUpdated": "2014-10-10 13:38:28.172778 +0000","lastLogin": "2014-10-10 13:34:38 +0000","created": "2014-10-10 13:34:38.557625 +0000","uuid": "6f6010fe-63af-4ae9-9bbc-7534cfb814eb"}]')
        result = calculate_last_user_events(test_user_list)
        self.assertEqual(len(result[0]["events"]), 2)
        self.assertEqual(len(result[0]["event_times"]), 2)

    def test_build_s3_url(self):
        test_user_list = yaml.load('[{"uuid": "6f6010fe-63af-4ae9-9bbc-7534cfb814eb", "created": "2014-10-10 13:34:38.557625 +0000", "sites": [], "lastUpdated": "2014-10-10 13:38:28.172778 +0000", "lastLogin": "2014-10-10 13:34:38 +0000", "event_times": ["2014-10-10 13:34:38.557625 +0000", "2014-10-10 13:38:28.172778 +0000"], "events": ["entity_create", "entity_update"], "update_array": []}]')
        result = build_s3_url(test_user_list[0])
        self.assertEqual(len(result), 2)

    def test_get_s3_keys(self):
    	test_user = yaml.load('[{"created": "2014-10-10 00:35:36.400638 +0000", "lastUpdated": "2014-10-10 00:59:22.766024 +0000", "uuid": "d483b9e1-b61c-4878-83bd-7bdaffb90644", "lastLogin": "2014-10-10 00:35:36 +0000", "sites": []}]')[0]
    	test_url = 'capture/entity_update/2014/10/10/00/00/00/fdyc2rm7kvqcnftgyjzsrbawer/'
    	result = get_s3_keys(test_url,test_user)
    	self.assertEqual(len(result), 1)

    def test_lookup_site_name(self):
    	test_client_id = '5wegfxdbreywskakdfnkwpc8efd643et'
    	result = lookup_site_name(test_client_id)
    	self.assertEqual(result, 'CNHI')

    def test_build_update_object(self):
    	test_result_set = yaml.load('[{"last_updated": "2014-10-10 00:59:22.766024 +0000", "event_type": "entity_update", "backup_date": "2014-10-10 00:59:22 +0000", "client_id": "jangca5vg6rt2x8gzfu9uq2ys6yz2ycb", "application_id": "fdyc2rm7kvqcnftgyjzsrbawer", "created": "2014-10-10 00:35:36.400638 +0000", "type_name": "user", "value": {"uuid": "d483b9e1-b61c-4878-83bd-7bdaffb90644"}, "last_login": "2014-10-10 00:35:36 +0000", "attributes": ["/emailVerified"], "apid_version": "1abf03d"}]')
    	result = build_update_object(test_result_set)
    	self.assertEqual(len(result), 1)

    def test_find_user_events(self):
        test_user = yaml.load('[{"uuid": "d483b9e1-b61c-4878-83bd-7bdaffb90644", "created": "2014-10-10 00:35:36.400638 +0000", "sites": [], "lastUpdated": "2014-10-10 00:59:22.766024 +0000", "lastLogin": "2014-10-10 00:35:36 +0000", "event_times": ["2014-10-10 00:35:36.400638 +0000", "2014-10-10 00:59:22.766024 +0000"], "events": ["entity_create", "entity_update"], "update_array": []}, {"uuid": "83f6ffe1-9521-4dc8-b36c-714d19bc2419", "created": "2014-10-10 20:54:34.49473 +0000", "sites": [], "lastUpdated": "2014-10-10 20:54:34.49473 +0000", "lastLogin": "2014-10-10 20:54:34 +0000", "event_times": ["2014-10-10 20:54:34.49473 +0000", "2014-10-10 20:54:34.49473 +0000"], "events": ["entity_create", "entity_update"], "update_array": []}]')[0]
        result = find_user_events(test_user)
        self.assertEqual(len(result), 1)

if __name__ == "__main__":
    unittest.main()

# Brian, help me
# I am trying to import functions from populate/site_populate.py and test them with unittest
# Getting error when I run $ python -m unittest test.test_site_populate.py
# How do I do these imports/run the tests?