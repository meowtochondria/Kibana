#!/usr/bin/env python
from __future__ import absolute_import, unicode_literals, print_function

import re
try:
    from urllib2 import urlopen, HTTPError
except ImportError:
    # Python 3
    from urllib.request import urlopen
    from urllib.error import HTTPError
import json
import requests
import time
import sys


PY3 = False
if sys.version_info[0] >= 3:
    PY3 = True


def iteritems(d):
    if PY3:
        return d.items()
    else:
        return d.iteritems()


class KibanaMapping():
    def __init__(self, index, index_pattern, host, debug=False):
        self.index = index
        self._index_pattern = index_pattern
        self._host = host
        self.update_urls()
        # from the js possible mappings are:
        #     { type, indexed, analyzed, doc_values }
        # but indexed and analyzed are .kibana specific,
        # determined by the value within ES's 'index', which could be:
        #     { analyzed, no, not_analyzed }
        self.mappings = ['type', 'doc_values']
        # ignore system fields:
        self.sys_mappings = ['_source', '_index', '_type', '_id']
        # .kibana has some fields to ignore too:
        self.mappings_ignore = ['count']
        self.debug = debug

    def pr_dbg(self, msg):
        if self.debug:
            print('[DBG] Mapping %s' % msg)

    def pr_inf(self, msg):
        print('[INF] Mapping %s' % msg)

    def pr_err(self, msg):
        print('[ERR] Mapping %s' % msg)

    def update_urls(self):
        # 'http://localhost:5601/elasticsearch/.kibana/index-pattern/aaa*'
        # 'http://localhost:9200/.kibana/index-pattern/aaa*'
        self.post_url = ('http://%s:%s/' % (self._host[0], self._host[1]) +
                         '%s/' % self.index +
                         'index-pattern/%s' % self._index_pattern)
        # 'http://localhost:5601/elasticsearch/.kibana/index-pattern/aaa*'
        # 'http://localhost:9200/.kibana/index-pattern/_search/?id=aaa*'
        # 'http://localhost:9200/.kibana/index-pattern/aaa*/'
        self.get_url = ('http://%s:%s/' % (self._host[0], self._host[1]) +
                        '%s/' % self.index +
                        'index-pattern/%s/' % self._index_pattern)
        self.indices_url = ('http://%s:%s/' % (self._host[0], self._host[1]) +
                            '_cat/indices/%s?h=index' % self._index_pattern)

    def mapping_url_for_index(self, index):
        # 'http://localhost:5601/elasticsearch/aaa*/_mapping/field/*?ignore_unavailable=false&allow_no_indices=false&include_defaults=true'
        # 'http://localhost:9200/aaa*/_mapping/field/*?ignore_unavailable=false&allow_no_indices=false&include_defaults=true'
        url = ('http://%s:%s/' % (self._host[0], self._host[1]) +
                '%s/' % index +
                '_mapping/field/' +
                '*?ignore_unavailable=false&' +
                'allow_no_indices=false&' +
                'include_defaults=true')
        return url

    def field_capability_url_for_index(self, index):
        # http://localhost:9200/aaaa-2020.02.21/_field_caps?fields=*
        url = ('http://%s:%s/' % (self._host[0], self._host[1]) +
                '%s/' % index +
                '_field_caps?fields=*')
        return url

    @property
    def index_pattern(self):
        return self._index_pattern

    @index_pattern.setter
    def index_pattern_setter(self, index_pattern):
        self._index_pattern = index_pattern
        self.update_urls()

    @property
    def host(self):
        return self._host

    @host.setter
    def host_setter(self, host):
        self._host = host
        self.update_urls()

    def get_field_cache_from_kibana(self):
        try:
            self.pr_dbg("Getting field cache from Kibana: %s" % self.get_url)
            search_results = urlopen(self.get_url).read().decode('utf-8')
            self.pr_dbg('Kibana mapping retrival complete.')
        except HTTPError as e:
            self.pr_err("get_field_cache_from_kibana, HTTPError: %s" % e)
            return []
        index_pattern = json.loads(search_results)
        # Results look like: {"_index":".kibana","_type":"index-pattern","_id":"aaa*","_version":6,"found":true,"_source":{"title":"aaa*","fields":"<what we want>"}}  # noqa
        fields_str = index_pattern['_source']['fields']
        return json.loads(fields_str)

    def get_field_cache_from_es(self):
        self.pr_dbg("Getting list of indices: %s" % self.indices_url)
        indices = urlopen(self.indices_url).read().decode('utf-8').split("\n")
        # Remove empty strings from list because it can cause huge mappings to be retrieved, which we want to avoid.
        indices = [i for i in indices if i]
        num_indices = len(indices)
        self.pr_dbg("%d indices matched: %s" % (num_indices, ', '.join(indices)))
        field_cache = []
        processing_index_num = 0
        for index in indices:
            #index = 'logstash-2020.02.24-000002' # DEBUG
            processing_index_num += 1
            self.pr_dbg('Now processing index %s (%d of %d)' % (index, processing_index_num, num_indices))
            index_mapping_url = self.mapping_url_for_index(index)
            index_mapping_json = urlopen(index_mapping_url).read().decode('utf-8')
            index_mapping = json.loads(index_mapping_json)
            self.pr_dbg('Size of index_mapping json object: %d bytes' % sys.getsizeof(index_mapping))
            m_dict = index_mapping[index]['mappings']
            field_capability_url = self.field_capability_url_for_index(index)
            field_caps_json = urlopen(field_capability_url).read().decode('utf-8')
            field_caps = json.loads(field_caps_json)
            mappings = self.get_index_mappings(m_dict, field_caps['fields'])
            field_cache.extend(mappings)
            # dedupe as soon as possible because this data structure can get huge.
            field_cache = self.dedup_field_cache(field_cache)
            self.pr_dbg('Size of field_cache after deduping: %d bytes' % sys.getsizeof(field_cache))
            #break # DEBUG
        return field_cache

    def dedup_field_cache(self, field_cache):
        deduped = []
        fields_found = {}
        for field in field_cache:
            name = field['name']
            if name not in fields_found:
                deduped.append(field)
                fields_found[name] = field
            elif fields_found[name] != field:
                self.pr_dbg("Dup field doesn't match")
                self.pr_dbg("1st found: %s" % fields_found[name])
                self.pr_dbg("  Dup one: %s" % field)
            # else ignore, pass
        return deduped

    def post_field_cache(self, field_cache):
        """Where field_cache is a list of fields' mappings"""
        index_pattern = self.field_cache_to_index_pattern(field_cache)
        resp = requests.post(self.post_url, data=index_pattern).text
        ## DEBUG
        # Open the file for writing.
        debug_file = '/tmp/post_payload'
        with open(debug_file, 'w') as f:
            f.write('Post URL %s\n' % self.post_url)
            f.write(index_pattern)
            self.pr_dbg('Payload written to %s' % debug_file)
            f.flush()

        # resp ='{}'
        ## END DEBUG

        # resp = {"_index":".kibana","_type":"index-pattern","_id":"aaa*","_version":1,"created":true}  # noqa
        self.pr_dbg(resp)
        if 'error' in resp:
            self.pr_err(resp)
            return 1
        return 0

    def field_cache_to_index_pattern(self, field_cache):
        """Return a .kibana index-pattern doc_type"""
        mapping_dict = {}
        mapping_dict['customFormats'] = "{}"
        mapping_dict['title'] = self.index_pattern
        # now post the data into .kibana
        mapping_dict['fields'] = json.dumps(field_cache, separators=(',', ':'))
        # in order to post, we need to create the post string
        mapping_str = json.dumps(mapping_dict, separators=(',', ':'))
        return mapping_str

    def check_mapping(self, m):
        """Assert minimum set of fields in cache, does not validate contents"""
        if 'name' not in m:
            self.pr_dbg("Missing %s" % "name")
            return False
        for x in ['analyzed', 'indexed', 'type', 'scripted', 'count', 'searchable', 'aggregatable']:
            if x not in m or m[x] == "":
                self.pr_dbg("Missing %s" % x)
                self.pr_dbg("Full %s" % m)
                return False
        if 'doc_values' not in m or m['doc_values'] == "":
            if not m['name'].startswith('_'):
                self.pr_dbg("Missing %s" % "doc_values")
                return False
            m['doc_values'] = False
        return True

    def get_index_mappings(self, index, field_caps):
        """Converts all index's doc_types to .kibana"""
        fields_arr = []
        for (key, val) in iteritems(index):
            # self.pr_dbg("\tdoc_type: %s" % key)
            doc_mapping = self.get_doc_type_mappings(val, field_caps)
            # self.pr_dbg("\tdoc_mapping: %s" % doc_mapping)
            if doc_mapping is None:
                return None
            # keep adding to the fields array
            fields_arr.extend(doc_mapping)
        return fields_arr

    def get_doc_type_mappings(self, doc_type, field_caps):
        """Converts all doc_types' fields to .kibana"""
        doc_fields_arr = []
        found_score = False
        for (key, val) in iteritems(doc_type):
            # self.pr_dbg("\t\tfield: %s" % key)
            # self.pr_dbg("\tval: %s" % val)
            retdict = {}
            # _ are system
            if key.startswith('_'):
                continue

            if 'mapping' not in doc_type[key]:
                self.pr_err("No mapping in doc_type[%s]" % key)
                continue
            if key in doc_type[key]['mapping']:
                subkey_name = key
            else:
                subkey_name = re.sub('.*\.', '', key)
            if subkey_name not in doc_type[key]['mapping']:
                self.pr_err(
                    "Couldn't find subkey " +
                    "doc_type[%s]['mapping'][%s]" % (key, subkey_name))
                continue
            # self.pr_dbg("\t\tsubkey_name: %s" % subkey_name)
            retdict = self.get_field_mappings(
                doc_type[key]['mapping'][subkey_name], field_caps[key])
            # system mappings don't list a type,
            # but kibana makes them all strings
            # if key in self.sys_mappings:
            #     retdict['analyzed'] = False
            #     retdict['indexed'] = False
            #     retdict['searchable'] = False
            #     retdict['aggregatable'] = False
            #     if key == '_source':
            #         retdict = self.get_field_mappings(
            #             doc_type[key]['mapping'][key], field_caps[key])
            #         retdict['type'] = "_source"
            #     elif 'type' not in retdict:
            #         retdict['type'] = "string"
            #     add_it = True
            retdict['name'] = key

            if not self.check_mapping(retdict):
                self.pr_err("Error, invalid mapping for %s" % key)
                continue
            # the fields element is an escaped array of json
            # make the array here, after all collected, then escape it
            doc_fields_arr.append(retdict)
        # if not found_score:
        #     doc_fields_arr.append(
        #         {"name": "_score",
        #          "type": "number",
        #          "count": 0,
        #          "scripted": False,
        #          "indexed": False,
        #          "analyzed": False,
        #          "doc_values": False})
        #self.pr_dbg("\tget_doc_type_mappings: returning doc_fields_arr with %d values." % len(doc_fields_arr))
        return doc_fields_arr

    def get_field_mappings(self, field, capabilities):
        """Converts ES field mappings to .kibana field mappings"""
        retdict = {}
        retdict['doc_values'] = field['doc_values']
        retdict['indexed'] = 'index' in field and field['index']
        retdict['analyzed'] = 'analyzer' in field
        retdict['searchable'] = False
        retdict['aggregatable'] = False
        retdict['count'] = 0  # always init to 0
        retdict['scripted'] = False  # I haven't observed a True yet

        type_val = field['type']
        retdict['searchable'] = capabilities[type_val]['searchable']
        retdict['aggregatable'] = capabilities[type_val]['aggregatable']
        retdict['type'] = type_val
        if (type_val == "long" or
            type_val == "integer" or
            type_val == "double" or
            type_val == "float"):

            retdict['type'] = "number"

        if (type_val == 'text' or
            type_val == 'keyword'):

            retdict['type'] = "string"

        return retdict

    def refresh_poll(self, period):
        self.poll_another = True
        while self.poll_another:
            self.do_refresh()
            self.pr_inf("Polling again in %s secs" % period)
            try:
                time.sleep(period)
            except KeyboardInterrupt:
                self.poll_another = False

    def needs_refresh(self):
        es_cache = self.get_field_cache_from_es()
        k_cache = self.get_field_cache_from_kibana()
        if self.is_kibana_cache_incomplete(es_cache, k_cache):
            return True
        return False

    def do_refresh(self, force=False):
        es_cache = self.get_field_cache_from_es()
        if force:
            self.pr_inf("Forcing mapping update")
            # no need to get kibana if we are forcing it
            return self.post_field_cache(es_cache)
        k_cache = self.get_field_cache_from_kibana()
        if self.is_kibana_cache_incomplete(es_cache, k_cache):
            self.pr_inf("Mapping is incomplete, doing update")
            return self.post_field_cache(es_cache)
        self.pr_inf("Mapping is correct, no refresh needed")
        return 0

    def is_kibana_cache_incomplete(self, es_cache, k_cache):
        """Test if k_cache is incomplete

        Assume k_cache is always correct, but could be missing new
        fields that es_cache has
        """
        # convert list into dict, with each item's ['name'] as key
        k_dict = {}
        for field in k_cache:
            # self.pr_dbg("field: %s" % field)
            k_dict[field['name']] = field
            for ign_f in self.mappings_ignore:
                k_dict[field['name']][ign_f] = 0
        es_dict = {}
        for field in es_cache:
            es_dict[field['name']] = field
            for ign_f in self.mappings_ignore:
                es_dict[field['name']][ign_f] = 0
        es_set = set(es_dict.keys())
        k_set = set(k_dict.keys())
        # reasons why kibana cache could be incomplete:
        #     k_dict is missing keys that are within es_dict
        #     We don't care if k has keys that es doesn't
        # es {1,2} k {1,2,3}; intersection {1,2}; len(es-{}) 0
        # es {1,2} k {1,2};   intersection {1,2}; len(es-{}) 0
        # es {1,2} k {};      intersection {};    len(es-{}) 2
        # es {1,2} k {1};     intersection {1};   len(es-{}) 1
        # es {2,3} k {1};     intersection {};    len(es-{}) 2
        # es {2,3} k {1,2};   intersection {2};   len(es-{}) 1
        return len(es_set - k_set.intersection(es_set)) > 0

    def list_to_compare_dict(self, list_form):
        """Convert list into a data structure we can query easier"""
        compare_dict = {}
        for field in list_form:
            if field['name'] in compare_dict:
                self.pr_dbg("List has duplicate field %s:\n%s" %
                            (field['name'], compare_dict[field['name']]))
                if compare_dict[field['name']] != field:
                    self.pr_dbg("And values are different:\n%s" % field)
                return None
            compare_dict[field['name']] = field
            for ign_f in self.mappings_ignore:
                compare_dict[field['name']][ign_f] = 0
        return compare_dict

    def compare_field_caches(self, replica, original):
        """Verify original is subset of replica"""
        if original is None:
            original = []
        if replica is None:
            replica = []
        self.pr_dbg("Comparing orig with %s fields to replica with %s fields" %
                    (len(original), len(replica)))
        # convert list into dict, with each item's ['name'] as key
        orig = self.list_to_compare_dict(original)
        if orig is None:
            self.pr_dbg("Original has duplicate fields")
            return 1
        repl = self.list_to_compare_dict(replica)
        if repl is None:
            self.pr_dbg("Replica has duplicate fields")
            return 1
        # search orig for each item in repl
        # if any items in repl not within orig or vice versa, then complain
        # make sure contents of each item match
        orig_found = {}
        for (key, field) in iteritems(repl):
            field_name = field['name']
            if field_name not in orig:
                self.pr_dbg("Replica has field not found in orig %s: %s" %
                            (field_name, field))
                return 1
            orig_found[field_name] = True
            if orig[field_name] != field:
                self.pr_dbg("Field in replica doesn't match orig:")
                self.pr_dbg("orig:%s\nrepl:%s" % (orig[field_name], field))
                return 1
        unfound = set(orig_found.keys()) - set(repl.keys())
        if len(unfound) > 0:
            self.pr_dbg("Orig contains fields that were not in replica")
            self.pr_dbg('%s' % unfound)
            return 1
        # We don't care about case when replica has more fields than orig
        # unfound = set(repl.keys()) - set(orig_found.keys())
        # if len(unfound) > 0:
        #     self.pr_dbg("Replica contains fields that were not in orig")
        #     self.pr_dbg('%s' % unfound)
        #     return 1
        self.pr_dbg("Original matches replica")
        return 0

    def test_cache(self):
        """Test if this code is equiv to Kibana.refreshFields()

        Within Kibana GUI click refreshFields, then either:
            * self.test_cache()
            * vagrant ssh -c "python -c \"
                import kibana; kibana.DotKibana('aaa*').mapping.test_cache()\""
        """
        es_cache = self.get_field_cache_from_es()
        # self.pr_dbg(json.dumps(es_cache))
        kibana_cache = self.get_field_cache_from_kibana()
        # self.pr_dbg(json.dumps(kibana_cache))
        return self.compare_field_caches(es_cache, kibana_cache)


# end mapping.py
