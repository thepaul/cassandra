# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
from .cqlhandling import cql_typename, cql_escape, cql_dequote

try:
    import json
except ImportError:
    import simplejson as json

keywords = set((
    'select', 'from', 'where', 'and', 'key', 'insert', 'update', 'with',
    'limit', 'using', 'consistency', 'one', 'quorum', 'all', 'any',
    'local_quorum', 'each_quorum', 'two', 'three', 'use', 'count', 'set',
    'begin', 'apply', 'batch', 'truncate', 'delete', 'in', 'create',
    'keyspace', 'schema', 'columnfamily', 'table', 'index', 'on', 'drop',
    'primary', 'into', 'values', 'timestamp', 'ttl', 'alter', 'add', 'type',
    'compact', 'storage', 'order', 'by', 'asc', 'desc'
))

columnfamily_options = (
    'comment',
    'bloom_filter_fp_chance',
    'caching',
    'read_repair_chance',
    # 'local_read_repair_chance',   -- not yet a valid cql option
    'gc_grace_seconds',
    'min_compaction_threshold',
    'max_compaction_threshold',
    'replicate_on_write',
    'compaction_strategy_class',
)

columnfamily_map_options = (
    ('compaction_strategy_options',
        ()),
    ('compression_parameters',
        ('sstable_compression', 'chunk_length_kb', 'crc_check_chance')),
)

def cql3_escape_value(value):
    return cql_escape(value)

def cql3_escape_name(name):
    return '"%s"' % name.replace('"', '""')

def cql3_dequote_value(value):
    return cql_dequote(value)

def cql3_dequote_name(name):
    name = name.strip()
    if name == '':
        return name
    if name[0] == '"':
        name = name[1:-1].replace('""', '"')
    return name

valid_cql3_word_re = re.compile(r'^[a-z][0-9a-z_]*$', re.I)

def is_valid_cql3_name(s):
    return valid_cql3_word_re.match(s) is not None and s not in keywords

def maybe_cql3_escape_name(name):
    if is_valid_cql3_name(name):
        return name
    return cql3_escape_name(name)

class CqlColumnDef:
    index_name = None

    def __init__(self, name, cqltype):
        self.name = name
        self.cqltype = cqltype

    @classmethod
    def from_layout(cls, layout):
        c = cls(layout[u'column'], cql_typename(layout[u'validator']))
        c.index_name = layout[u'index_name']
        return c

class CqlTableDef:
    json_attrs = ('column_aliases', 'compaction_strategy_options', 'compression_parameters')
    composite_type_name = 'org.apache.cassandra.db.marshal.CompositeType'
    column_class = CqlColumnDef

    key_components = ()
    columns = ()

    def __init__(self, name):
        self.name = name

    @classmethod
    def from_layout(cls, layout, coldefs):
        cf = cls(name=layout[u'columnfamily'])
        for attr, val in layout.items():
            setattr(cf, attr.encode('ascii'), val)
        for attr in cls.json_attrs:
            try:
                setattr(cf, attr, json.loads(getattr(cf, attr)))
            except AttributeError:
                pass
        cf.key_components = [cf.key_alias.decode('ascii')] + list(cf.column_aliases)
        cf.key_validator = cql_typename(cf.key_validator)
        cf.default_validator = cql_typename(cf.default_validator)
        cf.columns = cls.parse_composite(cf.key_components, cf.comparator) \
                   + map(cls.column_class.from_layout, coldefs)
        return cf

    @classmethod
    def parse_composite(cls, col_aliases, comparator):
        if comparator.startswith(cls.composite_type_name + '('):
            subtypes = comparator[len(cls.composite_type_name) + 1:-1].split(',')
        else:
            subtypes = [comparator]
        assert len(subtypes) == len(col_aliases)
        return [cls.column_class(a, cql_typename(t)) for (a, t) in zip(col_aliases, subtypes)]
