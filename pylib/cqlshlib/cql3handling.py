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
from warnings import warn
from .cqlhandling import (cql_typename, cql_escape, cql_dequote,
                          make_module_completers)

try:
    import json
except ImportError:
    import simplejson as json

class UnexpectedTableStructure(UserWarning):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return 'Unexpected table structure; may not translate correctly to CQL. ' + self.msg

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

special_completers = []
completer_for, explain_completion = make_module_completers(special_completers)



# BEGIN SYNTAX/COMPLETION RULE DEFINITIONS

syntax_rules = r'''
<Start> ::= <CQL_Statement>*
          ;

<CQL_Statement> ::= [statements]=<statementBody> ";"
                  ;

# the order of these terminal productions is significant:
<endline> ::= /\n/ ;

JUNK ::= /([ \t\r\f\v]+|(--|[/][/])[^\n\r]*([\n\r]|$)|[/][*].*?[*][/])/ ;

<stringLiteral> ::= /'([^']|'')*'/ ;
<quotedName> ::=    /"([^"]|"")*"/ ;
<float> ::=         /-?[0-9]+\.[0-9]+/ ;
<integer> ::=       /-?[0-9]+/ ;
<uuid> ::=          /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/ ;
<identifier> ::=    /[a-z][a-z0-9_]*/ ;
<colon> ::=         ":" ;
<star> ::=          "*" ;
<endtoken> ::=      ";" ;
<op> ::=            /[-+=,().]/ ;
<cmp> ::=           /[<>]=?/ ;

<unclosedString>  ::= /'([^']|'')*/ ;
<unclosedComment> ::= /[/][*][^\n]*$/ ;

<symbol> ::= <star>
           | <op>
           | <cmp>
           ;
<name> ::= <identifier>
         | <stringLiteral>
         | <quotedName>
         | <integer>
         ;
<term> ::= <stringLiteral>
         | <integer>
         | <float>
         | <uuid>
         ;
<colname> ::= <term>
            | <identifier>
            ;

<statementBody> ::= <useStatement>
                  | <selectStatement>
                  | <dataChangeStatement>
                  | <schemaChangeStatement>
                  ;

<dataChangeStatement> ::= <insertStatement>
                        | <updateStatement>
                        | <deleteStatement>
                        | <truncateStatement>
                        | <batchStatement>
                        ;

<schemaChangeStatement> ::= <createKeyspaceStatement>
                          | <createColumnFamilyStatement>
                          | <createIndexStatement>
                          | <dropKeyspaceStatement>
                          | <dropColumnFamilyStatement>
                          | <dropIndexStatement>
                          | <alterTableStatement>
                          ;

<consistencylevel> ::= cl=<identifier> ;

<storageType> ::= typename=( <identifier> | <stringLiteral> );
'''

@completer_for('consistencylevel', 'cl')
def cl_completer(ctxt, cass):
    return consistency_levels

@completer_for('storageType', 'typename')
def storagetype_completer(ctxt, cass):
    return cql_types

syntax_rules += r'''
<useStatement> ::= "USE" ksname=<name>
                 ;
'''

@completer_for('useStatement', 'ksname')
def use_ks_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_keyspace_names())

syntax_rules += r'''
<selectStatement> ::= "SELECT" <whatToSelect>
                        "FROM" ( selectks=<name> "." )? selectsource=<name>
                          ("USING" "CONSISTENCY" <consistencylevel>)?
                          ("WHERE" <selectWhereClause>)?
                          ("LIMIT" <integer>)?
                    ;
<selectWhereClause> ::= <relation> ("AND" <relation>)*
                      | keyname=<colname> "IN" "(" <term> ("," <term>)* ")"
                      ;
<relation> ::= [rel_lhs]=<colname> ("=" | "<" | ">" | "<=" | ">=") <colname>
             ;
<whatToSelect> ::= colname=<colname> ("," colname=<colname>)*
                 | ("FIRST" <integer>)? "REVERSED"? (rangestart=<colname> ".." rangeend=<colname>
                                                     | "*")
                 | "COUNT" countparens="(" "*" ")"
                 ;
'''

@completer_for('selectStatement', 'selectsource')
def select_source_completer(ctxt, cass):
    ks = ctxt.get_binding('selectks', None)
    if ks is not None:
        ks = cql_dequote(ks)
    try:
        cfnames = cass.get_columnfamily_names(ks)
    except Exception:
        if ks is None:
            return ()
        raise
    return map(maybe_cql_escape, cfnames)

@completer_for('selectStatement', 'selectks')
def select_keyspace_completer(ctxt, cass):
    return [maybe_cql_escape(ks) + '.' for ks in cass.get_keyspace_names()]

@completer_for('selectWhereClause', 'keyname')
def select_where_keyname_completer(ctxt, cass):
    ksname = ctxt.get_binding('selectks')
    if ksname is not None:
        ksname = cql_dequote(ksname)
    selectsource = cql_dequote(ctxt.get_binding('selectsource'))
    cfdef = cass.get_columnfamily(selectsource, ksname=ksname)
    return [cfdef.key_alias if cfdef.key_alias is not None else 'KEY']

@completer_for('relation', 'rel_lhs')
def select_relation_lhs_completer(ctxt, cass):
    ksname = ctxt.get_binding('selectks')
    if ksname is not None:
        ksname = cql_dequote(ksname)
    selectsource = cql_dequote(ctxt.get_binding('selectsource'))
    return map(maybe_cql_escape, cass.filterable_column_names(selectsource, ksname=ksname))

@completer_for('whatToSelect', 'countparens')
def select_count_parens_completer(ctxt, cass):
    return ['(*)']

explain_completion('whatToSelect', 'colname')
explain_completion('whatToSelect', 'rangestart', '<range_start>')
explain_completion('whatToSelect', 'rangeend', '<range_end>')

syntax_rules += r'''
<insertStatement> ::= "INSERT" "INTO" ( insertks=<name> "." )? insertcf=<name>
                               "(" keyname=<colname> ","
                                   [colname]=<colname> ( "," [colname]=<colname> )* ")"
                      "VALUES" "(" <term> "," <term> ( "," <term> )* ")"
                      ( "USING" [insertopt]=<usingOption>
                                ( "AND" [insertopt]=<usingOption> )* )?
                    ;
<usingOption> ::= "CONSISTENCY" <consistencylevel>
                | "TIMESTAMP" <integer>
                | "TTL" <integer>
                ;
'''

@completer_for('insertStatement', 'insertks')
def insert_ks_completer(ctxt, cass):
    return [maybe_cql_escape(ks) + '.' for ks in cass.get_keyspace_names()]

@completer_for('insertStatement', 'insertcf')
def insert_cf_completer(ctxt, cass):
    ks = ctxt.get_binding('insertks', None)
    if ks is not None:
        ks = cql_dequote(ks)
    try:
        cfnames = cass.get_columnfamily_names(ks)
    except Exception:
        if ks is None:
            return ()
        raise
    return map(maybe_cql_escape, cfnames)

@completer_for('insertStatement', 'keyname')
def insert_keyname_completer(ctxt, cass):
    insertcf = ctxt.get_binding('insertcf')
    cfdef = cass.get_columnfamily(cql_dequote(insertcf))
    return [cfdef.key_alias if cfdef.key_alias is not None else 'KEY']

explain_completion('insertStatement', 'colname')

@completer_for('insertStatement', 'insertopt')
def insert_option_completer(ctxt, cass):
    opts = set('CONSISTENCY TIMESTAMP TTL'.split())
    for opt in ctxt.get_binding('insertopt', ()):
        opts.discard(opt.split()[0])
    return opts

syntax_rules += r'''
<updateStatement> ::= "UPDATE" ( updateks=<name> "." )? updatecf=<name>
                        ( "USING" [updateopt]=<usingOption>
                                  ( "AND" [updateopt]=<usingOption> )* )?
                        "SET" <assignment> ( "," <assignment> )*
                        "WHERE" <updateWhereClause>
                    ;
<assignment> ::= updatecol=<colname> "=" update_rhs=<colname>
                                         ( counterop=( "+" | "-"? ) <integer> )?
               ;
<updateWhereClause> ::= updatefiltercol=<colname> "=" <term>
                      | updatefilterkey=<colname> filter_in="IN" "(" <term> ( "," <term> )* ")"
                      ;
'''

@completer_for('updateStatement', 'updateks')
def update_cf_completer(ctxt, cass):
    return [maybe_cql_escape(ks) + '.' for ks in cass.get_keyspace_names()]

@completer_for('updateStatement', 'updatecf')
def update_cf_completer(ctxt, cass):
    ks = ctxt.get_binding('updateks', None)
    if ks is not None:
        ks = cql_dequote(ks)
    try:
        cfnames = cass.get_columnfamily_names(ks)
    except Exception:
        if ks is None:
            return ()
        raise
    return map(maybe_cql_escape, cfnames)

@completer_for('updateStatement', 'updateopt')
def insert_option_completer(ctxt, cass):
    opts = set('CONSISTENCY TIMESTAMP TTL'.split())
    for opt in ctxt.get_binding('updateopt', ()):
        opts.discard(opt.split()[0])
    return opts

@completer_for('assignment', 'updatecol')
def update_col_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    colnames = map(maybe_cql_escape, [cm.name for cm in cfdef.column_metadata])
    return colnames + [Hint('<colname>')]

@completer_for('assignment', 'update_rhs')
def update_countername_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    curcol = cql_dequote(ctxt.get_binding('updatecol', ''))
    return [maybe_cql_escape(curcol)] if is_counter_col(cfdef, curcol) else [Hint('<term>')]

@completer_for('assignment', 'counterop')
def update_counterop_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    curcol = cql_dequote(ctxt.get_binding('updatecol', ''))
    return ['+', '-'] if is_counter_col(cfdef, curcol) else []

@completer_for('updateWhereClause', 'updatefiltercol')
def update_filtercol_completer(ctxt, cass):
    cfname = cql_dequote(ctxt.get_binding('cf'))
    return map(maybe_cql_escape, cass.filterable_column_names(cfname))

@completer_for('updateWhereClause', 'updatefilterkey')
def update_filterkey_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    return [cfdef.key_alias if cfdef.key_alias is not None else 'KEY']

@completer_for('updateWhereClause', 'filter_in')
def update_filter_in_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    fk = ctxt.get_binding('updatefilterkey')
    return ['IN'] if fk in ('KEY', cfdef.key_alias) else []

syntax_rules += r'''
<deleteStatement> ::= "DELETE" ( [delcol]=<colname> ( "," [delcol]=<colname> )* )?
                        "FROM" ( deleteks=<name> "." )? deletecf=<name>
                        ( "USING" [delopt]=<deleteOption> ( "AND" [delopt]=<deleteOption> )* )?
                        "WHERE" <updateWhereClause>
                    ;
<deleteOption> ::= "CONSISTENCY" <consistencylevel>
                 | "TIMESTAMP" <integer>
                 ;
'''

@completer_for('deleteStatement', 'deleteks')
def update_cf_completer(ctxt, cass):
    return [maybe_cql_escape(ks) + '.' for ks in cass.get_keyspace_names()]

@completer_for('deleteStatement', 'deletecf')
def delete_cf_completer(ctxt, cass):
    ks = ctxt.get_binding('deleteks', None)
    if ks is not None:
        ks = cql_dequote(ks)
    try:
        cfnames = cass.get_columnfamily_names(ks)
    except Exception:
        if ks is None:
            return ()
        raise
    return map(maybe_cql_escape, cfnames)

@completer_for('deleteStatement', 'delopt')
def delete_opt_completer(ctxt, cass):
    opts = set('CONSISTENCY TIMESTAMP'.split())
    for opt in ctxt.get_binding('delopt', ()):
        opts.discard(opt.split()[0])
    return opts

explain_completion('deleteStatement', 'delcol', '<column_to_delete>')

syntax_rules += r'''
<batchStatement> ::= "BEGIN" "BATCH"
                        ( "USING" [batchopt]=<usingOption>
                                  ( "AND" [batchopt]=<usingOption> )* )?
                        [batchstmt]=<batchStatementMember> ";"
                            ( [batchstmt]=<batchStatementMember> ";" )*
                     "APPLY" "BATCH"
                   ;
<batchStatementMember> ::= <insertStatement>
                         | <updateStatement>
                         | <deleteStatement>
                         ;
'''

@completer_for('batchStatement', 'batchopt')
def batch_opt_completer(ctxt, cass):
    opts = set('CONSISTENCY TIMESTAMP'.split())
    for opt in ctxt.get_binding('batchopt', ()):
        opts.discard(opt.split()[0])
    return opts

syntax_rules += r'''
<truncateStatement> ::= "TRUNCATE" ( truncateks=<name> "." )? truncatecf=<name>
                      ;
'''

@completer_for('truncateStatement', 'truncateks')
def update_cf_completer(ctxt, cass):
    return [maybe_cql_escape(ks) + '.' for ks in cass.get_keyspace_names()]

@completer_for('truncateStatement', 'truncatecf')
def truncate_cf_completer(ctxt, cass):
    ks = ctxt.get_binding('truncateks', None)
    if ks is not None:
        ks = cql_dequote(ks)
    try:
        cfnames = cass.get_columnfamily_names(ks)
    except Exception:
        if ks is None:
            return ()
        raise
    return map(maybe_cql_escape, cfnames)

syntax_rules += r'''
<createKeyspaceStatement> ::= "CREATE" "KEYSPACE" ksname=<name>
                                 "WITH" [optname]=<optionName> "=" [optval]=<optionVal>
                                 ( "AND" [optname]=<optionName> "=" [optval]=<optionVal> )*
                            ;
<optionName> ::= <identifier> ( ":" ( <identifier> | <integer> ) )?
               ;
<optionVal> ::= <stringLiteral>
              | <identifier>
              | <integer>
              ;
'''

explain_completion('createKeyspaceStatement', 'ksname', '<new_keyspace_name>')

@completer_for('createKeyspaceStatement', 'optname')
def create_ks_opt_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('optname', ())
    try:
        stratopt = exist_opts.index('strategy_class')
    except ValueError:
        return ['strategy_class =']
    vals = ctxt.get_binding('optval')
    stratclass = cql_dequote(vals[stratopt])
    if stratclass in ('SimpleStrategy', 'OldNetworkTopologyStrategy'):
        return ['strategy_options:replication_factor =']
    return [Hint('<strategy_option_name>')]

@completer_for('createKeyspaceStatement', 'optval')
def create_ks_optval_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('optname', (None,))
    if exist_opts[-1] == 'strategy_class':
        return map(cql_escape, replication_strategies)
    return [Hint('<option_value>')]

syntax_rules += r'''
<createColumnFamilyStatement> ::= "CREATE" ( "COLUMNFAMILY" | "TABLE" ) cf=<name>
                                    "(" keyalias=<colname> <storageType> "PRIMARY" "KEY"
                                        ( "," colname=<colname> <storageType> )* ")"
                                   ( "WITH" [cfopt]=<cfOptionName> "=" [optval]=<cfOptionVal>
                                     ( "AND" [cfopt]=<cfOptionName> "=" [optval]=<cfOptionVal> )* )?
                                ;

<cfOptionName> ::= cfoptname=<identifier> ( cfoptsep=":" cfsubopt=( <identifier> | <integer> ) )?
                 ;

<cfOptionVal> ::= <identifier>
                | <stringLiteral>
                | <integer>
                | <float>
                ;
'''

explain_completion('createColumnFamilyStatement', 'keyalias', '<new_key_name>')
explain_completion('createColumnFamilyStatement', 'cf', '<new_table_name>')
explain_completion('createColumnFamilyStatement', 'colname', '<new_column_name>')

@completer_for('cfOptionName', 'cfoptname')
def create_cf_option_completer(ctxt, cass):
    return [c[0] for c in columnfamily_options] + \
           [c[0] + ':' for c in columnfamily_map_options]

@completer_for('cfOptionName', 'cfoptsep')
def create_cf_suboption_separator(ctxt, cass):
    opt = ctxt.get_binding('cfoptname')
    if any(opt == c[0] for c in columnfamily_map_options):
        return [':']
    return ()

@completer_for('cfOptionName', 'cfsubopt')
def create_cf_suboption_completer(ctxt, cass):
    opt = ctxt.get_binding('cfoptname')
    if opt == 'compaction_strategy_options':
        # try to determine the strategy class in use
        prevopts = ctxt.get_binding('cfopt', ())
        prevvals = ctxt.get_binding('optval', ())
        for prevopt, prevval in zip(prevopts, prevvals):
            if prevopt == 'compaction_strategy_class':
                csc = cql_dequote(prevval)
                break
        else:
            cf = ctxt.get_binding('cf')
            try:
                csc = cass.get_columnfamily(cf).compaction_strategy
            except Exception:
                csc = ''
        csc = csc.split('.')[-1]
        if csc == 'SizeTieredCompactionStrategy':
            return ['min_sstable_size']
        elif csc == 'LeveledCompactionStrategy':
            return ['sstable_size_in_mb']
    for optname, _, subopts in columnfamily_map_options:
        if opt == optname:
            return subopts
    return ()

def create_cf_option_val_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('cfopt')
    this_opt = exist_opts[-1]
    if this_opt == 'compression_parameters:sstable_compression':
        return map(cql_escape, available_compression_classes)
    if this_opt == 'compaction_strategy_class':
        return map(cql_escape, available_compaction_classes)
    if any(this_opt == opt[0] for opt in obsolete_cf_options):
        return ["'<obsolete_option>'"]
    if this_opt in ('comparator', 'default_validation'):
        return cql_types
    if this_opt == 'read_repair_chance':
        return [Hint('<float_between_0_and_1>')]
    if this_opt == 'replicate_on_write':
        return [Hint('<yes_or_no>')]
    if this_opt in ('min_compaction_threshold', 'max_compaction_threshold', 'gc_grace_seconds'):
        return [Hint('<integer>')]
    return [Hint('<option_value>')]

completer_for('createColumnFamilyStatement', 'optval') \
    (create_cf_option_val_completer)

syntax_rules += r'''
<createIndexStatement> ::= "CREATE" "INDEX" indexname=<identifier>? "ON"
                               cf=<name> "(" col=<colname> ")"
                         ;
'''

explain_completion('createIndexStatement', 'indexname', '<new_index_name>')

@completer_for('createIndexStatement', 'cf')
def create_index_cf_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_columnfamily_names())

@completer_for('createIndexStatement', 'col')
def create_index_col_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    colnames = [md.name for md in cfdef.column_metadata if md.index_name is None]
    return map(maybe_cql_escape, colnames)

syntax_rules += r'''
<dropKeyspaceStatement> ::= "DROP" "KEYSPACE" ksname=<name>
                          ;
'''

@completer_for('dropKeyspaceStatement', 'ksname')
def drop_ks_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_keyspace_names())

syntax_rules += r'''
<dropColumnFamilyStatement> ::= "DROP" ( "COLUMNFAMILY" | "TABLE" ) cf=<name>
                              ;
'''

@completer_for('dropColumnFamilyStatement', 'cf')
def drop_cf_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_columnfamily_names())

syntax_rules += r'''
<dropIndexStatement> ::= "DROP" "INDEX" indexname=<name>
                       ;
'''

@completer_for('dropIndexStatement', 'cf')
def drop_index_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_index_names())

syntax_rules += r'''
<alterTableStatement> ::= "ALTER" ( "COLUMNFAMILY" | "TABLE" ) cf=<name> <alterInstructions>
                        ;
<alterInstructions> ::= "ALTER" existcol=<name> "TYPE" <storageType>
                      | "ADD" newcol=<name> <storageType>
                      | "DROP" existcol=<name>
                      | "WITH" [cfopt]=<cfOptionName> "=" [optval]=<cfOptionVal>
                        ( "AND" [cfopt]=<cfOptionName> "=" [optval]=<cfOptionVal> )*
                      ;
'''

@completer_for('alterTableStatement', 'cf')
def alter_table_cf_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_columnfamily_names())

@completer_for('alterInstructions', 'existcol')
def alter_table_col_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    cols = [md.name for md in cfdef.column_metadata]
    if cfdef.key_alias is not None:
        cols.append(cfdef.key_alias)
    return map(maybe_cql_escape, cols)

explain_completion('alterInstructions', 'newcol', '<new_column_name>')

completer_for('alterInstructions', 'optval') \
    (create_cf_option_val_completer)

# END SYNTAX/COMPLETION RULE DEFINITIONS



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

    def __str__(self):
        indexstr = ' (index %s)' % self.index_name if self.index_name is not None else ''
        return '<CqlColumnDef %r %r%s>' % (self.name, self.cqltype, indexstr)
    __repr__ = __str__

class CqlTableDef:
    json_attrs = ('column_aliases', 'compaction_strategy_options', 'compression_parameters')
    composite_type_name = 'org.apache.cassandra.db.marshal.CompositeType'
    colname_type_name = 'org.apache.cassandra.db.marshal.UTF8Type'
    column_class = CqlColumnDef
    compact_storage = False

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
        cf.coldefs = coldefs
        cf.parse_composite()
        cf.check_assumptions()
        return cf

    def check_assumptions(self):
        """
        be explicit about assumptions being made; warn if not met. if some of
        these are accurate but not the others, it's not clear whether the
        right results will come out.
        """

        # assumption is that all valid CQL tables match the rules in the following table.
        # if they don't, give a warning and try anyway, but there should be no expectation
        # of success.
        #
        #                               non-null     non-empty     comparator is    entries in
        #                             value_alias  column_aliases    composite    schema_columns
        #                            +----------------------------------------------------------
        # composite, compact storage |    yes           yes           either           no
        # composite, dynamic storage |    no            yes            yes             yes
        # single-column primary key  |    no            no             no             either

        if self.value_alias is not None:
            # composite cf with compact storage
            if len(self.coldefs) > 0:
                warn(UnexpectedTableStructure(
                        "expected compact storage CF (has value alias) to have no "
                        "column definitions in system.schema_columns, but found %r"
                        % (self.coldefs,)))
            elif len(self.column_aliases) == 0:
                warn(UnexpectedTableStructure(
                        "expected compact storage CF (has value alias) to have "
                        "column aliases, but found none"))
        elif self.comparator.startswith(self.composite_type_name + '('):
            # composite cf with dynamic storage
            if len(self.column_aliases) == 0:
                warn(UnexpectedTableStructure(
                        "expected composite key CF to have column aliases, "
                        "but found none"))
            elif not self.comparator.endswith(self.colname_type_name + ')'):
                warn(UnexpectedTableStructure(
                        "expected non-compact composite CF to have %s as "
                        "last component of composite comparator, but found %r"
                        % (self.colname_type_name, self.comparator)))
            elif len(self.coldefs) == 0:
                warn(UnexpectedTableStructure(
                        "expected non-compact composite CF to have entries in "
                        "system.schema_columns, but found none"))
        else:
            # non-composite cf
            if len(self.column_aliases) > 0:
                warn(UnexpectedTableStructure(
                        "expected non-composite CF to have no column aliases, "
                        "but found %r." % (self.column_aliases,)))
        num_subtypes = self.comparator.count(',') + 1
        if self.compact_storage:
            num_subtypes += 1
        if len(self.key_components) != num_subtypes:
            warn(UnexpectedTableStructure(
                    "expected %r length to be %d, but it's %d. comparator=%r"
                    % (self.key_components, num_subtypes, len(self.key_components), self.comparator)))

    def parse_composite(self):
        subtypes = [self.key_validator]
        if self.comparator.startswith(self.composite_type_name + '('):
            subtypenames = self.comparator[len(self.composite_type_name) + 1:-1]
            subtypes.extend(map(cql_typename, subtypenames.split(',')))
        else:
            subtypes.append(cql_typename(self.comparator))

        value_cols = []
        if len(self.column_aliases) > 0:
            if len(self.coldefs) > 0:
                # composite cf, dynamic storage
                subtypes.pop(-1)
            else:
                # composite cf, compact storage
                self.compact_storage = True
                value_cols = [self.column_class(self.value_alias, self.default_validator)]

        keycols = map(self.column_class, self.key_components, subtypes)
        normal_cols = map(self.column_class.from_layout, self.coldefs)
        self.columns = keycols + value_cols + normal_cols

    def __str__(self):
        return '<%s %s.%s>' % (self.__class__.__name__, self.keyspace, self.name)
    __repr__ = __str__
