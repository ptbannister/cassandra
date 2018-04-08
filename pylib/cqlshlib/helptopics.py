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


class CQL3HelpTopics(object):
    def get_help_topics(self):
        return [t[5:] for t in dir(self) if t.startswith('help_')]

    def get_help_topic(self, topic):
        return getattr(self, 'help_' + topic.lower())()

    def help_types(self):
        return 'types.html'

    def help_timestamp(self):
        return 'types.html#working-with-timestamps'

    def help_date(self):
        return 'types.html#working-with-dates'

    def help_time(self):
        return 'types.html#working-with-times'

    def help_duration(self):
        return 'types.html#working-with-durations'

    def help_map(self):
        return 'types.html#maps'

    def help_set(self):
        return 'types.html#sets'

    def help_list(self):
        return 'types.html#lists'

    def help_blob(self):
        return 'definitions.html#constants'

    def help_uuid(self):
        return 'definitions.html#constants'

    def help_boolean(self):
        return 'definitions.html#constants'

    def help_int(self):
        return 'definitions.html#constants'

    def help_counter(self):
        return 'types.html#counters'

    def help_text(self):
        return 'definitions.html#constants'
    help_ascii = help_text

    def help_tuple(self):
        return 'types.html#tuples'

    def help_use(self):
        return 'ddl.html#use'

    def help_insert(self):
        return 'dml.html#insert'

    def help_update(self):
        return 'dml.html#update'

    def help_delete(self):
        return 'dml.html#delete'

    def help_select(self):
        return 'dml.html#select'

    def help_json(self):
        return 'json.html'

    def help_select_json(self):
        return 'json.html#select-json'

    def help_insert_json(self):
        return 'json.html#insert-json'

    def help_batch(self):
        return 'dml.html#batch'
    help_begin = help_batch
    help_apply = help_batch

    def help_create_keyspace(self):
        return 'ddl.html#create-keyspace'

    def help_alter_keyspace(self):
        return 'ddl.html#alter-keyspace'

    def help_drop_keyspace(self):
        return 'ddl.html#drop-keyspace'

    def help_create_table(self):
        return 'ddl.html#create-table'
    help_create_columnfamily = help_create_table

    def help_alter_table(self):
        return 'ddl.html#alter-table'

    def help_drop_table(self):
        return 'ddl.html#drop-table'
    help_drop_columnfamily = help_drop_table

    def help_create_index(self):
        return 'indexes.html#create-index'

    def help_drop_index(self):
        return 'indexes.html#drop-index'

    def help_truncate(self):
        return 'ddl.html#truncate'

    def help_create_type(self):
        return 'types.html#creating-a-udt'

    def help_alter_type(self):
        return 'types.html#altering-a-udt'

    def help_drop_type(self):
        return 'types.html#dropping-a-udt'

    def help_create_function(self):
        return 'functions.html#create-function'

    def help_drop_function(self):
        return 'functions.html#drop-function'

    def help_functions(self):
        return 'functions.html'

    def help_create_aggregate(self):
        return 'functions.html#create-aggregate'

    def help_drop_aggregate(self):
        return 'functions.html#drop-aggregate'

    def help_aggregates(self):
        return 'functions.html#aggregate-functions'

    def help_create_trigger(self):
        return 'triggers.html#create-trigger'

    def help_drop_trigger(self):
        return 'triggers.html#drop-trigger'

    def help_create_materialized_view(self):
        return 'mvs.html#create-materialized-view'

    def help_alter_materialized_view(self):
        return 'mvs.html#alter-materialized-view'

    def help_drop_materialized_view(self):
        return 'mvs.html#drop-materialized-view'

    def help_keywords(self):
        return 'appendices.html#appendix-a-cql-keywords'

    def help_create_user(self):
        return 'security.html#create-user'

    def help_alter_user(self):
        return 'security.html#alter-user'

    def help_drop_user(self):
        return 'security.html#drop-user'

    def help_list_users(self):
        return 'security.html#list-users'

    def help_create_role(self):
        return 'security.html#create-role'

    def help_alter_role(self):
        return 'security.html#alter-role'

    def help_drop_role(self):
        return 'security.html#drop-role'

    def help_grant_role(self):
        return 'security.html#grant-role'

    def help_revoke_role(self):
        return 'security.html#revoke-role'

    def help_list_roles(self):
        return 'security.html#list-roles'

    def help_permissions(self):
        return 'security.html#permissions'

    def help_list_permissions(self):
        return 'security.html#list-permissions'

    def help_grant_permission(self):
        return 'security.html#grant-permission'

    def help_revoke_permission(self):
        return 'security.html#revoke-permission'
