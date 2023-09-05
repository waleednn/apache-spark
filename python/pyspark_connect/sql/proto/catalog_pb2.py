#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: spark/connect/catalog.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from pyspark_connect.sql.proto import common_pb2 as spark_dot_connect_dot_common__pb2
from pyspark_connect.sql.proto import types_pb2 as spark_dot_connect_dot_types__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x1bspark/connect/catalog.proto\x12\rspark.connect\x1a\x1aspark/connect/common.proto\x1a\x19spark/connect/types.proto"\xc6\x0e\n\x07\x43\x61talog\x12K\n\x10\x63urrent_database\x18\x01 \x01(\x0b\x32\x1e.spark.connect.CurrentDatabaseH\x00R\x0f\x63urrentDatabase\x12U\n\x14set_current_database\x18\x02 \x01(\x0b\x32!.spark.connect.SetCurrentDatabaseH\x00R\x12setCurrentDatabase\x12\x45\n\x0elist_databases\x18\x03 \x01(\x0b\x32\x1c.spark.connect.ListDatabasesH\x00R\rlistDatabases\x12<\n\x0blist_tables\x18\x04 \x01(\x0b\x32\x19.spark.connect.ListTablesH\x00R\nlistTables\x12\x45\n\x0elist_functions\x18\x05 \x01(\x0b\x32\x1c.spark.connect.ListFunctionsH\x00R\rlistFunctions\x12?\n\x0clist_columns\x18\x06 \x01(\x0b\x32\x1a.spark.connect.ListColumnsH\x00R\x0blistColumns\x12?\n\x0cget_database\x18\x07 \x01(\x0b\x32\x1a.spark.connect.GetDatabaseH\x00R\x0bgetDatabase\x12\x36\n\tget_table\x18\x08 \x01(\x0b\x32\x17.spark.connect.GetTableH\x00R\x08getTable\x12?\n\x0cget_function\x18\t \x01(\x0b\x32\x1a.spark.connect.GetFunctionH\x00R\x0bgetFunction\x12H\n\x0f\x64\x61tabase_exists\x18\n \x01(\x0b\x32\x1d.spark.connect.DatabaseExistsH\x00R\x0e\x64\x61tabaseExists\x12?\n\x0ctable_exists\x18\x0b \x01(\x0b\x32\x1a.spark.connect.TableExistsH\x00R\x0btableExists\x12H\n\x0f\x66unction_exists\x18\x0c \x01(\x0b\x32\x1d.spark.connect.FunctionExistsH\x00R\x0e\x66unctionExists\x12X\n\x15\x63reate_external_table\x18\r \x01(\x0b\x32".spark.connect.CreateExternalTableH\x00R\x13\x63reateExternalTable\x12?\n\x0c\x63reate_table\x18\x0e \x01(\x0b\x32\x1a.spark.connect.CreateTableH\x00R\x0b\x63reateTable\x12\x43\n\x0e\x64rop_temp_view\x18\x0f \x01(\x0b\x32\x1b.spark.connect.DropTempViewH\x00R\x0c\x64ropTempView\x12V\n\x15\x64rop_global_temp_view\x18\x10 \x01(\x0b\x32!.spark.connect.DropGlobalTempViewH\x00R\x12\x64ropGlobalTempView\x12Q\n\x12recover_partitions\x18\x11 \x01(\x0b\x32 .spark.connect.RecoverPartitionsH\x00R\x11recoverPartitions\x12\x36\n\tis_cached\x18\x12 \x01(\x0b\x32\x17.spark.connect.IsCachedH\x00R\x08isCached\x12<\n\x0b\x63\x61\x63he_table\x18\x13 \x01(\x0b\x32\x19.spark.connect.CacheTableH\x00R\ncacheTable\x12\x42\n\runcache_table\x18\x14 \x01(\x0b\x32\x1b.spark.connect.UncacheTableH\x00R\x0cuncacheTable\x12<\n\x0b\x63lear_cache\x18\x15 \x01(\x0b\x32\x19.spark.connect.ClearCacheH\x00R\nclearCache\x12\x42\n\rrefresh_table\x18\x16 \x01(\x0b\x32\x1b.spark.connect.RefreshTableH\x00R\x0crefreshTable\x12\x46\n\x0frefresh_by_path\x18\x17 \x01(\x0b\x32\x1c.spark.connect.RefreshByPathH\x00R\rrefreshByPath\x12H\n\x0f\x63urrent_catalog\x18\x18 \x01(\x0b\x32\x1d.spark.connect.CurrentCatalogH\x00R\x0e\x63urrentCatalog\x12R\n\x13set_current_catalog\x18\x19 \x01(\x0b\x32 .spark.connect.SetCurrentCatalogH\x00R\x11setCurrentCatalog\x12\x42\n\rlist_catalogs\x18\x1a \x01(\x0b\x32\x1b.spark.connect.ListCatalogsH\x00R\x0clistCatalogsB\n\n\x08\x63\x61t_type"\x11\n\x0f\x43urrentDatabase"-\n\x12SetCurrentDatabase\x12\x17\n\x07\x64\x62_name\x18\x01 \x01(\tR\x06\x64\x62Name":\n\rListDatabases\x12\x1d\n\x07pattern\x18\x01 \x01(\tH\x00R\x07pattern\x88\x01\x01\x42\n\n\x08_pattern"a\n\nListTables\x12\x1c\n\x07\x64\x62_name\x18\x01 \x01(\tH\x00R\x06\x64\x62Name\x88\x01\x01\x12\x1d\n\x07pattern\x18\x02 \x01(\tH\x01R\x07pattern\x88\x01\x01\x42\n\n\x08_db_nameB\n\n\x08_pattern"d\n\rListFunctions\x12\x1c\n\x07\x64\x62_name\x18\x01 \x01(\tH\x00R\x06\x64\x62Name\x88\x01\x01\x12\x1d\n\x07pattern\x18\x02 \x01(\tH\x01R\x07pattern\x88\x01\x01\x42\n\n\x08_db_nameB\n\n\x08_pattern"V\n\x0bListColumns\x12\x1d\n\ntable_name\x18\x01 \x01(\tR\ttableName\x12\x1c\n\x07\x64\x62_name\x18\x02 \x01(\tH\x00R\x06\x64\x62Name\x88\x01\x01\x42\n\n\x08_db_name"&\n\x0bGetDatabase\x12\x17\n\x07\x64\x62_name\x18\x01 \x01(\tR\x06\x64\x62Name"S\n\x08GetTable\x12\x1d\n\ntable_name\x18\x01 \x01(\tR\ttableName\x12\x1c\n\x07\x64\x62_name\x18\x02 \x01(\tH\x00R\x06\x64\x62Name\x88\x01\x01\x42\n\n\x08_db_name"\\\n\x0bGetFunction\x12#\n\rfunction_name\x18\x01 \x01(\tR\x0c\x66unctionName\x12\x1c\n\x07\x64\x62_name\x18\x02 \x01(\tH\x00R\x06\x64\x62Name\x88\x01\x01\x42\n\n\x08_db_name")\n\x0e\x44\x61tabaseExists\x12\x17\n\x07\x64\x62_name\x18\x01 \x01(\tR\x06\x64\x62Name"V\n\x0bTableExists\x12\x1d\n\ntable_name\x18\x01 \x01(\tR\ttableName\x12\x1c\n\x07\x64\x62_name\x18\x02 \x01(\tH\x00R\x06\x64\x62Name\x88\x01\x01\x42\n\n\x08_db_name"_\n\x0e\x46unctionExists\x12#\n\rfunction_name\x18\x01 \x01(\tR\x0c\x66unctionName\x12\x1c\n\x07\x64\x62_name\x18\x02 \x01(\tH\x00R\x06\x64\x62Name\x88\x01\x01\x42\n\n\x08_db_name"\xc6\x02\n\x13\x43reateExternalTable\x12\x1d\n\ntable_name\x18\x01 \x01(\tR\ttableName\x12\x17\n\x04path\x18\x02 \x01(\tH\x00R\x04path\x88\x01\x01\x12\x1b\n\x06source\x18\x03 \x01(\tH\x01R\x06source\x88\x01\x01\x12\x34\n\x06schema\x18\x04 \x01(\x0b\x32\x17.spark.connect.DataTypeH\x02R\x06schema\x88\x01\x01\x12I\n\x07options\x18\x05 \x03(\x0b\x32/.spark.connect.CreateExternalTable.OptionsEntryR\x07options\x1a:\n\x0cOptionsEntry\x12\x10\n\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n\x05value\x18\x02 \x01(\tR\x05value:\x02\x38\x01\x42\x07\n\x05_pathB\t\n\x07_sourceB\t\n\x07_schema"\xed\x02\n\x0b\x43reateTable\x12\x1d\n\ntable_name\x18\x01 \x01(\tR\ttableName\x12\x17\n\x04path\x18\x02 \x01(\tH\x00R\x04path\x88\x01\x01\x12\x1b\n\x06source\x18\x03 \x01(\tH\x01R\x06source\x88\x01\x01\x12%\n\x0b\x64\x65scription\x18\x04 \x01(\tH\x02R\x0b\x64\x65scription\x88\x01\x01\x12\x34\n\x06schema\x18\x05 \x01(\x0b\x32\x17.spark.connect.DataTypeH\x03R\x06schema\x88\x01\x01\x12\x41\n\x07options\x18\x06 \x03(\x0b\x32\'.spark.connect.CreateTable.OptionsEntryR\x07options\x1a:\n\x0cOptionsEntry\x12\x10\n\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n\x05value\x18\x02 \x01(\tR\x05value:\x02\x38\x01\x42\x07\n\x05_pathB\t\n\x07_sourceB\x0e\n\x0c_descriptionB\t\n\x07_schema"+\n\x0c\x44ropTempView\x12\x1b\n\tview_name\x18\x01 \x01(\tR\x08viewName"1\n\x12\x44ropGlobalTempView\x12\x1b\n\tview_name\x18\x01 \x01(\tR\x08viewName"2\n\x11RecoverPartitions\x12\x1d\n\ntable_name\x18\x01 \x01(\tR\ttableName")\n\x08IsCached\x12\x1d\n\ntable_name\x18\x01 \x01(\tR\ttableName"\x84\x01\n\nCacheTable\x12\x1d\n\ntable_name\x18\x01 \x01(\tR\ttableName\x12\x45\n\rstorage_level\x18\x02 \x01(\x0b\x32\x1b.spark.connect.StorageLevelH\x00R\x0cstorageLevel\x88\x01\x01\x42\x10\n\x0e_storage_level"-\n\x0cUncacheTable\x12\x1d\n\ntable_name\x18\x01 \x01(\tR\ttableName"\x0c\n\nClearCache"-\n\x0cRefreshTable\x12\x1d\n\ntable_name\x18\x01 \x01(\tR\ttableName"#\n\rRefreshByPath\x12\x12\n\x04path\x18\x01 \x01(\tR\x04path"\x10\n\x0e\x43urrentCatalog"6\n\x11SetCurrentCatalog\x12!\n\x0c\x63\x61talog_name\x18\x01 \x01(\tR\x0b\x63\x61talogName"9\n\x0cListCatalogs\x12\x1d\n\x07pattern\x18\x01 \x01(\tH\x00R\x07pattern\x88\x01\x01\x42\n\n\x08_patternB6\n\x1eorg.apache.spark.connect.protoP\x01Z\x12internal/generatedb\x06proto3'
)

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "spark.connect.catalog_pb2", globals())
if _descriptor._USE_C_DESCRIPTORS == False:

    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = (
        b"\n\036org.apache.spark.connect.protoP\001Z\022internal/generated"
    )
    _CREATEEXTERNALTABLE_OPTIONSENTRY._options = None
    _CREATEEXTERNALTABLE_OPTIONSENTRY._serialized_options = b"8\001"
    _CREATETABLE_OPTIONSENTRY._options = None
    _CREATETABLE_OPTIONSENTRY._serialized_options = b"8\001"
    _CATALOG._serialized_start = 102
    _CATALOG._serialized_end = 1964
    _CURRENTDATABASE._serialized_start = 1966
    _CURRENTDATABASE._serialized_end = 1983
    _SETCURRENTDATABASE._serialized_start = 1985
    _SETCURRENTDATABASE._serialized_end = 2030
    _LISTDATABASES._serialized_start = 2032
    _LISTDATABASES._serialized_end = 2090
    _LISTTABLES._serialized_start = 2092
    _LISTTABLES._serialized_end = 2189
    _LISTFUNCTIONS._serialized_start = 2191
    _LISTFUNCTIONS._serialized_end = 2291
    _LISTCOLUMNS._serialized_start = 2293
    _LISTCOLUMNS._serialized_end = 2379
    _GETDATABASE._serialized_start = 2381
    _GETDATABASE._serialized_end = 2419
    _GETTABLE._serialized_start = 2421
    _GETTABLE._serialized_end = 2504
    _GETFUNCTION._serialized_start = 2506
    _GETFUNCTION._serialized_end = 2598
    _DATABASEEXISTS._serialized_start = 2600
    _DATABASEEXISTS._serialized_end = 2641
    _TABLEEXISTS._serialized_start = 2643
    _TABLEEXISTS._serialized_end = 2729
    _FUNCTIONEXISTS._serialized_start = 2731
    _FUNCTIONEXISTS._serialized_end = 2826
    _CREATEEXTERNALTABLE._serialized_start = 2829
    _CREATEEXTERNALTABLE._serialized_end = 3155
    _CREATEEXTERNALTABLE_OPTIONSENTRY._serialized_start = 3066
    _CREATEEXTERNALTABLE_OPTIONSENTRY._serialized_end = 3124
    _CREATETABLE._serialized_start = 3158
    _CREATETABLE._serialized_end = 3523
    _CREATETABLE_OPTIONSENTRY._serialized_start = 3066
    _CREATETABLE_OPTIONSENTRY._serialized_end = 3124
    _DROPTEMPVIEW._serialized_start = 3525
    _DROPTEMPVIEW._serialized_end = 3568
    _DROPGLOBALTEMPVIEW._serialized_start = 3570
    _DROPGLOBALTEMPVIEW._serialized_end = 3619
    _RECOVERPARTITIONS._serialized_start = 3621
    _RECOVERPARTITIONS._serialized_end = 3671
    _ISCACHED._serialized_start = 3673
    _ISCACHED._serialized_end = 3714
    _CACHETABLE._serialized_start = 3717
    _CACHETABLE._serialized_end = 3849
    _UNCACHETABLE._serialized_start = 3851
    _UNCACHETABLE._serialized_end = 3896
    _CLEARCACHE._serialized_start = 3898
    _CLEARCACHE._serialized_end = 3910
    _REFRESHTABLE._serialized_start = 3912
    _REFRESHTABLE._serialized_end = 3957
    _REFRESHBYPATH._serialized_start = 3959
    _REFRESHBYPATH._serialized_end = 3994
    _CURRENTCATALOG._serialized_start = 3996
    _CURRENTCATALOG._serialized_end = 4012
    _SETCURRENTCATALOG._serialized_start = 4014
    _SETCURRENTCATALOG._serialized_end = 4068
    _LISTCATALOGS._serialized_start = 4070
    _LISTCATALOGS._serialized_end = 4127
# @@protoc_insertion_point(module_scope)
