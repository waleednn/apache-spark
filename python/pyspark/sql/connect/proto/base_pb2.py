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
# source: spark/connect/base.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import any_pb2 as google_dot_protobuf_dot_any__pb2
from pyspark.sql.connect.proto import commands_pb2 as spark_dot_connect_dot_commands__pb2
from pyspark.sql.connect.proto import common_pb2 as spark_dot_connect_dot_common__pb2
from pyspark.sql.connect.proto import expressions_pb2 as spark_dot_connect_dot_expressions__pb2
from pyspark.sql.connect.proto import relations_pb2 as spark_dot_connect_dot_relations__pb2
from pyspark.sql.connect.proto import types_pb2 as spark_dot_connect_dot_types__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x18spark/connect/base.proto\x12\rspark.connect\x1a\x19google/protobuf/any.proto\x1a\x1cspark/connect/commands.proto\x1a\x1aspark/connect/common.proto\x1a\x1fspark/connect/expressions.proto\x1a\x1dspark/connect/relations.proto\x1a\x19spark/connect/types.proto"t\n\x04Plan\x12-\n\x04root\x18\x01 \x01(\x0b\x32\x17.spark.connect.RelationH\x00R\x04root\x12\x32\n\x07\x63ommand\x18\x02 \x01(\x0b\x32\x16.spark.connect.CommandH\x00R\x07\x63ommandB\t\n\x07op_type"z\n\x0bUserContext\x12\x17\n\x07user_id\x18\x01 \x01(\tR\x06userId\x12\x1b\n\tuser_name\x18\x02 \x01(\tR\x08userName\x12\x35\n\nextensions\x18\xe7\x07 \x03(\x0b\x32\x14.google.protobuf.AnyR\nextensions"\xf5\x12\n\x12\x41nalyzePlanRequest\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12=\n\x0cuser_context\x18\x02 \x01(\x0b\x32\x1a.spark.connect.UserContextR\x0buserContext\x12$\n\x0b\x63lient_type\x18\x03 \x01(\tH\x01R\nclientType\x88\x01\x01\x12\x42\n\x06schema\x18\x04 \x01(\x0b\x32(.spark.connect.AnalyzePlanRequest.SchemaH\x00R\x06schema\x12\x45\n\x07\x65xplain\x18\x05 \x01(\x0b\x32).spark.connect.AnalyzePlanRequest.ExplainH\x00R\x07\x65xplain\x12O\n\x0btree_string\x18\x06 \x01(\x0b\x32,.spark.connect.AnalyzePlanRequest.TreeStringH\x00R\ntreeString\x12\x46\n\x08is_local\x18\x07 \x01(\x0b\x32).spark.connect.AnalyzePlanRequest.IsLocalH\x00R\x07isLocal\x12R\n\x0cis_streaming\x18\x08 \x01(\x0b\x32-.spark.connect.AnalyzePlanRequest.IsStreamingH\x00R\x0bisStreaming\x12O\n\x0binput_files\x18\t \x01(\x0b\x32,.spark.connect.AnalyzePlanRequest.InputFilesH\x00R\ninputFiles\x12U\n\rspark_version\x18\n \x01(\x0b\x32..spark.connect.AnalyzePlanRequest.SparkVersionH\x00R\x0csparkVersion\x12I\n\tddl_parse\x18\x0b \x01(\x0b\x32*.spark.connect.AnalyzePlanRequest.DDLParseH\x00R\x08\x64\x64lParse\x12X\n\x0esame_semantics\x18\x0c \x01(\x0b\x32/.spark.connect.AnalyzePlanRequest.SameSemanticsH\x00R\rsameSemantics\x12U\n\rsemantic_hash\x18\r \x01(\x0b\x32..spark.connect.AnalyzePlanRequest.SemanticHashH\x00R\x0csemanticHash\x12\x45\n\x07persist\x18\x0e \x01(\x0b\x32).spark.connect.AnalyzePlanRequest.PersistH\x00R\x07persist\x12K\n\tunpersist\x18\x0f \x01(\x0b\x32+.spark.connect.AnalyzePlanRequest.UnpersistH\x00R\tunpersist\x12_\n\x11get_storage_level\x18\x10 \x01(\x0b\x32\x31.spark.connect.AnalyzePlanRequest.GetStorageLevelH\x00R\x0fgetStorageLevel\x1a\x31\n\x06Schema\x12\'\n\x04plan\x18\x01 \x01(\x0b\x32\x13.spark.connect.PlanR\x04plan\x1a\xbb\x02\n\x07\x45xplain\x12\'\n\x04plan\x18\x01 \x01(\x0b\x32\x13.spark.connect.PlanR\x04plan\x12X\n\x0c\x65xplain_mode\x18\x02 \x01(\x0e\x32\x35.spark.connect.AnalyzePlanRequest.Explain.ExplainModeR\x0b\x65xplainMode"\xac\x01\n\x0b\x45xplainMode\x12\x1c\n\x18\x45XPLAIN_MODE_UNSPECIFIED\x10\x00\x12\x17\n\x13\x45XPLAIN_MODE_SIMPLE\x10\x01\x12\x19\n\x15\x45XPLAIN_MODE_EXTENDED\x10\x02\x12\x18\n\x14\x45XPLAIN_MODE_CODEGEN\x10\x03\x12\x15\n\x11\x45XPLAIN_MODE_COST\x10\x04\x12\x1a\n\x16\x45XPLAIN_MODE_FORMATTED\x10\x05\x1aZ\n\nTreeString\x12\'\n\x04plan\x18\x01 \x01(\x0b\x32\x13.spark.connect.PlanR\x04plan\x12\x19\n\x05level\x18\x02 \x01(\x05H\x00R\x05level\x88\x01\x01\x42\x08\n\x06_level\x1a\x32\n\x07IsLocal\x12\'\n\x04plan\x18\x01 \x01(\x0b\x32\x13.spark.connect.PlanR\x04plan\x1a\x36\n\x0bIsStreaming\x12\'\n\x04plan\x18\x01 \x01(\x0b\x32\x13.spark.connect.PlanR\x04plan\x1a\x35\n\nInputFiles\x12\'\n\x04plan\x18\x01 \x01(\x0b\x32\x13.spark.connect.PlanR\x04plan\x1a\x0e\n\x0cSparkVersion\x1a)\n\x08\x44\x44LParse\x12\x1d\n\nddl_string\x18\x01 \x01(\tR\tddlString\x1ay\n\rSameSemantics\x12\x34\n\x0btarget_plan\x18\x01 \x01(\x0b\x32\x13.spark.connect.PlanR\ntargetPlan\x12\x32\n\nother_plan\x18\x02 \x01(\x0b\x32\x13.spark.connect.PlanR\totherPlan\x1a\x37\n\x0cSemanticHash\x12\'\n\x04plan\x18\x01 \x01(\x0b\x32\x13.spark.connect.PlanR\x04plan\x1a\x97\x01\n\x07Persist\x12\x33\n\x08relation\x18\x01 \x01(\x0b\x32\x17.spark.connect.RelationR\x08relation\x12\x45\n\rstorage_level\x18\x02 \x01(\x0b\x32\x1b.spark.connect.StorageLevelH\x00R\x0cstorageLevel\x88\x01\x01\x42\x10\n\x0e_storage_level\x1an\n\tUnpersist\x12\x33\n\x08relation\x18\x01 \x01(\x0b\x32\x17.spark.connect.RelationR\x08relation\x12\x1f\n\x08\x62locking\x18\x02 \x01(\x08H\x00R\x08\x62locking\x88\x01\x01\x42\x0b\n\t_blocking\x1a\x46\n\x0fGetStorageLevel\x12\x33\n\x08relation\x18\x01 \x01(\x0b\x32\x17.spark.connect.RelationR\x08relationB\t\n\x07\x61nalyzeB\x0e\n\x0c_client_type"\x99\r\n\x13\x41nalyzePlanResponse\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12\x43\n\x06schema\x18\x02 \x01(\x0b\x32).spark.connect.AnalyzePlanResponse.SchemaH\x00R\x06schema\x12\x46\n\x07\x65xplain\x18\x03 \x01(\x0b\x32*.spark.connect.AnalyzePlanResponse.ExplainH\x00R\x07\x65xplain\x12P\n\x0btree_string\x18\x04 \x01(\x0b\x32-.spark.connect.AnalyzePlanResponse.TreeStringH\x00R\ntreeString\x12G\n\x08is_local\x18\x05 \x01(\x0b\x32*.spark.connect.AnalyzePlanResponse.IsLocalH\x00R\x07isLocal\x12S\n\x0cis_streaming\x18\x06 \x01(\x0b\x32..spark.connect.AnalyzePlanResponse.IsStreamingH\x00R\x0bisStreaming\x12P\n\x0binput_files\x18\x07 \x01(\x0b\x32-.spark.connect.AnalyzePlanResponse.InputFilesH\x00R\ninputFiles\x12V\n\rspark_version\x18\x08 \x01(\x0b\x32/.spark.connect.AnalyzePlanResponse.SparkVersionH\x00R\x0csparkVersion\x12J\n\tddl_parse\x18\t \x01(\x0b\x32+.spark.connect.AnalyzePlanResponse.DDLParseH\x00R\x08\x64\x64lParse\x12Y\n\x0esame_semantics\x18\n \x01(\x0b\x32\x30.spark.connect.AnalyzePlanResponse.SameSemanticsH\x00R\rsameSemantics\x12V\n\rsemantic_hash\x18\x0b \x01(\x0b\x32/.spark.connect.AnalyzePlanResponse.SemanticHashH\x00R\x0csemanticHash\x12\x46\n\x07persist\x18\x0c \x01(\x0b\x32*.spark.connect.AnalyzePlanResponse.PersistH\x00R\x07persist\x12L\n\tunpersist\x18\r \x01(\x0b\x32,.spark.connect.AnalyzePlanResponse.UnpersistH\x00R\tunpersist\x12`\n\x11get_storage_level\x18\x0e \x01(\x0b\x32\x32.spark.connect.AnalyzePlanResponse.GetStorageLevelH\x00R\x0fgetStorageLevel\x1a\x39\n\x06Schema\x12/\n\x06schema\x18\x01 \x01(\x0b\x32\x17.spark.connect.DataTypeR\x06schema\x1a\x30\n\x07\x45xplain\x12%\n\x0e\x65xplain_string\x18\x01 \x01(\tR\rexplainString\x1a-\n\nTreeString\x12\x1f\n\x0btree_string\x18\x01 \x01(\tR\ntreeString\x1a$\n\x07IsLocal\x12\x19\n\x08is_local\x18\x01 \x01(\x08R\x07isLocal\x1a\x30\n\x0bIsStreaming\x12!\n\x0cis_streaming\x18\x01 \x01(\x08R\x0bisStreaming\x1a"\n\nInputFiles\x12\x14\n\x05\x66iles\x18\x01 \x03(\tR\x05\x66iles\x1a(\n\x0cSparkVersion\x12\x18\n\x07version\x18\x01 \x01(\tR\x07version\x1a;\n\x08\x44\x44LParse\x12/\n\x06parsed\x18\x01 \x01(\x0b\x32\x17.spark.connect.DataTypeR\x06parsed\x1a\'\n\rSameSemantics\x12\x16\n\x06result\x18\x01 \x01(\x08R\x06result\x1a&\n\x0cSemanticHash\x12\x16\n\x06result\x18\x01 \x01(\x05R\x06result\x1a\t\n\x07Persist\x1a\x0b\n\tUnpersist\x1aS\n\x0fGetStorageLevel\x12@\n\rstorage_level\x18\x01 \x01(\x0b\x32\x1b.spark.connect.StorageLevelR\x0cstorageLevelB\x08\n\x06result"\xa0\x04\n\x12\x45xecutePlanRequest\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12=\n\x0cuser_context\x18\x02 \x01(\x0b\x32\x1a.spark.connect.UserContextR\x0buserContext\x12&\n\x0coperation_id\x18\x06 \x01(\tH\x00R\x0boperationId\x88\x01\x01\x12\'\n\x04plan\x18\x03 \x01(\x0b\x32\x13.spark.connect.PlanR\x04plan\x12$\n\x0b\x63lient_type\x18\x04 \x01(\tH\x01R\nclientType\x88\x01\x01\x12X\n\x0frequest_options\x18\x05 \x03(\x0b\x32/.spark.connect.ExecutePlanRequest.RequestOptionR\x0erequestOptions\x12\x12\n\x04tags\x18\x07 \x03(\tR\x04tags\x1a\xa5\x01\n\rRequestOption\x12K\n\x10reattach_options\x18\x01 \x01(\x0b\x32\x1e.spark.connect.ReattachOptionsH\x00R\x0freattachOptions\x12\x35\n\textension\x18\xe7\x07 \x01(\x0b\x32\x14.google.protobuf.AnyH\x00R\textensionB\x10\n\x0erequest_optionB\x0f\n\r_operation_idB\x0e\n\x0c_client_type"\x99\x0f\n\x13\x45xecutePlanResponse\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12!\n\x0coperation_id\x18\x0c \x01(\tR\x0boperationId\x12\x1f\n\x0bresponse_id\x18\r \x01(\tR\nresponseId\x12P\n\x0b\x61rrow_batch\x18\x02 \x01(\x0b\x32-.spark.connect.ExecutePlanResponse.ArrowBatchH\x00R\narrowBatch\x12\x63\n\x12sql_command_result\x18\x05 \x01(\x0b\x32\x33.spark.connect.ExecutePlanResponse.SqlCommandResultH\x00R\x10sqlCommandResult\x12~\n#write_stream_operation_start_result\x18\x08 \x01(\x0b\x32..spark.connect.WriteStreamOperationStartResultH\x00R\x1fwriteStreamOperationStartResult\x12q\n\x1estreaming_query_command_result\x18\t \x01(\x0b\x32*.spark.connect.StreamingQueryCommandResultH\x00R\x1bstreamingQueryCommandResult\x12k\n\x1cget_resources_command_result\x18\n \x01(\x0b\x32(.spark.connect.GetResourcesCommandResultH\x00R\x19getResourcesCommandResult\x12\x87\x01\n&streaming_query_manager_command_result\x18\x0b \x01(\x0b\x32\x31.spark.connect.StreamingQueryManagerCommandResultH\x00R"streamingQueryManagerCommandResult\x12\\\n\x0fresult_complete\x18\x0e \x01(\x0b\x32\x31.spark.connect.ExecutePlanResponse.ResultCompleteH\x00R\x0eresultComplete\x12\x35\n\textension\x18\xe7\x07 \x01(\x0b\x32\x14.google.protobuf.AnyH\x00R\textension\x12\x44\n\x07metrics\x18\x04 \x01(\x0b\x32*.spark.connect.ExecutePlanResponse.MetricsR\x07metrics\x12]\n\x10observed_metrics\x18\x06 \x03(\x0b\x32\x32.spark.connect.ExecutePlanResponse.ObservedMetricsR\x0fobservedMetrics\x12/\n\x06schema\x18\x07 \x01(\x0b\x32\x17.spark.connect.DataTypeR\x06schema\x1aG\n\x10SqlCommandResult\x12\x33\n\x08relation\x18\x01 \x01(\x0b\x32\x17.spark.connect.RelationR\x08relation\x1a=\n\nArrowBatch\x12\x1b\n\trow_count\x18\x01 \x01(\x03R\x08rowCount\x12\x12\n\x04\x64\x61ta\x18\x02 \x01(\x0cR\x04\x64\x61ta\x1a\x85\x04\n\x07Metrics\x12Q\n\x07metrics\x18\x01 \x03(\x0b\x32\x37.spark.connect.ExecutePlanResponse.Metrics.MetricObjectR\x07metrics\x1a\xcc\x02\n\x0cMetricObject\x12\x12\n\x04name\x18\x01 \x01(\tR\x04name\x12\x17\n\x07plan_id\x18\x02 \x01(\x03R\x06planId\x12\x16\n\x06parent\x18\x03 \x01(\x03R\x06parent\x12z\n\x11\x65xecution_metrics\x18\x04 \x03(\x0b\x32M.spark.connect.ExecutePlanResponse.Metrics.MetricObject.ExecutionMetricsEntryR\x10\x65xecutionMetrics\x1a{\n\x15\x45xecutionMetricsEntry\x12\x10\n\x03key\x18\x01 \x01(\tR\x03key\x12L\n\x05value\x18\x02 \x01(\x0b\x32\x36.spark.connect.ExecutePlanResponse.Metrics.MetricValueR\x05value:\x02\x38\x01\x1aX\n\x0bMetricValue\x12\x12\n\x04name\x18\x01 \x01(\tR\x04name\x12\x14\n\x05value\x18\x02 \x01(\x03R\x05value\x12\x1f\n\x0bmetric_type\x18\x03 \x01(\tR\nmetricType\x1a`\n\x0fObservedMetrics\x12\x12\n\x04name\x18\x01 \x01(\tR\x04name\x12\x39\n\x06values\x18\x02 \x03(\x0b\x32!.spark.connect.Expression.LiteralR\x06values\x1a\x10\n\x0eResultCompleteB\x0f\n\rresponse_type"A\n\x08KeyValue\x12\x10\n\x03key\x18\x01 \x01(\tR\x03key\x12\x19\n\x05value\x18\x02 \x01(\tH\x00R\x05value\x88\x01\x01\x42\x08\n\x06_value"\x84\x08\n\rConfigRequest\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12=\n\x0cuser_context\x18\x02 \x01(\x0b\x32\x1a.spark.connect.UserContextR\x0buserContext\x12\x44\n\toperation\x18\x03 \x01(\x0b\x32&.spark.connect.ConfigRequest.OperationR\toperation\x12$\n\x0b\x63lient_type\x18\x04 \x01(\tH\x00R\nclientType\x88\x01\x01\x1a\xf2\x03\n\tOperation\x12\x34\n\x03set\x18\x01 \x01(\x0b\x32 .spark.connect.ConfigRequest.SetH\x00R\x03set\x12\x34\n\x03get\x18\x02 \x01(\x0b\x32 .spark.connect.ConfigRequest.GetH\x00R\x03get\x12W\n\x10get_with_default\x18\x03 \x01(\x0b\x32+.spark.connect.ConfigRequest.GetWithDefaultH\x00R\x0egetWithDefault\x12G\n\nget_option\x18\x04 \x01(\x0b\x32&.spark.connect.ConfigRequest.GetOptionH\x00R\tgetOption\x12>\n\x07get_all\x18\x05 \x01(\x0b\x32#.spark.connect.ConfigRequest.GetAllH\x00R\x06getAll\x12:\n\x05unset\x18\x06 \x01(\x0b\x32".spark.connect.ConfigRequest.UnsetH\x00R\x05unset\x12P\n\ris_modifiable\x18\x07 \x01(\x0b\x32).spark.connect.ConfigRequest.IsModifiableH\x00R\x0cisModifiableB\t\n\x07op_type\x1a\x34\n\x03Set\x12-\n\x05pairs\x18\x01 \x03(\x0b\x32\x17.spark.connect.KeyValueR\x05pairs\x1a\x19\n\x03Get\x12\x12\n\x04keys\x18\x01 \x03(\tR\x04keys\x1a?\n\x0eGetWithDefault\x12-\n\x05pairs\x18\x01 \x03(\x0b\x32\x17.spark.connect.KeyValueR\x05pairs\x1a\x1f\n\tGetOption\x12\x12\n\x04keys\x18\x01 \x03(\tR\x04keys\x1a\x30\n\x06GetAll\x12\x1b\n\x06prefix\x18\x01 \x01(\tH\x00R\x06prefix\x88\x01\x01\x42\t\n\x07_prefix\x1a\x1b\n\x05Unset\x12\x12\n\x04keys\x18\x01 \x03(\tR\x04keys\x1a"\n\x0cIsModifiable\x12\x12\n\x04keys\x18\x01 \x03(\tR\x04keysB\x0e\n\x0c_client_type"z\n\x0e\x43onfigResponse\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12-\n\x05pairs\x18\x02 \x03(\x0b\x32\x17.spark.connect.KeyValueR\x05pairs\x12\x1a\n\x08warnings\x18\x03 \x03(\tR\x08warnings"\xe7\x06\n\x13\x41\x64\x64\x41rtifactsRequest\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12=\n\x0cuser_context\x18\x02 \x01(\x0b\x32\x1a.spark.connect.UserContextR\x0buserContext\x12$\n\x0b\x63lient_type\x18\x06 \x01(\tH\x01R\nclientType\x88\x01\x01\x12@\n\x05\x62\x61tch\x18\x03 \x01(\x0b\x32(.spark.connect.AddArtifactsRequest.BatchH\x00R\x05\x62\x61tch\x12Z\n\x0b\x62\x65gin_chunk\x18\x04 \x01(\x0b\x32\x37.spark.connect.AddArtifactsRequest.BeginChunkedArtifactH\x00R\nbeginChunk\x12H\n\x05\x63hunk\x18\x05 \x01(\x0b\x32\x30.spark.connect.AddArtifactsRequest.ArtifactChunkH\x00R\x05\x63hunk\x1a\x35\n\rArtifactChunk\x12\x12\n\x04\x64\x61ta\x18\x01 \x01(\x0cR\x04\x64\x61ta\x12\x10\n\x03\x63rc\x18\x02 \x01(\x03R\x03\x63rc\x1ao\n\x13SingleChunkArtifact\x12\x12\n\x04name\x18\x01 \x01(\tR\x04name\x12\x44\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x30.spark.connect.AddArtifactsRequest.ArtifactChunkR\x04\x64\x61ta\x1a]\n\x05\x42\x61tch\x12T\n\tartifacts\x18\x01 \x03(\x0b\x32\x36.spark.connect.AddArtifactsRequest.SingleChunkArtifactR\tartifacts\x1a\xc1\x01\n\x14\x42\x65ginChunkedArtifact\x12\x12\n\x04name\x18\x01 \x01(\tR\x04name\x12\x1f\n\x0btotal_bytes\x18\x02 \x01(\x03R\ntotalBytes\x12\x1d\n\nnum_chunks\x18\x03 \x01(\x03R\tnumChunks\x12U\n\rinitial_chunk\x18\x04 \x01(\x0b\x32\x30.spark.connect.AddArtifactsRequest.ArtifactChunkR\x0cinitialChunkB\t\n\x07payloadB\x0e\n\x0c_client_type"\xbc\x01\n\x14\x41\x64\x64\x41rtifactsResponse\x12Q\n\tartifacts\x18\x01 \x03(\x0b\x32\x33.spark.connect.AddArtifactsResponse.ArtifactSummaryR\tartifacts\x1aQ\n\x0f\x41rtifactSummary\x12\x12\n\x04name\x18\x01 \x01(\tR\x04name\x12*\n\x11is_crc_successful\x18\x02 \x01(\x08R\x0fisCrcSuccessful"\xc3\x01\n\x17\x41rtifactStatusesRequest\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12=\n\x0cuser_context\x18\x02 \x01(\x0b\x32\x1a.spark.connect.UserContextR\x0buserContext\x12$\n\x0b\x63lient_type\x18\x03 \x01(\tH\x00R\nclientType\x88\x01\x01\x12\x14\n\x05names\x18\x04 \x03(\tR\x05namesB\x0e\n\x0c_client_type"\x8c\x02\n\x18\x41rtifactStatusesResponse\x12Q\n\x08statuses\x18\x01 \x03(\x0b\x32\x35.spark.connect.ArtifactStatusesResponse.StatusesEntryR\x08statuses\x1a(\n\x0e\x41rtifactStatus\x12\x16\n\x06\x65xists\x18\x01 \x01(\x08R\x06\x65xists\x1as\n\rStatusesEntry\x12\x10\n\x03key\x18\x01 \x01(\tR\x03key\x12L\n\x05value\x18\x02 \x01(\x0b\x32\x36.spark.connect.ArtifactStatusesResponse.ArtifactStatusR\x05value:\x02\x38\x01"\xd8\x03\n\x10InterruptRequest\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12=\n\x0cuser_context\x18\x02 \x01(\x0b\x32\x1a.spark.connect.UserContextR\x0buserContext\x12$\n\x0b\x63lient_type\x18\x03 \x01(\tH\x01R\nclientType\x88\x01\x01\x12T\n\x0einterrupt_type\x18\x04 \x01(\x0e\x32-.spark.connect.InterruptRequest.InterruptTypeR\rinterruptType\x12%\n\roperation_tag\x18\x05 \x01(\tH\x00R\x0coperationTag\x12#\n\x0coperation_id\x18\x06 \x01(\tH\x00R\x0boperationId"\x80\x01\n\rInterruptType\x12\x1e\n\x1aINTERRUPT_TYPE_UNSPECIFIED\x10\x00\x12\x16\n\x12INTERRUPT_TYPE_ALL\x10\x01\x12\x16\n\x12INTERRUPT_TYPE_TAG\x10\x02\x12\x1f\n\x1bINTERRUPT_TYPE_OPERATION_ID\x10\x03\x42\x0b\n\tinterruptB\x0e\n\x0c_client_type"[\n\x11InterruptResponse\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12\'\n\x0finterrupted_ids\x18\x02 \x03(\tR\x0einterruptedIds"5\n\x0fReattachOptions\x12"\n\x0creattachable\x18\x01 \x01(\x08R\x0creattachable"\x93\x02\n\x16ReattachExecuteRequest\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12=\n\x0cuser_context\x18\x02 \x01(\x0b\x32\x1a.spark.connect.UserContextR\x0buserContext\x12!\n\x0coperation_id\x18\x03 \x01(\tR\x0boperationId\x12$\n\x0b\x63lient_type\x18\x04 \x01(\tH\x00R\nclientType\x88\x01\x01\x12-\n\x10last_response_id\x18\x05 \x01(\tH\x01R\x0elastResponseId\x88\x01\x01\x42\x0e\n\x0c_client_typeB\x13\n\x11_last_response_id"\xc6\x03\n\x15ReleaseExecuteRequest\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12=\n\x0cuser_context\x18\x02 \x01(\x0b\x32\x1a.spark.connect.UserContextR\x0buserContext\x12!\n\x0coperation_id\x18\x03 \x01(\tR\x0boperationId\x12$\n\x0b\x63lient_type\x18\x04 \x01(\tH\x01R\nclientType\x88\x01\x01\x12R\n\x0brelease_all\x18\x05 \x01(\x0b\x32/.spark.connect.ReleaseExecuteRequest.ReleaseAllH\x00R\nreleaseAll\x12X\n\rrelease_until\x18\x06 \x01(\x0b\x32\x31.spark.connect.ReleaseExecuteRequest.ReleaseUntilH\x00R\x0creleaseUntil\x1a\x0c\n\nReleaseAll\x1a/\n\x0cReleaseUntil\x12\x1f\n\x0bresponse_id\x18\x01 \x01(\tR\nresponseIdB\t\n\x07releaseB\x0e\n\x0c_client_type"p\n\x16ReleaseExecuteResponse\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12&\n\x0coperation_id\x18\x02 \x01(\tH\x00R\x0boperationId\x88\x01\x01\x42\x0f\n\r_operation_id"\xc9\x01\n\x18\x46\x65tchErrorDetailsRequest\x12\x1d\n\nsession_id\x18\x01 \x01(\tR\tsessionId\x12=\n\x0cuser_context\x18\x02 \x01(\x0b\x32\x1a.spark.connect.UserContextR\x0buserContext\x12\x19\n\x08\x65rror_id\x18\x03 \x01(\tR\x07\x65rrorId\x12$\n\x0b\x63lient_type\x18\x04 \x01(\tH\x00R\nclientType\x88\x01\x01\x42\x0e\n\x0c_client_type"\xb5\x04\n\x19\x46\x65tchErrorDetailsResponse\x12)\n\x0eroot_error_idx\x18\x01 \x01(\x05H\x00R\x0crootErrorIdx\x88\x01\x01\x12\x46\n\x06\x65rrors\x18\x02 \x03(\x0b\x32..spark.connect.FetchErrorDetailsResponse.ErrorR\x06\x65rrors\x1a\xae\x01\n\x11StackTraceElement\x12\'\n\x0f\x64\x65\x63laring_class\x18\x01 \x01(\tR\x0e\x64\x65\x63laringClass\x12\x1f\n\x0bmethod_name\x18\x02 \x01(\tR\nmethodName\x12 \n\tfile_name\x18\x03 \x01(\tH\x00R\x08\x66ileName\x88\x01\x01\x12\x1f\n\x0bline_number\x18\x04 \x01(\x05R\nlineNumberB\x0c\n\n_file_name\x1a\xe0\x01\n\x05\x45rror\x12\x30\n\x14\x65rror_type_hierarchy\x18\x01 \x03(\tR\x12\x65rrorTypeHierarchy\x12\x18\n\x07message\x18\x02 \x01(\tR\x07message\x12[\n\x0bstack_trace\x18\x03 \x03(\x0b\x32:.spark.connect.FetchErrorDetailsResponse.StackTraceElementR\nstackTrace\x12 \n\tcause_idx\x18\x04 \x01(\x05H\x00R\x08\x63\x61useIdx\x88\x01\x01\x42\x0c\n\n_cause_idxB\x11\n\x0f_root_error_idx2\xd1\x06\n\x13SparkConnectService\x12X\n\x0b\x45xecutePlan\x12!.spark.connect.ExecutePlanRequest\x1a".spark.connect.ExecutePlanResponse"\x00\x30\x01\x12V\n\x0b\x41nalyzePlan\x12!.spark.connect.AnalyzePlanRequest\x1a".spark.connect.AnalyzePlanResponse"\x00\x12G\n\x06\x43onfig\x12\x1c.spark.connect.ConfigRequest\x1a\x1d.spark.connect.ConfigResponse"\x00\x12[\n\x0c\x41\x64\x64\x41rtifacts\x12".spark.connect.AddArtifactsRequest\x1a#.spark.connect.AddArtifactsResponse"\x00(\x01\x12\x63\n\x0e\x41rtifactStatus\x12&.spark.connect.ArtifactStatusesRequest\x1a\'.spark.connect.ArtifactStatusesResponse"\x00\x12P\n\tInterrupt\x12\x1f.spark.connect.InterruptRequest\x1a .spark.connect.InterruptResponse"\x00\x12`\n\x0fReattachExecute\x12%.spark.connect.ReattachExecuteRequest\x1a".spark.connect.ExecutePlanResponse"\x00\x30\x01\x12_\n\x0eReleaseExecute\x12$.spark.connect.ReleaseExecuteRequest\x1a%.spark.connect.ReleaseExecuteResponse"\x00\x12h\n\x11\x46\x65tchErrorDetails\x12\'.spark.connect.FetchErrorDetailsRequest\x1a(.spark.connect.FetchErrorDetailsResponse"\x00\x42\x36\n\x1eorg.apache.spark.connect.protoP\x01Z\x12internal/generatedb\x06proto3'
)

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "spark.connect.base_pb2", globals())
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = (
        b"\n\036org.apache.spark.connect.protoP\001Z\022internal/generated"
    )
    _EXECUTEPLANRESPONSE_METRICS_METRICOBJECT_EXECUTIONMETRICSENTRY._options = None
    _EXECUTEPLANRESPONSE_METRICS_METRICOBJECT_EXECUTIONMETRICSENTRY._serialized_options = b"8\001"
    _ARTIFACTSTATUSESRESPONSE_STATUSESENTRY._options = None
    _ARTIFACTSTATUSESRESPONSE_STATUSESENTRY._serialized_options = b"8\001"
    _PLAN._serialized_start = 219
    _PLAN._serialized_end = 335
    _USERCONTEXT._serialized_start = 337
    _USERCONTEXT._serialized_end = 459
    _ANALYZEPLANREQUEST._serialized_start = 462
    _ANALYZEPLANREQUEST._serialized_end = 2883
    _ANALYZEPLANREQUEST_SCHEMA._serialized_start = 1657
    _ANALYZEPLANREQUEST_SCHEMA._serialized_end = 1706
    _ANALYZEPLANREQUEST_EXPLAIN._serialized_start = 1709
    _ANALYZEPLANREQUEST_EXPLAIN._serialized_end = 2024
    _ANALYZEPLANREQUEST_EXPLAIN_EXPLAINMODE._serialized_start = 1852
    _ANALYZEPLANREQUEST_EXPLAIN_EXPLAINMODE._serialized_end = 2024
    _ANALYZEPLANREQUEST_TREESTRING._serialized_start = 2026
    _ANALYZEPLANREQUEST_TREESTRING._serialized_end = 2116
    _ANALYZEPLANREQUEST_ISLOCAL._serialized_start = 2118
    _ANALYZEPLANREQUEST_ISLOCAL._serialized_end = 2168
    _ANALYZEPLANREQUEST_ISSTREAMING._serialized_start = 2170
    _ANALYZEPLANREQUEST_ISSTREAMING._serialized_end = 2224
    _ANALYZEPLANREQUEST_INPUTFILES._serialized_start = 2226
    _ANALYZEPLANREQUEST_INPUTFILES._serialized_end = 2279
    _ANALYZEPLANREQUEST_SPARKVERSION._serialized_start = 2281
    _ANALYZEPLANREQUEST_SPARKVERSION._serialized_end = 2295
    _ANALYZEPLANREQUEST_DDLPARSE._serialized_start = 2297
    _ANALYZEPLANREQUEST_DDLPARSE._serialized_end = 2338
    _ANALYZEPLANREQUEST_SAMESEMANTICS._serialized_start = 2340
    _ANALYZEPLANREQUEST_SAMESEMANTICS._serialized_end = 2461
    _ANALYZEPLANREQUEST_SEMANTICHASH._serialized_start = 2463
    _ANALYZEPLANREQUEST_SEMANTICHASH._serialized_end = 2518
    _ANALYZEPLANREQUEST_PERSIST._serialized_start = 2521
    _ANALYZEPLANREQUEST_PERSIST._serialized_end = 2672
    _ANALYZEPLANREQUEST_UNPERSIST._serialized_start = 2674
    _ANALYZEPLANREQUEST_UNPERSIST._serialized_end = 2784
    _ANALYZEPLANREQUEST_GETSTORAGELEVEL._serialized_start = 2786
    _ANALYZEPLANREQUEST_GETSTORAGELEVEL._serialized_end = 2856
    _ANALYZEPLANRESPONSE._serialized_start = 2886
    _ANALYZEPLANRESPONSE._serialized_end = 4575
    _ANALYZEPLANRESPONSE_SCHEMA._serialized_start = 3994
    _ANALYZEPLANRESPONSE_SCHEMA._serialized_end = 4051
    _ANALYZEPLANRESPONSE_EXPLAIN._serialized_start = 4053
    _ANALYZEPLANRESPONSE_EXPLAIN._serialized_end = 4101
    _ANALYZEPLANRESPONSE_TREESTRING._serialized_start = 4103
    _ANALYZEPLANRESPONSE_TREESTRING._serialized_end = 4148
    _ANALYZEPLANRESPONSE_ISLOCAL._serialized_start = 4150
    _ANALYZEPLANRESPONSE_ISLOCAL._serialized_end = 4186
    _ANALYZEPLANRESPONSE_ISSTREAMING._serialized_start = 4188
    _ANALYZEPLANRESPONSE_ISSTREAMING._serialized_end = 4236
    _ANALYZEPLANRESPONSE_INPUTFILES._serialized_start = 4238
    _ANALYZEPLANRESPONSE_INPUTFILES._serialized_end = 4272
    _ANALYZEPLANRESPONSE_SPARKVERSION._serialized_start = 4274
    _ANALYZEPLANRESPONSE_SPARKVERSION._serialized_end = 4314
    _ANALYZEPLANRESPONSE_DDLPARSE._serialized_start = 4316
    _ANALYZEPLANRESPONSE_DDLPARSE._serialized_end = 4375
    _ANALYZEPLANRESPONSE_SAMESEMANTICS._serialized_start = 4377
    _ANALYZEPLANRESPONSE_SAMESEMANTICS._serialized_end = 4416
    _ANALYZEPLANRESPONSE_SEMANTICHASH._serialized_start = 4418
    _ANALYZEPLANRESPONSE_SEMANTICHASH._serialized_end = 4456
    _ANALYZEPLANRESPONSE_PERSIST._serialized_start = 2521
    _ANALYZEPLANRESPONSE_PERSIST._serialized_end = 2530
    _ANALYZEPLANRESPONSE_UNPERSIST._serialized_start = 2674
    _ANALYZEPLANRESPONSE_UNPERSIST._serialized_end = 2685
    _ANALYZEPLANRESPONSE_GETSTORAGELEVEL._serialized_start = 4482
    _ANALYZEPLANRESPONSE_GETSTORAGELEVEL._serialized_end = 4565
    _EXECUTEPLANREQUEST._serialized_start = 4578
    _EXECUTEPLANREQUEST._serialized_end = 5122
    _EXECUTEPLANREQUEST_REQUESTOPTION._serialized_start = 4924
    _EXECUTEPLANREQUEST_REQUESTOPTION._serialized_end = 5089
    _EXECUTEPLANRESPONSE._serialized_start = 5125
    _EXECUTEPLANRESPONSE._serialized_end = 7070
    _EXECUTEPLANRESPONSE_SQLCOMMANDRESULT._serialized_start = 6283
    _EXECUTEPLANRESPONSE_SQLCOMMANDRESULT._serialized_end = 6354
    _EXECUTEPLANRESPONSE_ARROWBATCH._serialized_start = 6356
    _EXECUTEPLANRESPONSE_ARROWBATCH._serialized_end = 6417
    _EXECUTEPLANRESPONSE_METRICS._serialized_start = 6420
    _EXECUTEPLANRESPONSE_METRICS._serialized_end = 6937
    _EXECUTEPLANRESPONSE_METRICS_METRICOBJECT._serialized_start = 6515
    _EXECUTEPLANRESPONSE_METRICS_METRICOBJECT._serialized_end = 6847
    _EXECUTEPLANRESPONSE_METRICS_METRICOBJECT_EXECUTIONMETRICSENTRY._serialized_start = 6724
    _EXECUTEPLANRESPONSE_METRICS_METRICOBJECT_EXECUTIONMETRICSENTRY._serialized_end = 6847
    _EXECUTEPLANRESPONSE_METRICS_METRICVALUE._serialized_start = 6849
    _EXECUTEPLANRESPONSE_METRICS_METRICVALUE._serialized_end = 6937
    _EXECUTEPLANRESPONSE_OBSERVEDMETRICS._serialized_start = 6939
    _EXECUTEPLANRESPONSE_OBSERVEDMETRICS._serialized_end = 7035
    _EXECUTEPLANRESPONSE_RESULTCOMPLETE._serialized_start = 7037
    _EXECUTEPLANRESPONSE_RESULTCOMPLETE._serialized_end = 7053
    _KEYVALUE._serialized_start = 7072
    _KEYVALUE._serialized_end = 7137
    _CONFIGREQUEST._serialized_start = 7140
    _CONFIGREQUEST._serialized_end = 8168
    _CONFIGREQUEST_OPERATION._serialized_start = 7360
    _CONFIGREQUEST_OPERATION._serialized_end = 7858
    _CONFIGREQUEST_SET._serialized_start = 7860
    _CONFIGREQUEST_SET._serialized_end = 7912
    _CONFIGREQUEST_GET._serialized_start = 7914
    _CONFIGREQUEST_GET._serialized_end = 7939
    _CONFIGREQUEST_GETWITHDEFAULT._serialized_start = 7941
    _CONFIGREQUEST_GETWITHDEFAULT._serialized_end = 8004
    _CONFIGREQUEST_GETOPTION._serialized_start = 8006
    _CONFIGREQUEST_GETOPTION._serialized_end = 8037
    _CONFIGREQUEST_GETALL._serialized_start = 8039
    _CONFIGREQUEST_GETALL._serialized_end = 8087
    _CONFIGREQUEST_UNSET._serialized_start = 8089
    _CONFIGREQUEST_UNSET._serialized_end = 8116
    _CONFIGREQUEST_ISMODIFIABLE._serialized_start = 8118
    _CONFIGREQUEST_ISMODIFIABLE._serialized_end = 8152
    _CONFIGRESPONSE._serialized_start = 8170
    _CONFIGRESPONSE._serialized_end = 8292
    _ADDARTIFACTSREQUEST._serialized_start = 8295
    _ADDARTIFACTSREQUEST._serialized_end = 9166
    _ADDARTIFACTSREQUEST_ARTIFACTCHUNK._serialized_start = 8682
    _ADDARTIFACTSREQUEST_ARTIFACTCHUNK._serialized_end = 8735
    _ADDARTIFACTSREQUEST_SINGLECHUNKARTIFACT._serialized_start = 8737
    _ADDARTIFACTSREQUEST_SINGLECHUNKARTIFACT._serialized_end = 8848
    _ADDARTIFACTSREQUEST_BATCH._serialized_start = 8850
    _ADDARTIFACTSREQUEST_BATCH._serialized_end = 8943
    _ADDARTIFACTSREQUEST_BEGINCHUNKEDARTIFACT._serialized_start = 8946
    _ADDARTIFACTSREQUEST_BEGINCHUNKEDARTIFACT._serialized_end = 9139
    _ADDARTIFACTSRESPONSE._serialized_start = 9169
    _ADDARTIFACTSRESPONSE._serialized_end = 9357
    _ADDARTIFACTSRESPONSE_ARTIFACTSUMMARY._serialized_start = 9276
    _ADDARTIFACTSRESPONSE_ARTIFACTSUMMARY._serialized_end = 9357
    _ARTIFACTSTATUSESREQUEST._serialized_start = 9360
    _ARTIFACTSTATUSESREQUEST._serialized_end = 9555
    _ARTIFACTSTATUSESRESPONSE._serialized_start = 9558
    _ARTIFACTSTATUSESRESPONSE._serialized_end = 9826
    _ARTIFACTSTATUSESRESPONSE_ARTIFACTSTATUS._serialized_start = 9669
    _ARTIFACTSTATUSESRESPONSE_ARTIFACTSTATUS._serialized_end = 9709
    _ARTIFACTSTATUSESRESPONSE_STATUSESENTRY._serialized_start = 9711
    _ARTIFACTSTATUSESRESPONSE_STATUSESENTRY._serialized_end = 9826
    _INTERRUPTREQUEST._serialized_start = 9829
    _INTERRUPTREQUEST._serialized_end = 10301
    _INTERRUPTREQUEST_INTERRUPTTYPE._serialized_start = 10144
    _INTERRUPTREQUEST_INTERRUPTTYPE._serialized_end = 10272
    _INTERRUPTRESPONSE._serialized_start = 10303
    _INTERRUPTRESPONSE._serialized_end = 10394
    _REATTACHOPTIONS._serialized_start = 10396
    _REATTACHOPTIONS._serialized_end = 10449
    _REATTACHEXECUTEREQUEST._serialized_start = 10452
    _REATTACHEXECUTEREQUEST._serialized_end = 10727
    _RELEASEEXECUTEREQUEST._serialized_start = 10730
    _RELEASEEXECUTEREQUEST._serialized_end = 11184
    _RELEASEEXECUTEREQUEST_RELEASEALL._serialized_start = 11096
    _RELEASEEXECUTEREQUEST_RELEASEALL._serialized_end = 11108
    _RELEASEEXECUTEREQUEST_RELEASEUNTIL._serialized_start = 11110
    _RELEASEEXECUTEREQUEST_RELEASEUNTIL._serialized_end = 11157
    _RELEASEEXECUTERESPONSE._serialized_start = 11186
    _RELEASEEXECUTERESPONSE._serialized_end = 11298
    _FETCHERRORDETAILSREQUEST._serialized_start = 11301
    _FETCHERRORDETAILSREQUEST._serialized_end = 11502
    _FETCHERRORDETAILSRESPONSE._serialized_start = 11505
    _FETCHERRORDETAILSRESPONSE._serialized_end = 12070
    _FETCHERRORDETAILSRESPONSE_STACKTRACEELEMENT._serialized_start = 11650
    _FETCHERRORDETAILSRESPONSE_STACKTRACEELEMENT._serialized_end = 11824
    _FETCHERRORDETAILSRESPONSE_ERROR._serialized_start = 11827
    _FETCHERRORDETAILSRESPONSE_ERROR._serialized_end = 12051
    _SPARKCONNECTSERVICE._serialized_start = 12073
    _SPARKCONNECTSERVICE._serialized_end = 12922
# @@protoc_insertion_point(module_scope)
