#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import copy
import datetime
import glob
import hashlib
import inspect
import json
import logging
import numbers
import os
import random
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from io import BytesIO
from queue import Empty as EmptyQueue
from queue import Queue
from string import Template

import yaml
from graphscope.framework import utils
from graphscope.framework.graph_schema import GraphSchema
from graphscope.framework.utils import transform_labeled_vertex_data_selector
from graphscope.framework.utils import transform_labeled_vertex_property_data_selector
from graphscope.framework.utils import transform_vertex_data_selector
from graphscope.framework.utils import transform_vertex_property_data_selector
from graphscope.proto import attr_value_pb2
from graphscope.proto import graph_def_pb2
from graphscope.proto import op_def_pb2
from graphscope.proto import types_pb2

from gscoordinator.io_utils import PipeWatcher

logger = logging.getLogger("graphscope")

try:
    import gscoordinator

    COORDINATOR_HOME = os.path.abspath(os.path.join(gscoordinator.__file__, "..", ".."))
except ModuleNotFoundError:
    COORDINATOR_HOME = os.path.abspath(os.path.join(__file__, "..", ".."))

GRAPHSCOPE_HOME = os.path.join(COORDINATOR_HOME, "..")

WORKSPACE = "/tmp/gs"
DEFAULT_GS_CONFIG_FILE = ".gs_conf.yaml"
ANALYTICAL_ENGINE_HOME = os.path.join(GRAPHSCOPE_HOME, "analytical_engine")
ANALYTICAL_ENGINE_PATH = os.path.join(ANALYTICAL_ENGINE_HOME, "build", "grape_engine")

if not os.path.isfile(ANALYTICAL_ENGINE_PATH):
    ANALYTICAL_ENGINE_HOME = "/usr/local/bin"
    ANALYTICAL_ENGINE_PATH = "/usr/local/bin/grape_engine"
TEMPLATE_DIR = os.path.join(COORDINATOR_HOME, "gscoordinator", "template")
BUILTIN_APP_RESOURCE_PATH = os.path.join(
    COORDINATOR_HOME, "gscoordinator", "builtin/app/builtin_app.gar"
)


def is_port_in_use(host, port):
    """Check whether a port is in use.

    Args:
        port (int): A port.

    Returns:
        bool: True if the port in use.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((host, port)) == 0


def get_timestamp():
    now = datetime.datetime.now()
    return datetime.datetime.timestamp(now)


def get_lib_path(app_dir, app_name):
    lib_path = ""
    if sys.platform == "linux" or sys.platform == "linux2":
        lib_path = os.path.join(app_dir, "lib%s.so" % app_name)
    elif sys.platform == "darwin":
        lib_path = os.path.join(app_dir, "lib%s.dylib" % app_name)
    else:
        raise RuntimeError("Unsupported platform.")
    return lib_path


def get_app_sha256(attr):
    (
        app_type,
        app_header,
        app_class,
        vd_type,
        md_type,
        pregel_combine,
    ) = _codegen_app_info(attr, DEFAULT_GS_CONFIG_FILE)
    graph_header, graph_type = _codegen_graph_info(attr)
    logger.info("Codegened graph type: %s, Graph header: %s", graph_type, graph_header)
    if app_type == "cpp_pie":
        return hashlib.sha256(
            f"{app_type}.{app_class}.{graph_type}".encode("utf-8")
        ).hexdigest()
    else:
        s = hashlib.sha256()
        s.update(f"{app_type}.{app_class}.{graph_type}".encode("utf-8"))
        if types_pb2.GAR in attr:
            s.update(attr[types_pb2.GAR].s)
        return s.hexdigest()


def get_graph_sha256(attr):
    _, graph_class = _codegen_graph_info(attr)
    return hashlib.sha256(graph_class.encode("utf-8")).hexdigest()


def compile_app(workspace: str, library_name, attr, engine_config: dict):
    """Compile an application.

    Args:
        workspace (str): working dir.
        library_name (str): name of library
        attr (`AttrValue`): All information needed to compile an app.
        engine_config (dict): for options of NETWORKX

    Returns:
        str: Path of the built library.
    """
    app_dir = os.path.join(workspace, library_name)
    os.makedirs(app_dir, exist_ok=True)

    _extract_gar(app_dir, attr)
    # codegen app and graph info
    # vd_type and md_type is None in cpp_pie
    (
        app_type,
        app_header,
        app_class,
        vd_type,
        md_type,
        pregel_combine,
    ) = _codegen_app_info(attr, DEFAULT_GS_CONFIG_FILE)
    logger.info(
        "Codegened application type: %s, app header: %s, app_class: %s, vd_type: %s, md_type: %s, pregel_combine: %s",
        app_type,
        app_header,
        app_class,
        str(vd_type),
        str(md_type),
        str(pregel_combine),
    )

    graph_header, graph_type = _codegen_graph_info(attr)
    logger.info("Codegened graph type: %s, Graph header: %s", graph_type, graph_header)

    os.chdir(app_dir)

    module_name = ""
    cmake_commands = [
        "cmake",
        ".",
        "-DNETWORKX=" + engine_config["networkx"],
    ]
    if app_type != "cpp_pie":
        if app_type == "cython_pregel":
            pxd_name = "pregel"
            cmake_commands += ["-DCYTHON_PREGEL_APP=True"]
            if pregel_combine:
                cmake_commands += ["-DENABLE_PREGEL_COMBINE=True"]
        else:
            pxd_name = "pie"
            cmake_commands += ["-DCYTHON_PIE_APP=True"]

        # Copy pxd file and generate cc file from pyx
        shutil.copyfile(
            os.path.join(TEMPLATE_DIR, "{}.pxd.template".format(pxd_name)),
            os.path.join(app_dir, "{}.pxd".format(pxd_name)),
        )
        # Assume the gar will have and only have one .pyx file
        for pyx_file in glob.glob(app_dir + "/*.pyx"):
            module_name = os.path.splitext(os.path.basename(pyx_file))[0]
            cc_file = os.path.join(app_dir, module_name + ".cc")
            subprocess.check_call(["cython", "-3", "--cplus", "-o", cc_file, pyx_file])
        app_header = "{}.h".format(module_name)

    # replace and generate cmakelist
    cmakelists_file_tmp = os.path.join(TEMPLATE_DIR, "CMakeLists.template")
    cmakelists_file = os.path.join(app_dir, "CMakeLists.txt")
    with open(cmakelists_file_tmp, mode="r") as template:
        content = template.read()
        content = Template(content).safe_substitute(
            _analytical_engine_home=ANALYTICAL_ENGINE_HOME,
            _frame_name=library_name,
            _vd_type=vd_type,
            _md_type=md_type,
            _graph_type=graph_type,
            _graph_header=graph_header,
            _module_name=module_name,
            _app_type=app_class,
            _app_header=app_header,
        )
        with open(cmakelists_file, mode="w") as f:
            f.write(content)

    # compile
    logger.info("Building app ...")
    cmake_process = subprocess.Popen(
        cmake_commands,
        env=os.environ.copy(),
        universal_newlines=True,
        encoding="utf-8",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    cmake_stderr_watcher = PipeWatcher(cmake_process.stderr, sys.stdout)
    setattr(cmake_process, "stderr_watcher", cmake_stderr_watcher)
    cmake_process.wait()

    make_process = subprocess.Popen(
        ["make", "-j4"],
        env=os.environ.copy(),
        universal_newlines=True,
        encoding="utf-8",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    make_stderr_watcher = PipeWatcher(make_process.stderr, sys.stdout)
    setattr(make_process, "stderr_watcher", make_stderr_watcher)
    make_process.wait()
    lib_path = get_lib_path(app_dir, library_name)
    assert os.path.isfile(lib_path), "Error occurs when building the frame library."
    return lib_path


def compile_graph_frame(workspace: str, library_name, attr: dict, engine_config: dict):
    """Compile an application.

    Args:
        workspace (str): Working dir.
        library_name (str): name of library
        attr (`AttrValue`): All information needed to compile a graph library.
        engine_config (dict): for options of NETWORKX

    Raises:
        ValueError: When graph_type is not supported.

    Returns:
        str: Path of the built graph library.
    """

    _, graph_class = _codegen_graph_info(attr)

    logger.info("Codegened graph frame type: %s", graph_class)

    library_dir = os.path.join(workspace, library_name)
    os.makedirs(library_dir, exist_ok=True)

    os.chdir(library_dir)

    graph_type = attr[types_pb2.GRAPH_TYPE].graph_type

    cmake_commands = [
        "cmake",
        ".",
        "-DNETWORKX=" + engine_config["networkx"],
    ]
    if graph_type == graph_def_pb2.ARROW_PROPERTY:
        cmake_commands += ["-DPROPERTY_GRAPH_FRAME=True"]
    elif (
        graph_type == graph_def_pb2.ARROW_PROJECTED
        or graph_type == graph_def_pb2.DYNAMIC_PROJECTED
    ):
        cmake_commands += ["-DPROJECT_FRAME=True"]
    else:
        raise ValueError("Illegal graph type: {}".format(graph_type))
    # replace and generate cmakelist
    cmakelists_file_tmp = os.path.join(TEMPLATE_DIR, "CMakeLists.template")
    cmakelists_file = os.path.join(library_dir, "CMakeLists.txt")
    with open(cmakelists_file_tmp, mode="r") as template:
        content = template.read()
        content = Template(content).safe_substitute(
            _analytical_engine_home=ANALYTICAL_ENGINE_HOME,
            _frame_name=library_name,
            _graph_type=graph_class,
        )
        with open(cmakelists_file, mode="w") as f:
            f.write(content)

    # compile
    logger.info("Building graph library ...")
    cmake_process = subprocess.Popen(
        cmake_commands,
        env=os.environ.copy(),
        universal_newlines=True,
        encoding="utf-8",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    cmake_stderr_watcher = PipeWatcher(cmake_process.stderr, sys.stdout)
    setattr(cmake_process, "stderr_watcher", cmake_stderr_watcher)
    cmake_process.wait()

    make_process = subprocess.Popen(
        ["make", "-j4"],
        env=os.environ.copy(),
        universal_newlines=True,
        encoding="utf-8",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    make_stderr_watcher = PipeWatcher(make_process.stderr, sys.stdout)
    setattr(make_process, "stderr_watcher", make_stderr_watcher)
    make_process.wait()
    lib_path = get_lib_path(library_dir, library_name)
    assert os.path.isfile(lib_path), "Error occurs when building the frame library."
    return lib_path


def op_pre_process(op, op_result_pool, key_to_op):
    if op.op == types_pb2.REPORT_GRAPH:
        # do nothing for nx report graph
        return
    if op.op == types_pb2.ADD_LABELS:
        _pre_process_for_add_labels_op(op, op_result_pool, key_to_op)
    if op.op == types_pb2.RUN_APP:
        _pre_process_for_run_app_op(op, op_result_pool, key_to_op)
    if op.op == types_pb2.BIND_APP:
        _pre_process_for_bind_app_op(op, op_result_pool, key_to_op)
    if op.op == types_pb2.PROJECT_GRAPH:
        _pre_process_for_project_op(op, op_result_pool, key_to_op)
    if op.op == types_pb2.PROJECT_TO_SIMPLE:
        _pre_process_for_project_to_simple_op(op, op_result_pool, key_to_op)
    if op.op == types_pb2.ADD_COLUMN:
        _pre_process_for_add_column_op(op, op_result_pool, key_to_op)
    if op.op == types_pb2.UNLOAD_GRAPH:
        _pre_process_for_unload_graph_op(op, op_result_pool, key_to_op)
    if op.op in (
        types_pb2.CONTEXT_TO_NUMPY,
        types_pb2.CONTEXT_TO_DATAFRAME,
        types_pb2.TO_VINEYARD_TENSOR,
        types_pb2.TO_VINEYARD_DATAFRAME,
    ):
        _pre_process_for_context_op(op, op_result_pool, key_to_op)


def _pre_process_for_add_labels_op(op, op_result_pool, key_to_op):
    assert len(op.parents) == 1
    key_of_parent_op = op.parents[0]
    result = op_result_pool[key_of_parent_op]
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(utils.s_to_attr(result.graph_def.key))


# get `bind_app` runtime informarion in lazy mode
def _pre_process_for_bind_app_op(op, op_result_pool, key_to_op):
    for key_of_parent_op in op.parents:
        parent_op = key_to_op[key_of_parent_op]
        if parent_op.op == types_pb2.CREATE_APP:
            # app assets
            op.attr[types_pb2.APP_ALGO].CopyFrom(parent_op.attr[types_pb2.APP_ALGO])
            if types_pb2.GAR in parent_op.attr:
                op.attr[types_pb2.GAR].CopyFrom(parent_op.attr[types_pb2.GAR])
        else:
            # get graph runtime information from results
            result = op_result_pool[key_of_parent_op]
            assert result.graph_def.extension.Is(
                graph_def_pb2.VineyardInfoPb.DESCRIPTOR
            )
            vy_info = graph_def_pb2.VineyardInfoPb()
            result.graph_def.extension.Unpack(vy_info)
            op.attr[types_pb2.GRAPH_NAME].CopyFrom(
                attr_value_pb2.AttrValue(s=result.graph_def.key.encode("utf-8"))
            )
            op.attr[types_pb2.GRAPH_TYPE].CopyFrom(
                attr_value_pb2.AttrValue(graph_type=result.graph_def.graph_type)
            )
            op.attr[types_pb2.OID_TYPE].CopyFrom(
                utils.s_to_attr(
                    utils.normalize_data_type_str(
                        utils.data_type_to_cpp(vy_info.oid_type)
                    )
                )
            )
            op.attr[types_pb2.VID_TYPE].CopyFrom(
                utils.s_to_attr(utils.data_type_to_cpp(vy_info.vid_type))
            )
            op.attr[types_pb2.V_DATA_TYPE].CopyFrom(
                utils.s_to_attr(utils.data_type_to_cpp(vy_info.vdata_type))
            )
            op.attr[types_pb2.E_DATA_TYPE].CopyFrom(
                utils.s_to_attr(utils.data_type_to_cpp(vy_info.edata_type))
            )


# get `run_app` runtime informarion in lazy mode
def _pre_process_for_run_app_op(op, op_result_pool, key_to_op):
    # run_app op has only one parent
    assert len(op.parents) == 1
    key_of_parent_op = op.parents[0]
    parent_op = key_to_op[key_of_parent_op]
    assert parent_op.op == types_pb2.BIND_APP
    # set graph key
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(parent_op.attr[types_pb2.GRAPH_NAME])
    result = op_result_pool[key_of_parent_op]
    # set app key
    op.attr[types_pb2.APP_NAME].CopyFrom(
        attr_value_pb2.AttrValue(s=result.result.decode("utf-8").encode("utf-8"))
    )


def _pre_process_for_unload_graph_op(op, op_result_pool, key_to_op):
    assert len(op.parents) == 1
    key_of_parent_op = op.parents[0]
    result = op_result_pool[key_of_parent_op]
    assert result.graph_def.extension.Is(graph_def_pb2.VineyardInfoPb.DESCRIPTOR)
    vy_info = graph_def_pb2.VineyardInfoPb()
    result.graph_def.extension.Unpack(vy_info)
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(utils.s_to_attr(result.graph_def.key))
    op.attr[types_pb2.VINEYARD_ID].CopyFrom(utils.i_to_attr(vy_info.vineyard_id))


def _pre_process_for_add_column_op(op, op_result_pool, key_to_op):
    for key_of_parent_op in op.parents:
        parent_op = key_to_op[key_of_parent_op]
        if parent_op.op != types_pb2.RUN_APP:
            # get graph information
            r = op_result_pool[key_of_parent_op]
            graph_name = r.graph_def.key
            graph_type = r.graph_def.graph_type
            schema = GraphSchema()
            schema.from_graph_def(r.graph_def)
    for key_of_parent_op in op.parents:
        parent_op = key_to_op[key_of_parent_op]
        if parent_op.op == types_pb2.RUN_APP:
            selector = op.attr[types_pb2.SELECTOR].s.decode("utf-8")
            r = op_result_pool[key_of_parent_op]
            parent_op_result = json.loads(r.result.decode("utf-8"))
            context_key = parent_op_result["context_key"]
            context_type = parent_op_result["context_type"]
            selector = _tranform_dataframe_selector(context_type, schema, selector)
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(utils.s_to_attr(graph_name))
    op.attr[types_pb2.GRAPH_TYPE].CopyFrom(utils.graph_type_to_attr(graph_type))
    op.attr[types_pb2.CTX_NAME].CopyFrom(utils.s_to_attr(context_key))
    op.attr[types_pb2.SELECTOR].CopyFrom(utils.s_to_attr(selector))


def _pre_process_for_context_op(op, op_result_pool, key_to_op):
    def __backtrack_key_of_graph_op(key):
        bfs_queue = Queue()
        bfs_queue.put(key)
        while not bfs_queue.empty():
            next_op_key = bfs_queue.get()
            if next_op_key in key_to_op:
                next_op = key_to_op[next_op_key]
                if next_op.op in (
                    types_pb2.CREATE_GRAPH,
                    types_pb2.ADD_LABELS,
                    types_pb2.TRANSFORM_GRAPH,
                    types_pb2.PROJECT_GRAPH,
                    types_pb2.PROJECT_TO_SIMPLE,
                ):
                    return next_op
                for parent_key in next_op.parents:
                    bfs_queue.put(parent_key)
        return None

    assert len(op.parents) == 1
    schema = None
    key_of_parent_op = op.parents[0]
    graph_op = __backtrack_key_of_graph_op(key_of_parent_op)
    r = op_result_pool[key_of_parent_op]
    # set context key
    parent_op_result = json.loads(r.result.decode("utf-8"))
    context_key = parent_op_result["context_key"]
    context_type = parent_op_result["context_type"]
    op.attr[types_pb2.CTX_NAME].CopyFrom(
        attr_value_pb2.AttrValue(s=context_key.encode("utf-8"))
    )
    r = op_result_pool[graph_op.key]
    # transform selector
    schema = GraphSchema()
    schema.from_graph_def(r.graph_def)
    selector = op.attr[types_pb2.SELECTOR].s.decode("utf-8")
    if op.op in (types_pb2.CONTEXT_TO_DATAFRAME, types_pb2.TO_VINEYARD_DATAFRAME):
        selector = _tranform_dataframe_selector(context_type, schema, selector)
    else:
        # to numpy
        selector = _tranform_numpy_selector(context_type, schema, selector)
    if selector is not None:
        op.attr[types_pb2.SELECTOR].CopyFrom(
            attr_value_pb2.AttrValue(s=selector.encode("utf-8"))
        )


def _pre_process_for_project_to_simple_op(op, op_result_pool, key_to_op):
    # for nx graph
    if op.attr[types_pb2.GRAPH_TYPE].graph_type == graph_def_pb2.DYNAMIC_PROJECTED:
        return
    assert len(op.parents) == 1
    # get parent graph schema
    key_of_parent_op = op.parents[0]
    r = op_result_pool[key_of_parent_op]
    schema = GraphSchema()
    schema.from_graph_def(r.graph_def)
    graph_name = r.graph_def.key
    check_argument(
        schema.vertex_label_num == 1,
        "Cannot project to simple, vertex label number is not one.",
    )
    check_argument(
        schema.edge_label_num == 1,
        "Cannot project to simple, edge label number is not one.",
    )
    v_label = schema.vertex_labels[0]
    e_label = schema.edge_labels[0]
    relation = (v_label, v_label)
    check_argument(
        relation in schema.get_relationships(e_label),
        f"Cannot project to simple, Graph doesn't contain such relationship: {v_label} -> {e_label} <- {v_label}.",
    )
    v_props = schema.get_vertex_properties(v_label)
    e_props = schema.get_edge_properties(e_label)
    check_argument(len(v_props) <= 1)
    check_argument(len(e_props) <= 1)
    v_label_id = schema.get_vertex_label_id(v_label)
    e_label_id = schema.get_edge_label_id(e_label)
    v_prop_id, vdata_type = (v_props[0].id, v_props[0].type) if v_props else (-1, None)
    e_prop_id, edata_type = (e_props[0].id, e_props[0].type) if e_props else (-1, None)
    oid_type = schema.oid_type
    vid_type = schema.vid_type
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(
        attr_value_pb2.AttrValue(s=graph_name.encode("utf-8"))
    )
    op.attr[types_pb2.GRAPH_TYPE].CopyFrom(
        utils.graph_type_to_attr(graph_def_pb2.ARROW_PROJECTED)
    )
    op.attr[types_pb2.V_LABEL_ID].CopyFrom(utils.i_to_attr(v_label_id))
    op.attr[types_pb2.V_PROP_ID].CopyFrom(utils.i_to_attr(v_prop_id))
    op.attr[types_pb2.E_LABEL_ID].CopyFrom(utils.i_to_attr(e_label_id))
    op.attr[types_pb2.E_PROP_ID].CopyFrom(utils.i_to_attr(e_prop_id))
    op.attr[types_pb2.OID_TYPE].CopyFrom(
        utils.s_to_attr(utils.data_type_to_cpp(oid_type))
    )
    op.attr[types_pb2.VID_TYPE].CopyFrom(
        utils.s_to_attr(utils.data_type_to_cpp(vid_type))
    )
    op.attr[types_pb2.V_DATA_TYPE].CopyFrom(
        utils.s_to_attr(utils.data_type_to_cpp(vdata_type))
    )
    op.attr[types_pb2.E_DATA_TYPE].CopyFrom(
        utils.s_to_attr(utils.data_type_to_cpp(edata_type))
    )


def _pre_process_for_project_op(op, op_result_pool, key_to_op):
    def _get_all_v_props_id(schema, label):
        props = schema.get_vertex_properties(label)
        return [schema.get_vertex_property_id(label, prop.name) for prop in props]

    def _get_all_e_props_id(schema, label):
        props = schema.get_edge_properties(label)
        return [schema.get_edge_property_id(label, prop.name) for prop in props]

    assert len(op.parents) == 1
    # get parent graph schema
    key_of_parent_op = op.parents[0]
    r = op_result_pool[key_of_parent_op]
    schema = GraphSchema()
    schema.from_graph_def(r.graph_def)
    graph_name = r.graph_def.key
    vertices = json.loads(op.attr[types_pb2.VERTEX_COLLECTIONS].s.decode("utf-8"))
    edges = json.loads(op.attr[types_pb2.EDGE_COLLECTIONS].s.decode("utf-8"))
    vertex_collections = {}
    edge_collections = {}
    for label, props in vertices.items():
        label_id = schema.get_vertex_label_id(label)
        if props is None:
            vertex_collections[label_id] = _get_all_v_props_id(schema, label)
        else:
            vertex_collections[label_id] = sorted(
                [schema.get_vertex_property_id(label, prop) for prop in props]
            )
    for label, props in edges.items():
        relations = schema.get_relationships(label)
        valid = False
        for src, dst in relations:
            if src in vertices and dst in vertices:
                valid = True
                break
        if not valid:
            raise ValueError("Cannot find a valid relation in given vertices and edges")
        label_id = schema.get_edge_label_id(label)
        if props is None:
            edge_collections[label_id] = _get_all_e_props_id(schema, label)
        else:
            edge_collections[label_id] = sorted(
                [schema.get_edge_property_id(label, prop) for prop in props]
            )
    vertex_collections = dict(sorted(vertex_collections.items()))
    edge_collections = dict(sorted(edge_collections.items()))

    # construct op attr
    attr = attr_value_pb2.AttrValue()
    v_attr = attr_value_pb2.NameAttrList()
    e_attr = attr_value_pb2.NameAttrList()
    for label, props in vertex_collections.items():
        v_attr.attr[label].CopyFrom(utils.list_i_to_attr(props))
    for label, props in edge_collections.items():
        e_attr.attr[label].CopyFrom(utils.list_i_to_attr(props))
    attr.list.func.extend([v_attr, e_attr])
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(
        attr_value_pb2.AttrValue(s=graph_name.encode("utf-8"))
    )
    op.attr[types_pb2.ARROW_PROPERTY_DEFINITION].CopyFrom(attr)
    del op.attr[types_pb2.VERTEX_COLLECTIONS]
    del op.attr[types_pb2.EDGE_COLLECTIONS]


def _tranform_numpy_selector(context_type, schema, selector):
    if context_type == "tensor":
        selector = None
    if context_type == "vertex_data":
        selector = transform_vertex_data_selector(selector)
    if context_type == "labeled_vertex_data":
        selector = transform_labeled_vertex_data_selector(schema, selector)
    if context_type == "vertex_property":
        selector = transform_vertex_property_data_selector(selector)
    if context_type == "labeled_vertex_property":
        selector = transform_labeled_vertex_property_data_selector(schema, selector)
    return selector


def _tranform_dataframe_selector(context_type, schema, selector):
    selector = json.loads(selector)
    if context_type == "tensor":
        selector = {key: None for key, value in selector.items()}
    if context_type == "vertex_data":
        selector = {
            key: transform_vertex_data_selector(value)
            for key, value in selector.items()
        }
    if context_type == "labeled_vertex_data":
        selector = {
            key: transform_labeled_vertex_data_selector(schema, value)
            for key, value in selector.items()
        }
    if context_type == "vertex_property":
        selector = {
            key: transform_vertex_property_data_selector(value)
            for key, value in selector.items()
        }
    if context_type == "labeled_vertex_property":
        selector = {
            key: transform_labeled_vertex_property_data_selector(schema, value)
            for key, value in selector.items()
        }
    return json.dumps(selector)


def _extract_gar(app_dir: str, attr):
    """Extract gar to workspace
    Args:
        workspace (str): Working directory
        attr (`AttrValue`): Optionally it can contains the bytes of gar.
    """
    fp = BUILTIN_APP_RESOURCE_PATH  # default is builtin app resources.
    if types_pb2.GAR in attr:
        # if gar sent via bytecode in attr, overwrite.
        fp = BytesIO(attr[types_pb2.GAR].s)
    with zipfile.ZipFile(fp, "r") as zip_ref:
        zip_ref.extractall(app_dir)


def _codegen_app_info(attr, meta_file: str):
    """Codegen application by instanize the template specialization.

    Args:
        workspace (str): Working directory
        meta_file (str): A yaml file that contains metas of all builtin app.
        attr (`AttrValue`): For get algorithm name of app.

    Raises:
        KeyError: If the algorithm name doesn't exist in the `meta_file`

    Returns:
        type: app_type
        app class: for fulfilling the CMakelists.
    """
    fp = BUILTIN_APP_RESOURCE_PATH  # default is builtin app resources.
    if types_pb2.GAR in attr:
        # if gar sent via bytecode in attr, overwrite.
        fp = BytesIO(attr[types_pb2.GAR].s)
    with zipfile.ZipFile(fp, "r") as zip_ref:
        with zip_ref.open(meta_file, "r") as f:
            config_yaml = yaml.safe_load(f)

    algo = attr[types_pb2.APP_ALGO].s.decode("utf-8")
    for app in config_yaml["app"]:
        if app["algo"] == algo:
            app_type = app["type"]  # cpp_pie or cython_pregel or cython_pie
            if app_type == "cpp_pie":
                return (
                    app_type,
                    app["src"],
                    "{}<_GRAPH_TYPE>".format(app["class_name"]),
                    None,
                    None,
                    None,
                )
            if app_type in ("cython_pregel", "cython_pie"):
                # cython app doesn't have c-header file
                return (
                    app_type,
                    "",
                    "",
                    app["vd_type"],
                    app["md_type"],
                    app["pregel_combine"],
                )

    raise KeyError("Algorithm does not exist in the gar resource.")


# a mapping for classname to header file.
GRAPH_HEADER_MAP = {
    graph_def_pb2.IMMUTABLE_EDGECUT: (
        "grape::ImmutableEdgecutFragment",
        "grape/fragment/immutable_edgecut_fragment.h",
    ),
    graph_def_pb2.DYNAMIC_PROJECTED: (
        "gs::DynamicProjectedFragment",
        "core/fragment/dynamic_projected_fragment.h",
    ),
    graph_def_pb2.ARROW_PROPERTY: (
        "vineyard::ArrowFragment",
        "vineyard/graph/fragment/arrow_fragment.h",
    ),
    graph_def_pb2.ARROW_PROJECTED: (
        "gs::ArrowProjectedFragment",
        "core/fragment/arrow_projected_fragment.h",
    ),
    graph_def_pb2.DYNAMIC_PROPERTY: (
        "gs::DynamicFragment",
        "core/fragment/dynamic_fragment.h",
    ),
}


def _codegen_graph_info(attr):
    graph_type = attr[types_pb2.GRAPH_TYPE].graph_type
    graph_class, graph_header = GRAPH_HEADER_MAP[graph_type]
    # graph_type is a literal of graph template in c++ side
    if graph_class == "vineyard::ArrowFragment":
        # in a format of full qualified name, e.g. vineyard::ArrowFragment<double, double>
        graph_fqn = "{}<{},{}>".format(
            graph_class,
            attr[types_pb2.OID_TYPE].s.decode("utf-8"),
            attr[types_pb2.VID_TYPE].s.decode("utf-8"),
        )
    elif graph_class in (
        "gs::ArrowProjectedFragment",
        "grape::ImmutableEdgecutFragment",
    ):
        # in a format of gs::ArrowProjectedFragment<int64_t, uint32_t, double, double>
        # or grape::ImmutableEdgecutFragment<int64_t, uint32_t, double, double>
        graph_fqn = "{}<{},{},{},{}>".format(
            graph_class,
            attr[types_pb2.OID_TYPE].s.decode("utf-8"),
            attr[types_pb2.VID_TYPE].s.decode("utf-8"),
            attr[types_pb2.V_DATA_TYPE].s.decode("utf-8"),
            attr[types_pb2.E_DATA_TYPE].s.decode("utf-8"),
        )
    else:
        # gs::DynamicProjectedFragment<double, double>
        graph_fqn = "{}<{},{}>".format(
            graph_class,
            attr[types_pb2.V_DATA_TYPE].s.decode("utf-8"),
            attr[types_pb2.E_DATA_TYPE].s.decode("utf-8"),
        )
    return graph_header, graph_fqn


def create_single_op_dag(op_type, config=None):
    op_def = op_def_pb2.OpDef(op=op_type, key=uuid.uuid4().hex)
    if config:
        for k, v in config.items():
            op_def.attr[k].CopyFrom(v)

    dag = op_def_pb2.DagDef()
    dag.op.extend([op_def])
    return dag


def dump_as_json(schema, path):
    out = {}
    items = []
    idx = 0
    for i in range(len(schema.vertex_labels)):
        vertex = {"id": idx, "label": schema.vertex_labels[i], "type": "VERTEX"}
        vertex["propertyDefList"] = []
        for j in range(len(schema.vertex_property_names[i].s)):
            names = schema.vertex_property_names[i]
            types = schema.vertex_property_types[i]
            vertex["propertyDefList"].append(
                {"id": j, "name": names.s[j], "data_type": types.s[j].upper()}
            )
        vertex["indexes"] = []
        vertex["indexes"].append({"propertyNames": [names.s[0]]})
        items.append(vertex)
        idx += 1

    for i in range(len(schema.edge_labels)):
        edge = {"id": idx, "label": schema.edge_labels[i], "type": "EDGE"}
        edge["propertyDefList"] = []
        for j in range(len(schema.edge_property_names[i].s)):
            names = schema.edge_property_names[i]
            types = schema.edge_property_types[i]
            edge["propertyDefList"].append(
                {"id": j, "name": names.s[j], "data_type": types.s[j].upper()}
            )
        edge["rawRelationShips"] = []
        edge["rawRelationShips"].append(
            {"srcVertexLabel": "xx", "dstVertexLabel": "xx"}
        )
        idx += 1
        items.append(edge)
    out["types"] = items
    out["partitionNum"] = 4
    with open(path, "w") as fp:
        json.dump(out, fp)


def dump_string(schema_string, path):
    with open(path, "w") as fp:
        fp.write(schema_string)


def parse_readable_memory(value):
    value = str(value).strip()
    num = value[:-2]
    suffix = value[-2:]
    try:
        float(num)
    except ValueError as e:
        raise ValueError(
            "Argument cannot be interpreted as a number: %s" % value
        ) from e
    if suffix not in ["Ki", "Mi", "Gi"]:
        raise ValueError("Memory suffix must be one of 'Ki', 'Mi' and 'Gi': %s" % value)
    return value


def parse_as_glog_level(log_level):
    # log level in glog: INFO=1, DEBUG=10
    # log level in python: DEBUG=10, INFO=20
    if isinstance(log_level, str):
        if log_level == "silent" or log_level == "SILENT":
            log_level = -1
        else:
            log_level = getattr(logging, log_level.upper())
    python_to_glog = {10: 10, 20: 1}
    return python_to_glog.get(log_level, 1)


def str2bool(s):
    if isinstance(s, bool):
        return s
    if s.lower() in ("yes", "true", "t", "y", "1"):
        return True
    return False


class ResolveMPICmdPrefix(object):
    """
    Class for resolving prefix of mpi command.

    Examples:

    .. code:: ipython

        >>> # openmpi found
        >>> rmcp = ResolveMPICmdPrefix()
        >>> (cmd, env) = rmcp.resolve(4, 'h1, h2, h3')
        >>> cmd
        ['mpirun', '--allow-run-as-root',
         '-n', '4', '-host', 'h1:2,h2:1,h3:1']

        >>> env
        {'OMPI_MCA_plm_rsh_agent': '/usr/bin/kube_ssh', # if /usr/bin/kube_ssh in $PATH
         'OMPI_MCA_btl_vader_single_copy_mechanism': 'none',
         'OMPI_MCA_orte_allowed_exit_without_sync': '1'}

        >>> # if openmpi not found, change to mpich
        >>> rmcp = ResolveMPICmdPrefix()
        >>> (cmd, env) = rmcp.resolve(4, 'h1, h2, h3')
        >>> cmd
        ['mpirun', '-n', '4', '-host', 'h1:2,h2:1,h3:1']
        >>> env
        {} # always empty
    """

    _OPENMPI_RSH_AGENT = "OMPI_MCA_plm_rsh_agent"
    _KUBE_SSH_EXEC = "kube_ssh"

    def __init__(self, rsh_agent=False):
        self._rsh_agent = rsh_agent

    @staticmethod
    def openmpi():
        try:
            subprocess.check_call(["ompi_info"], stdout=subprocess.DEVNULL)
        except FileNotFoundError:
            return False
        return True

    @staticmethod
    def alloc(num_workers, hosts):
        host_list = hosts.split(",")
        host_list_len = len(host_list)
        assert host_list_len != 0

        host_to_proc_num = {}
        if num_workers >= host_list_len:
            quotient = num_workers / host_list_len
            residue = num_workers % host_list_len
            for host in host_list:
                if residue > 0:
                    host_to_proc_num[host] = quotient + 1
                    residue -= 1
                else:
                    host_to_proc_num[host] = quotient
        else:
            raise RuntimeError("The number of hosts less then num_workers")

        for i in range(host_list_len):
            host_list[i] = "{}:{}".format(
                host_list[i], int(host_to_proc_num[host_list[i]])
            )

        return ",".join(host_list)

    def resolve(self, num_workers, hosts):
        cmd = []
        env = {}

        if self.openmpi():
            env["OMPI_MCA_btl_vader_single_copy_mechanism"] = "none"
            env["OMPI_MCA_orte_allowed_exit_without_sync"] = "1"
            # OMPI sends SIGCONT -> SIGTERM -> SIGKILL to the worker process,
            # set the following MCA parameter to zero will emilinates the chances
            # where the process dies before receiving the SIGTERM and do cleanup.
            env["OMPI_MCA_odls_base_sigkill_timeout"] = "0"

            if os.environ.get(self._OPENMPI_RSH_AGENT) is None:
                rsh_agent_path = shutil.which(self._KUBE_SSH_EXEC)
                if self._rsh_agent and rsh_agent_path is not None:
                    env[self._OPENMPI_RSH_AGENT] = rsh_agent_path
            cmd.extend(
                [
                    "mpirun",
                    "--allow-run-as-root",
                ]
            )
        else:
            # ssh agent supported only
            cmd.extend(["mpirun"])
        cmd.extend(["-n", str(num_workers)])
        cmd.extend(["-host", self.alloc(num_workers, hosts)])

        logger.debug("Resolve mpi cmd prefix: {}".format(cmd))
        logger.debug("Resolve mpi env: {}".format(env))
        return (cmd, env)


# In Analytical engine, assume label ids of vertex entries are continuous
# from zero, and property ids of each label is also continuous from zero.
# When transform schema to Maxgraph style, we gather all property names and
# unique them, assign each name a id (index of the vector), then preserve a
# vector<int> for each label, stores mappings from original id to transformed
# id.
def to_maxgraph_schema(gsa_schema_json):
    gsa_schema = json.loads(gsa_schema_json)
    prop_set = set()
    vertex_label_num = 0
    for item in gsa_schema["types"]:
        item["id"] = int(item["id"])
        if item["type"] == "VERTEX":
            vertex_label_num += 1
        for prop in item["propertyDefList"]:
            prop["id"] = int(prop["id"])
            prop_set.add(prop["name"])

    prop_list = sorted(list(prop_set))
    mg_schema = copy.deepcopy(gsa_schema)
    for item in mg_schema["types"]:
        if item["propertyDefList"] == "":
            item["propertyDefList"] = []
        if item["type"] == "VERTEX":
            for prop in item["propertyDefList"]:
                prop["id"] = 1 + prop_list.index(prop["name"])
        elif item["type"] == "EDGE":
            item["id"] = vertex_label_num + item["id"]
            for prop in item["propertyDefList"]:
                prop["id"] = 1 + prop_list.index(prop["name"])
    return json.dumps(mg_schema)


def check_argument(condition, message=None):
    if not condition:
        if message is None:
            message = "in '%s'" % inspect.stack()[1].code_context[0]
        raise ValueError("Check failed: %s" % message)