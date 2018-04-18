#!/usr/bin/env python

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

from __future__ import print_function
import logging
from optparse import OptionParser
import os
import re
import subprocess
import sys
import tempfile
from threading import Thread, Lock
import time
if sys.version < '3':
    import Queue
else:
    import queue as Queue
from distutils.version import LooseVersion


# Append `SPARK_HOME/dev` to the Python path so that we can import the sparktestsupport module
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../dev/"))


from sparktestsupport import SPARK_HOME  # noqa (suppress pep8 warnings)
from sparktestsupport.shellutils import which, subprocess_check_output  # noqa
from sparktestsupport.modules import all_modules, pyspark_sql  # noqa


python_modules = dict((m.name, m) for m in all_modules if m.python_test_goals if m.name != 'root')


def print_red(text):
    print('\033[31m' + text + '\033[0m')


LOG_FILE = os.path.join(SPARK_HOME, "python/unit-tests.log")
FAILURE_REPORTING_LOCK = Lock()
LOGGER = logging.getLogger()

# Find out where the assembly jars are located.
# Later, add back 2.12 to this list:
# for scala in ["2.11", "2.12"]:
for scala in ["2.11"]:
    build_dir = os.path.join(SPARK_HOME, "assembly", "target", "scala-" + scala)
    if os.path.isdir(build_dir):
        SPARK_DIST_CLASSPATH = os.path.join(build_dir, "jars", "*")
        break
else:
    raise Exception("Cannot find assembly build directory, please build Spark first.")


def run_individual_python_test(test_name, pyspark_python):
    env = dict(os.environ)
    env.update({
        'SPARK_DIST_CLASSPATH': SPARK_DIST_CLASSPATH,
        'SPARK_TESTING': '1',
        'SPARK_PREPEND_CLASSES': '1',
        'PYSPARK_PYTHON': which(pyspark_python),
        'PYSPARK_DRIVER_PYTHON': which(pyspark_python)
    })
    LOGGER.info("Starting test(%s): %s", pyspark_python, test_name)
    start_time = time.time()
    try:
        per_test_output = tempfile.TemporaryFile()
        retcode = subprocess.Popen(
            [os.path.join(SPARK_HOME, "bin/pyspark"), test_name],
            stderr=per_test_output, stdout=per_test_output, env=env).wait()
    except:
        LOGGER.exception("Got exception while running %s with %s", test_name, pyspark_python)
        # Here, we use os._exit() instead of sys.exit() in order to force Python to exit even if
        # this code is invoked from a thread other than the main thread.
        os._exit(1)
    duration = time.time() - start_time
    # Exit on the first failure.
    if retcode != 0:
        try:
            with FAILURE_REPORTING_LOCK:
                with open(LOG_FILE, 'ab') as log_file:
                    per_test_output.seek(0)
                    log_file.writelines(per_test_output)
                per_test_output.seek(0)
                for line in per_test_output:
                    decoded_line = line.decode()
                    if not re.match('[0-9]+', decoded_line):
                        print(decoded_line, end='')
                per_test_output.close()
        except:
            LOGGER.exception("Got an exception while trying to print failed test output")
        finally:
            print_red("\nHad test failures in %s with %s; see logs." % (test_name, pyspark_python))
            # Here, we use os._exit() instead of sys.exit() in order to force Python to exit even if
            # this code is invoked from a thread other than the main thread.
            os._exit(-1)
    else:
        per_test_output.close()
        LOGGER.info("Finished test(%s): %s (%is)", pyspark_python, test_name, duration)


def get_default_python_executables():
    python_execs = [x for x in ["python2.7", "python3.4", "pypy"] if which(x)]
    if "python2.7" not in python_execs:
        LOGGER.warning("Not testing against `python2.7` because it could not be found; falling"
                       " back to `python` instead")
        python_execs.insert(0, "python")
    return python_execs


def parse_opts():
    parser = OptionParser(
        prog="run-tests"
    )
    parser.add_option(
        "--python-executables", type="string", default=','.join(get_default_python_executables()),
        help="A comma-separated list of Python executables to test against (default: %default)"
    )
    parser.add_option(
        "--modules", type="string",
        default=",".join(sorted(python_modules.keys())),
        help="A comma-separated list of Python modules to test (default: %default)"
    )
    parser.add_option(
        "-p", "--parallelism", type="int", default=4,
        help="The number of suites to test in parallel (default %default)"
    )
    parser.add_option(
        "--verbose", action="store_true",
        help="Enable additional debug logging"
    )

    (opts, args) = parser.parse_args()
    if args:
        parser.error("Unsupported arguments: %s" % ' '.join(args))
    if opts.parallelism < 1:
        parser.error("Parallelism cannot be less than 1")
    return opts


def _check_dependencies(python_exec, modules_to_test):
    if "COVERAGE_PROCESS_START" in os.environ:
        # Make sure if coverage is installed.
        try:
            subprocess_check_output(
                [python_exec, "-c", "import coverage"],
                stderr=open(os.devnull, 'w'))
        except:
            print_red("Coverage is not installed in Python executable '%s' "
                      "but 'COVERAGE_PROCESS_START' environment variable is set, "
                      "exiting." % python_exec)
            sys.exit(-1)

    # If we should test 'pyspark-sql', it checks if PyArrow and Pandas are installed and
    # explicitly prints out. See SPARK-23300.
    if pyspark_sql in modules_to_test:
        # TODO(HyukjinKwon): Relocate and deduplicate these version specifications.
        minimum_pyarrow_version = '0.8.0'
        minimum_pandas_version = '0.19.2'

        try:
            pyarrow_version = subprocess_check_output(
                [python_exec, "-c", "import pyarrow; print(pyarrow.__version__)"],
                universal_newlines=True,
                stderr=open(os.devnull, 'w')).strip()
            if LooseVersion(pyarrow_version) >= LooseVersion(minimum_pyarrow_version):
                LOGGER.info("Will test PyArrow related features against Python executable "
                            "'%s' in '%s' module." % (python_exec, pyspark_sql.name))
            else:
                LOGGER.warning(
                    "Will skip PyArrow related features against Python executable "
                    "'%s' in '%s' module. PyArrow >= %s is required; however, PyArrow "
                    "%s was found." % (
                        python_exec, pyspark_sql.name, minimum_pyarrow_version, pyarrow_version))
        except:
            LOGGER.warning(
                "Will skip PyArrow related features against Python executable "
                "'%s' in '%s' module. PyArrow >= %s is required; however, PyArrow "
                "was not found." % (python_exec, pyspark_sql.name, minimum_pyarrow_version))

        try:
            pandas_version = subprocess_check_output(
                [python_exec, "-c", "import pandas; print(pandas.__version__)"],
                universal_newlines=True,
                stderr=open(os.devnull, 'w')).strip()
            if LooseVersion(pandas_version) >= LooseVersion(minimum_pandas_version):
                LOGGER.info("Will test Pandas related features against Python executable "
                            "'%s' in '%s' module." % (python_exec, pyspark_sql.name))
            else:
                LOGGER.warning(
                    "Will skip Pandas related features against Python executable "
                    "'%s' in '%s' module. Pandas >= %s is required; however, Pandas "
                    "%s was found." % (
                        python_exec, pyspark_sql.name, minimum_pandas_version, pandas_version))
        except:
            LOGGER.warning(
                "Will skip Pandas related features against Python executable "
                "'%s' in '%s' module. Pandas >= %s is required; however, Pandas "
                "was not found." % (python_exec, pyspark_sql.name, minimum_pandas_version))


def main():
    opts = parse_opts()
    if (opts.verbose):
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(stream=sys.stdout, level=log_level, format="%(message)s")
    LOGGER.info("Running PySpark tests. Output is in %s", LOG_FILE)
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    python_execs = opts.python_executables.split(',')
    modules_to_test = []
    for module_name in opts.modules.split(','):
        if module_name in python_modules:
            modules_to_test.append(python_modules[module_name])
        else:
            print("Error: unrecognized module '%s'. Supported modules: %s" %
                  (module_name, ", ".join(python_modules)))
            sys.exit(-1)
    LOGGER.info("Will test against the following Python executables: %s", python_execs)
    LOGGER.info("Will test the following Python modules: %s", [x.name for x in modules_to_test])

    task_queue = Queue.PriorityQueue()
    for python_exec in python_execs:
        # Check if the python executable has proper dependencies installed to run tests
        # for given modules properly.
        _check_dependencies(python_exec, modules_to_test)

        python_implementation = subprocess_check_output(
            [python_exec, "-c", "import platform; print(platform.python_implementation())"],
            universal_newlines=True).strip()
        LOGGER.debug("%s python_implementation is %s", python_exec, python_implementation)
        LOGGER.debug("%s version is: %s", python_exec, subprocess_check_output(
            [python_exec, "--version"], stderr=subprocess.STDOUT, universal_newlines=True).strip())
        for module in modules_to_test:
            if python_implementation not in module.blacklisted_python_implementations:
                for test_goal in module.python_test_goals:
                    if test_goal in ('pyspark.streaming.tests', 'pyspark.mllib.tests',
                                     'pyspark.tests', 'pyspark.sql.tests'):
                        priority = 0
                    else:
                        priority = 100
                    task_queue.put((priority, (python_exec, test_goal)))

    def process_queue(task_queue):
        while True:
            try:
                (priority, (python_exec, test_goal)) = task_queue.get_nowait()
            except Queue.Empty:
                break
            try:
                run_individual_python_test(test_goal, python_exec)
            finally:
                task_queue.task_done()

    start_time = time.time()
    for _ in range(opts.parallelism):
        worker = Thread(target=process_queue, args=(task_queue,))
        worker.daemon = True
        worker.start()
    try:
        task_queue.join()
    except (KeyboardInterrupt, SystemExit):
        print_red("Exiting due to interrupt")
        sys.exit(-1)
    total_duration = time.time() - start_time
    LOGGER.info("Tests passed in %i seconds", total_duration)


if __name__ == "__main__":
    main()
