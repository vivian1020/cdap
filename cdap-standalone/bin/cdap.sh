#!/usr/bin/env bash

#
# Copyright © 2014-2016 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

# This script is a wrapper for "cdap sdk" and will be removed in 5.0
echo
echo "[WARN] ${0} is deprecated and will be removed in CDAP 5.0. Please use 'cdap sdk' to manage Standalone CDAP."
echo
echo "  cdap sdk ${@}"
echo
echo

__script=${BASH_SOURCE[0]}

__target=$(readlink ${__script}) # TODO: readlink isn't portable, if we support more than Linux/macOS
if [[ $? -ne 0 ]]; then
  __target=${__script} # no symlink
fi
__app_home=$(cd $(dirname ${__target})/.. >&-; pwd -P)
${__app_home}/bin/cdap sdk ${@}
