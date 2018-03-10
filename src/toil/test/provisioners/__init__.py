# Copyright (C) 2015-2018 Regents of the University of California
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
from __future__ import absolute_import
from abc import abstractmethod

from toil import subprocess
from toil.test import ToilTest


class AbstractProvisionerTest(ToilTest):
    def __init__(self, provisioner, provisionerName, **kwargs):
        super(AbstractProvisionerTest, self).__init__(methodName=kwargs['methodName'])
        self.provisioner = provisioner
        # Ideally, provisionerName should be an attribute of provisioner.
        # I think that that would make the clusterutil refactor easier.
        self.provisionerName = provisionerName

    # TODO: Should the provisioner's name be specified in the provisioner class?
    def sshUtil(self, command):
        baseCommand = ['toil', 'ssh-cluster', '--insecure', '-p=%s' % self.provisionerName, self.clusterName]
        callCommand = baseCommand + command
        subprocess.check_call(callCommand)

    def rsyncUtil(self, src, dest):
        baseCommand = ['toil', 'rsync-cluster', '--insecure', '-p=%s' % self.provisionerName, self.clusterName]
        callCommand = baseCommand + [src, dest]
        subprocess.check_call(callCommand)

    def destroyClusterUtil(self):
        callCommand = ['toil', 'destroy-cluster', '-p=%s' % self.provisionerName, self.clusterName]
        subprocess.check_call(callCommand)

    def createClusterUtil(self, args=None):
        if args is None:
            args = []
        callCommand = ['toil', 'launch-cluster', '-p=%s' % self.provisionerName, '--keyPairName=%s' % self.keyName,
                       '--leaderNodeType=%s' % self.leaderInstanceType, self.clusterName]
        callCommand = callCommand + args if args else callCommand
        subprocess.check_call(callCommand)

    def cleanJobStoreUtil(self):
        callCommand = ['toil', 'clean', self.jobStore]
        subprocess.check_call(callCommand)

    def setUp(self):
        super(AbstractProvisionerTest, self).setUp()

    def tearDown(self):
        super(AbstractProvisionerTest, self).tearDown()
        self.destroyClusterUtil()
        self.cleanJobStoreUtil()

    def launchCluster(self):
        self.createClusterUtil()

    @abstractmethod
    def _getScript(self):
        """
        Download the test script needed by the inheriting unit test class.
        """
        raise NotImplementedError()

    @abstractmethod
    def _runScript(self, toilOptions):
        """
        Modify the provided Toil options to suit the test Toil script, then run the script with
        those arguments.

        :param toilOptions: List of Toil command line arguments. This list may need to be
               modified to suit the test script's requirements.
        """
        raise NotImplementedError()
