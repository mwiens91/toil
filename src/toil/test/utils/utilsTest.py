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

from builtins import str
import os
import sys
import uuid
import shutil
import tempfile
import pytest
import toil
import logging
import toil.test.sort.sort
from toil import subprocess
from toil import resolveEntryPoint
from toil.job import Job
from toil.test import ToilTest, needs_aws, needs_rsync3, integrative, slow
from toil.test.sort.sortTest import makeFileToSort
from toil.utils.toilStats import getStats, processData
from toil.common import Toil, Config
from toil.provisioners.aws.awsProvisioner import AWSProvisioner
from toil.lib.bioio import getBasicOptionParser
log = logging.getLogger(__name__)


class ClusterUtilsTest(ToilTest):

    def setUp(self):
        self.keyName = 'id_rsa' # os.getenv('TOIL_AWS_KEYNAME') # 'jenkins@jenkins-master'
        self.toilMain = resolveEntryPoint('toil')
        self.clusterName = 'test-cluster' + str(uuid.uuid4())
        self.dummyFile2Rsync = ''

    def tearDown(self):
        ToilTest.tearDown(self)

    def testLocalClusterUtilities(self):
        pass

    def testAWSClusterUtilities(self):
        self.toilLaunchCluster(provisioner='aws',
                               instance_type='t2.micro',
                               extraArgs=[])
        self.toilRsyncCluster(provisioner='aws',
                              fileToUpload=self.dummyFile2Rsync)
        self.toilSshCluster(provisioner='aws')
        self.toilDestroyCluster(provisioner='aws')

    def testGCEClusterUtilities(self):
        pass

    def testAzureClusterUtilities(self):
        pass

    def testLocalClean(self):
        pass

    def testAWSClean(self):
        pass

    def testGCEClean(self):
        pass

    def testAzureClean(self):
        pass

    def toilLaunchCluster(self, provisioner, instance_type, extraArgs):
        cmd = "{toil} launch-cluster {clustername} " \
                                    "--provisioner={provisioner} " \
                                    "--leaderNodeType={instance_type} " \
                                    "--keyPairName={keypair}".format(toil=self.toilMain,
                                                                     clustername=self.clusterName,
                                                                     provisioner=provisioner,
                                                                     instance_type=instance_type,
                                                                     keypair=self.keyName)
        cmd = cmd + ' '.join(extraArgs)
        log.info('Running: %s', ' '.join(cmd))
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        # log.info(stdout)
        # log.info(stderr)


    def toilRsyncCluster(self, provisioner, fileToUpload):
        cmd = "{toil} rsync-cluster {clustername} " \
                                   "--provisioner={provisioner} " \
                                   "--insecure " \
                                   "{uploadfile}".format(toil=self.toilMain,
                                                         clustername=self.clusterName,
                                                         provisioner=provisioner,
                                                         uploadfile=fileToUpload)
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

    def toilSshCluster(self, provisioner):
        cmd = "{toil} ssh-cluster {clustername} " \
                                 "--provisioner={provisioner} " \
                                 "--insecure".format(toil=self.toilMain,
                                                     clustername=self.clusterName,
                                                     provisioner=provisioner)
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

    def toilDestroyCluster(self, provisioner):
        cmd = "{toil} destroy-cluster {clustername} " \
                                     "--provisioner={provisioner}".format(toil=self.toilMain,
                                                                          clustername=self.clusterName,
                                                                          provisioner=provisioner)
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

    def toilClean(self, jobstore):
        cmd = "{toil} clean {jobstore}".format(toil=self.toilMain,
                                               jobstore=jobstore)
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

    def toilStats(self, toilscript, jobstore):
        """
        Workflows are run with --stats generate special stats logs readable by status.

        :param jobstore:
        :return:
        """
        cmd = "{toil} {toilscript} {jobstore} --stats".format(toil=self.toilMain,
                                                              toilscript=toilscript,
                                                              jobstore=jobstore)
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

    def toilStatus(self, jobstore):
        """
        Workflows run with --stats can then have their jobstore stats logs inspected by status.

        Workflows run without --stats return nothing.

        :param jobstore:
        :return:
        """
        cmd = "{toil} status {jobstore}".format(toil=self.toilMain,
                                                jobstore=jobstore)
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

    # misc nonsense?
    # def testAWSAddTags(self):
    #     # check for these tags
    #     userAddedTags = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}
    #     # default tags added to every cluster
    #     defaultTags = {'Name': self.clusterName, 'Owner': self.keyName}
    #     # add the defaults in to the user added stuff to check that this matches
    #     userAddedTags.update(defaultTags)
    #
    #     self.toilLaunchCluster(provisioner='aws',
    #                            instance_type='t2.micro',
    #                            extraArgs=['-t', 'key1=value1',
    #                                       '-t', 'key2=value2',
    #                                       '--tag', 'key3=value3'])
    #     leaderTags = AWSProvisioner._getLeader(self.clusterName).tags
    #     self.assertEqual(defaultTags, leaderTags)

        # # Add the host key to known_hosts so that the rest of the tests can
        # # pass without choking on the verification prompt.
        # AWSProvisioner.sshLeader(clusterName=self.clusterName, strict=True, sshOptions=['-oStrictHostKeyChecking=no'])
