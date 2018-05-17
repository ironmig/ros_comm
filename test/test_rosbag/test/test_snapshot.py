#!/usr/bin/env python
# Software License Agreement (BSD License)
#
# Copyright (c) 2008, Willow Garage, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above
#    copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials provided
#    with the distribution.
#  * Neither the name of Willow Garage, Inc. nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import hashlib
import heapq
import os
import shutil
import sys
import tempfile
import time
import unittest

import genpy

import rosbag
from rosbag import bag
import rospy
from std_msgs.msg import Int32
from std_msgs.msg import ColorRGBA
from std_msgs.msg import String
from rosbag.srv import TriggerSnapshot, TriggerSnapshotRequest

class TestRosbagSnapshot(unittest.TestCase):
    def __init__(self, *args):
        #self.pub1 = rospy.Publisher('/test1', Int32, queue_size=10)
        #self.pub2 = rospy.Publisher('/test2', String, queue_size=10)
        self.trigger = rospy.ServiceProxy("/trigger_snapshot", TriggerSnapshot)
        super(TestRosbagSnapshot, self).__init__(*args)

    def test_1_service_connects(self):
        self.trigger.wait_for_service(timeout=1.0)

    def test_2_invalid_topic_fails(self):
        filename = '/tmp/should_not_exist.bag'
        res = self.trigger(filename=filename, topics=['>43?'])
        self.assertFalse(res.success)
        self.assertFalse(os.path.isfile(filename))

    def test_3_write_success(self):
        rospy.sleep(0.5) # Give some time to get data
        filename = '/tmp/test.bag'
        req = TriggerSnapshotRequest(filename=filename)
        req.topics.append('/test')
        req.topics.append('/test2')
        res = self.trigger(req)
        self.assertTrue(res.success, msg="Snapshot should have succeeded. Message: {}".format(res.message))
        self.assertTrue(res.message == "")
        self.assertTrue(os.path.isfile(filename))


if __name__ == '__main__':
    import rostest
    PKG='rosbag'
    rospy.init_node('test_rosbag_snapshot', anonymous=True)
    rostest.run(PKG, 'TestRosbagSnapshot', TestRosbagSnapshot, sys.argv)
