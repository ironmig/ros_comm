/*********************************************************************
* Software License Agreement (BSD License)
*
*  Copyright (c) 2008, Willow Garage, Inc.
*  All rights reserved.
*
*  Redistribution and use in source and binary forms, with or without
*  modification, are permitted provided that the following conditions
*  are met:
*
*   * Redistributions of source code must retain the above copyright
*     notice, this list of conditions and the following disclaimer.
*   * Redistributions in binary form must reproduce the above
*     copyright notice, this list of conditions and the following
*     disclaimer in the documentation and/or other materials provided
*     with the distribution.
*   * Neither the name of Willow Garage, Inc. nor the names of its
*     contributors may be used to endorse or promote products derived
*     from this software without specific prior written permission.
*
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
*  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
*  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
*  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
*  COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
*  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
*  BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
*  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
*  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
*  LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
*  ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
*  POSSIBILITY OF SUCH DAMAGE.
********************************************************************/

#include "rosbag/snapshoter.h"
#include "rosbag/exceptions.h"

#include "boost/program_options.hpp"

namespace po = boost::program_options;

using rosbag::Snapshoter;
using rosbag::SnapshoterOptions;
using rosbag::SnapshoterTopicOptions;

//! Parse the command-line arguments for recorder options
SnapshoterOptions parseOptions(int argc, char** argv) {
    /// TODO: program options for stuff
    SnapshoterOptions opts(ros::Duration(10.0), SnapshoterTopicOptions::NO_MEMORY_LIMIT);
    return opts;
}

/* Read configured topics from by reading ~topics ROS param.
 * TODO: use exceptions instead of asserts to follow style conventions
 * This param should be set in the following (YAML represented) structure
 *   <rosparam>
 *       topics:                   # List of topics
 *           - /topic1             # Topic which will adopt default memory and duration limits
 *           - topic2:             # Topic with overriden memory and duration limit
 *               memory: 5000      # 5000 Byte limit on buffered data from this topic
 *               duration: 30      # 30 second duration limit between newest and oldest message from this topic
 *   </rosparam>
 */
void appendParamOptions(SnapshoterOptions& opts)
{
    using XmlRpc::XmlRpcValue;
    XmlRpcValue topics;
    if(!ros::param::get("~topics", topics))
    {
        ROS_INFO("NO PARAM TOPICS");
        return;
    }
    ROS_ASSERT_MSG(topics.getType() == XmlRpcValue::TypeArray, "topics param must be an array");
    // Iterator caused exception, hmmm...
    size_t size = topics.size();
    for (size_t i = 0; i < size; ++i)
    {
        XmlRpcValue topic_value = topics[i];
        // If it is just a string, add this topic
        if (topic_value.getType() == XmlRpcValue::TypeString)
        {
            opts.addTopic(topic_value);
        }
        else if (topic_value.getType() == XmlRpcValue::TypeStruct)
        {
            ROS_ASSERT_MSG(topic_value.size() == 1, "Paramater invalid for topic %lu", i);
            std::string const& topic = (*topic_value.begin()).first;
            XmlRpcValue& topic_config = (*topic_value.begin()).second;
            ROS_ASSERT_MSG(topic_config.getType() == XmlRpcValue::TypeStruct, "Topic limits invalid for: '%s'", topic.c_str());

            ros::Duration dur = SnapshoterTopicOptions::INHERIT_DURATION_LIMIT;
            int64_t mem = SnapshoterTopicOptions::INHERIT_MEMORY_LIMIT;
            std::string duration = "duration";
            std::string memory = "memory";
            if (topic_config.hasMember(duration))
            {
                XmlRpcValue& dur_limit = topic_config[duration];
                if (dur_limit.getType() == XmlRpcValue::TypeDouble)
                {
                    double seconds = dur_limit;
                    dur = ros::Duration(seconds);
                }
                else if (dur_limit.getType() == XmlRpcValue::TypeInt)
                {
                    int seconds = dur_limit;
                    dur = ros::Duration(seconds, 0);
                }
                else ROS_FATAL("err");
            }
            if (topic_config.hasMember("memory"))
            {
                XmlRpcValue& mem_limit = topic_config[memory];
                ROS_ASSERT_MSG(mem_limit.getType() == XmlRpcValue::TypeInt, "Memory limit is not an int for topic '%s'", topic.c_str());
                int tmp = mem_limit;
                mem = tmp;
            }
            opts.addTopic(topic, dur, mem);
        }
        else ROS_ASSERT_MSG(false, "Parameter invalid for topic %lu", i);
    }
}

int main(int argc, char** argv) {
    ros::init(argc, argv, "snapshot");//, ros::init_options::AnonymousName);  

    // Parse the command-line options
    SnapshoterOptions opts;
    try {
        opts = parseOptions(argc, argv);
    }
    catch (ros::Exception const& ex) {
        ROS_ERROR("Error reading options: %s", ex.what());
        return 1;
 
    }

    // Get additional topic configurations if they're in ROS params
    appendParamOptions(opts);

    // Run the snapshoter
    rosbag::Snapshoter snapshoter(opts);
    int result = snapshoter.run();
    return result;
}
