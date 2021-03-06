# Copyright 2016 Husky Team
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


include_directories(${PROJECT_SOURCE_DIR} ${Boost_INCLUDE_DIRS})

add_subdirectory(channel)

if(DEP_FOUND)
    include_directories(${ZMQ_INCLUDE_DIR})
endif(DEP_FOUND)

add_library(core-objs OBJECT
    accessor.cpp
    accessor_factory.cpp
    combiner.cpp
    config.cpp
    context.cpp
    coordinator.cpp
    hash_ring.cpp
    job_runner.cpp
    mailbox.cpp
    network.cpp
    objlist_factory.cpp
    shuffle_combiner_factory.cpp
    worker_info.cpp)
set_property(TARGET core-objs PROPERTY CXX_STANDARD 14)

add_library(husky-core STATIC $<TARGET_OBJECTS:core-objs> $<TARGET_OBJECTS:channel-objs>)
