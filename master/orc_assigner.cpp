// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifdef  WITH_ORC

#include <fstream>
#include <string>
#include <utility>

#include "master/orc_assigner.hpp"

#include "boost/filesystem.hpp"
#include "orc/OrcFile.hh"


#include "core/constants.hpp"
#include "core/context.hpp"
#include "master/master.hpp"
#include "base/log.hpp"

namespace husky {
using orc::Reader;
using orc::ReaderOptions;
using orc::createReader;
using orc::readLocalFile;
using orc::ColumnVectorBatch;

static ORCBlockAssigner orc_block_assigner;

ORCBlockAssigner::ORCBlockAssigner() {
    Master::get_instance().register_main_handler(TYPE_ORC_BLK_REQ,
                                                 std::bind(&ORCBlockAssigner::master_main_handler, this));
    Master::get_instance().register_setup_handler(std::bind(&ORCBlockAssigner::master_setup_handler, this));
}

void ORCBlockAssigner::master_main_handler() {
    auto& master = Master::get_instance();
    auto resp_socket = master.get_socket();
    std::string url;
    BinStream stream = zmq_recv_binstream(resp_socket.get());
    stream >> url;

    std::pair<std::string, size_t> ret = answer(url);
    stream.clear();
    stream << ret.first << ret.second;

    zmq_sendmore_string(resp_socket.get(), master.get_cur_client());
    zmq_sendmore_dummy(resp_socket.get());
    zmq_send_binstream(resp_socket.get(), stream);

    base::log_msg(" => " + ret.first + "@" + std::to_string(ret.second));
}

void ORCBlockAssigner::master_setup_handler() { num_workers_alive = Context::get_worker_info()->get_num_workers(); }

void ORCBlockAssigner::browse_local(const std::string& url) {
    // If url is a directory, recursively traverse all files in url
    // TODO(yidi) here I assume that url is a valid file
    try {
        if (boost::filesystem::exists(url)) {
            if (boost::filesystem::is_regular_file(url)) {
                std::unique_ptr<Reader> reader;
                ReaderOptions opts;
                reader = createReader(readLocalFile(url), opts);
                int rows_per_stripe = reader->getStripe(0)->getNumberOfRows();
                if ( rows_per_stripe != 0 ) {
                    row_batch_size = rows_per_stripe;
                }
                file_size[url] = reader->getNumberOfRows();
                file_offset[url] = 0;
                finish_dict[url] = 0;
            } else if (boost::filesystem::is_directory(url)) {
                // url should also be included in the finish_dict
                finish_dict[url] = 0;
                for (auto files : boost::filesystem::directory_iterator(url)) {
                    std::string path = files.path().string();
                    if (boost::filesystem::is_regular_file(path)) {
                        std::unique_ptr<Reader> reader;
                        ReaderOptions opts;
                        reader = createReader(readLocalFile(path),opts);
                        int rows_per_stripe = reader->getStripe(0)->getNumberOfRows();

                        file_size[path] = reader->getNumberOfRows();
                        file_offset[path] = 0;
                        finish_dict[path] = 0;
                    }
                }
            } else {
                base::log_msg("Given url:"+ url +" is not a regular file or diercotry");
            }
        } else {
            base::log_msg("Given url:"+ url +" doesn't exist!");
        }
    } catch (const std::exception& ex) {
        base::log_msg("Exception cought: ");
        base::log_msg(ex.what());
    }
}

std::pair<std::string, size_t> ORCBlockAssigner::answer(std::string& url) {
    // Directory or file status initialization
    // This condition is true either when the begining of the file or 
    // all the workers has finished reading this file or directory
    if (finish_dict.find(url) == finish_dict.end())
        browse_local(url);

    std::pair<std::string, size_t> ret = {"", 0};  // selected_file, offset
    if (boost::filesystem::is_regular_file(url)) {
        if (file_offset[url] < file_size[url]) {
            ret.first = url;
            ret.second = file_offset[url];
            file_offset[url] += row_batch_size;
        } 
    } else if (boost::filesystem::is_directory(url)) {
        for (auto files : boost::filesystem::directory_iterator(url)) {
            std::string path = files.path().string();
            // if this file hasn't been finished
            if (finish_dict.find(path) != finish_dict.end()) {
                if (file_offset[path] < file_size[path]) {
                    ret.first = path;
                    ret.second = file_offset[path];
                    file_offset[path] += row_batch_size;
                    // no need to continue searching for next file
                    break;
                } else {
                    finish_dict[path] += 1;
                    if (finish_dict[path] == num_workers_alive) {
                        finish_url(path);
                    }
                    // need to search for next file
                    continue;
                }
            } 
        }
    }
    // count how many workers won't ask for an answer
    if (ret.first == "" && ret.second == 0) {
        finish_dict[url] += 1;
        if (finish_dict[url] == num_workers_alive) {
            finish_url(url);
        }
    }
    // Once ret hasn't been assigned value, answer(url) will not be called anymore.
    // Reason:
    // ret = {"", 0} => splitter: fetch_block return "" => inputformat: next return false =>
    // executor: load break loop
    return ret;
}

/// Return the number of workers who have finished reading the files in
/// the given url
int ORCBlockAssigner::get_num_finished(std::string& url) { return finish_dict[url]; }

/// Use this when all workers finish reading the files in url
void ORCBlockAssigner::finish_url(std::string& url) {
    file_size.erase(url);
    file_offset.erase(url);
    finish_dict.erase(url);
}

}  // namespace husky
#endif
