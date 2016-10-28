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

#include <algorithm>
#include <string>
#include <vector>

#include "boost/tokenizer.hpp"

#include "core/engine.hpp"
#include "io/hdfs_manager.hpp"
#include "io/input/hdfs_line_inputformat.hpp"
#include "lib/aggregator_factory.hpp"

using husky::lib::Aggregator;

std::string ghdfs_dest;

class Group {
   public:
    using KeyT = std::string;

    Group() = default;
    explicit Group(const std::string& k) : key(k) {}
    const KeyT& id() { return key; }

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, const Group& g) {
        stream << g.records << g.key;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Group& g) {
        stream >> g.records >> g.key;
        return stream;
    }

    KeyT key;
    std::vector<std::string> records;
};

void cube() {
    // Get group set masks
    std::string schema_conf = husky::Context::get_param("schema");
    std::string select_conf = husky::Context::get_param("select");
    std::string group_conf = husky::Context::get_param("group_sets");
    ghdfs_dest = husky::Context::get_param("output");
    boost::char_separator<char> comma_sep(",");
    boost::char_separator<char> colon_sep(":");
    boost::tokenizer<boost::char_separator<char>> schema_tok(schema_conf, comma_sep);
    boost::tokenizer<boost::char_separator<char>> select_tok(select_conf, comma_sep);
    boost::tokenizer<boost::char_separator<char>> group_set_tok(group_conf, colon_sep);

     std::vector<std::string> schema_vec(schema_tok.begin(), schema_tok.end());
    // Convert select to indices
    std::vector<int> select;
    for (auto& s : select_tok) {
        auto it = std::find(schema_vec.begin(), schema_vec.end(), s);
        if (it != schema_vec.end()) {
            select.push_back(std::distance(schema_vec.begin(), it));
        }
        // TODO(Ruihao): Throw expection if input is wrong?
    }
    // Convert group sets to indices
    size_t group_set_size = std::distance(group_set_tok.begin(), group_set_tok.end());
    std::vector<std::vector<int>> group_sets(group_set_size);
    int i = 0;
    for (auto& group : group_set_tok) {
        boost::tokenizer<boost::char_separator<char>> colomn_tok(group, comma_sep);
        for (auto column : colomn_tok) {
            auto it = std::find(schema_vec.begin(), schema_vec.end(), column);
            if (it != schema_vec.end()) {
                group_sets[i].push_back(std::distance(schema_vec.begin(), it));
            }
            // TODO(Ruihao): Throw expection if input is wrong?
        }
        ++i;
    }
    // Get the index of "fuid" column
    int fuid_index;
    auto it = std::find(schema_vec.begin(), schema_vec.end(), "fuid");
    if (it != schema_vec.end()) {
        fuid_index = std::distance(schema_vec.begin(), it);
    }
    // TODO(Ruihao): Same

    // Load input and emit key -> uid
    husky::io::HDFSLineInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));

    auto& group_list = husky::ObjListFactory::create_objlist<Group>();
    auto& ch = husky::ChannelFactory::create_push_channel<std::string>(infmt, group_list);

    Aggregator<int> agg(0, [](int& a, const int& b) { a += b; });
    auto& agg_ch = husky::lib::AggregatorFactory::get_channel();

    auto parser = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        boost::char_separator<char> sep("\t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        std::string key = "";
        std::string fuid;
        for (size_t i = 0; i < group_sets.size(); ++i) {
            int j = 0;
            for (auto val : tok) {
                // j-th colomn is selected
                if (std::find(select.begin(), select.end(), j) != select.end()) {
                    // j-th column is in the group set
                    if (std::find(group_sets[i].begin(), group_sets[i].end(), j) != group_sets[i].end()) {
                        key = key + val + "\t";
                    } else {
                        key += "*\t";
                    }
                } else if (j == fuid_index) {
                    fuid = val;
                }
                ++j;
            }
            key.pop_back();  // Remove trailing tab
            ch.push(fuid, key);
            key = "";
        }
    };
    husky::load(infmt, parser);
    if (husky::Context::get_global_tid() == 0) {
        husky::base::log_msg("Finished loading.");
    }

    constexpr bool write_hdfs = true;
    std::string host = husky::Context::get_param("hdfs_namenode");
    std::string port = husky::Context::get_param("hdfs_namenode_port");

    // Receive
    husky::list_execute(group_list, {&ch}, {&agg_ch}, [&ch, &agg, &host, &port, &ghdfs_dest](Group& g) {
        std::string out;
        auto& msgs = ch.get(g);
        std::vector<std::string> uid(std::move(const_cast<std::vector<std::string>&>(msgs)));
        out = out + "\t" + std::to_string(uid.size());
        std::sort(uid.begin(), uid.end());
        // auto last = std::unique(uid.begin(), uid.end());
        // out = out + "\t" + std::to_string(std::distance(uid.begin(), last));
        int unique = 1;
        for (auto it  = uid.begin(); it != uid.end(); ++it) {
            auto next_it = it + 1;
            if (next_it != uid.end() && (*next_it) != (*it)) {
                unique++;
            }
        }
        out = out + "\t" + std::to_string(unique);
        if (write_hdfs) {
            agg.update(1);
            husky::io::HDFS::Write(host, port, g.id()+out+"\n", ghdfs_dest, husky::Context::get_global_tid());
        } else {
            husky::base::log_msg(g.id() + out);
        }
    });

    int total_num_write = agg.get_value();
    if (husky::Context::get_global_tid() == 0) {
        husky::base::log_msg("Total number of rows written to HDFS: " + std::to_string(total_num_write));
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    args.push_back("schema");
    args.push_back("select");
    args.push_back("group_sets");
    args.push_back("output");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(cube);
        return 0;
    }
    return 1;
}
