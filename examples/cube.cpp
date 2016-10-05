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
#include "io/input/hdfs_line_inputformat.hpp"

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
    std::string group_conf = husky::Context::get_param("group_sets");
    boost::char_separator<char> space_sep(" ");
    boost::char_separator<char> colon_sep(":");
    boost::tokenizer<boost::char_separator<char>> schema_tok(schema_conf, space_sep);
    boost::tokenizer<boost::char_separator<char>> group_set_tok(group_conf, colon_sep);
    // Group set contains the indices of selected columns
    size_t group_set_size = std::distance(group_set_tok.begin(), group_set_tok.end());
    std::vector<std::vector<int>> group_sets(group_set_size);
    int i = 0;
    for (auto& group : group_set_tok) {
        boost::tokenizer<boost::char_separator<char>> colomn_tok(group, space_sep);
        for (auto column : colomn_tok) {
            auto it = std::find(schema_tok.begin(), schema_tok.end(), column);
            if (it != schema_tok.end()) {
                group_sets[i].push_back(std::distance(schema_tok.begin(), it));
            }
            // TODO(Ruihao): Throw expection if input is wrong?
        }
        ++i;
    }
    // Get the index of "fuid" column
    int fuid_index;
    auto it = std::find(schema_tok.begin(), schema_tok.end(), "fuid");
    if (it != schema_tok.end()) {
        fuid_index = std::distance(schema_tok.begin(), it);
    }
    // TODO(Ruihao): Same

    // Load input and emit key -> uid
    husky::io::HDFSLineInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));

    auto& group_list = husky::ObjListFactory::create_objlist<Group>();
    auto& ch = husky::ChannelFactory::create_push_channel<std::string>(infmt, group_list);

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
                // The colomn is selected
                if (std::find(group_sets[i].begin(), group_sets[i].end(), j) != group_sets[i].end()) {
                    key += val;
                } else {
                    key += "*";
                }
                if (j == fuid_index) {
                    fuid = val;
                }
                ++j;
            }
            ch.push(fuid, key);
            key = "";
        }
    };
    husky::load(infmt, parser);

    // Receive
    husky::list_execute(group_list, [&ch](Group& g) {
        std::string res;
        auto& msgs = ch.get(g);
        std::vector<std::string> uid(msgs);
        res += std::to_string(uid.size());
        std::sort(uid.begin(), uid.end());
        auto last = std::unique(uid.begin(), uid.end());
        res = res + " " + std::to_string(std::distance(uid.begin(), last));
        husky::base::log_msg(g.id() + res);
    });
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    args.push_back("schema");
    args.push_back("group_sets");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(cube);
        return 0;
    }
    return 1;
}
