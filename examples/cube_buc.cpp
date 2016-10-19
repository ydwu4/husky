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

#include <climits>
#include <map>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "boost/tokenizer.hpp"

#include "core/engine.hpp"
#include "io/input/hdfs_line_inputformat.hpp"

typedef std::vector<int> Attribute;
typedef std::map<int, int> DimMap;
typedef std::vector<std::string> Tuple;
typedef std::vector<Tuple> TupleVector;
typedef TupleVector::iterator TVIterator;

class Group {
   public:
    using KeyT = std::string;

    Group() = default;
    explicit Group(const KeyT& t) : key(t) {}
    // explicit Group(Tuple&& t) : key(std::move(t)) {}

    const KeyT& id() { return key; }
    KeyT key;
};

class TreeNode {
   public:
    TreeNode() = default;
    explicit TreeNode(Attribute&& key) : key_(std::move(key)) {
        // std::sort(key_.begin(), key_.end());
        lv_ = key_.size();
        is_visited_ = false;
    }

    ~TreeNode() {
        for (auto c : children_) {
            delete c;
        }
    }

    const Attribute& Key() { return key_; }

    std::vector<TreeNode*>& Children() { return children_; }

    int Level() { return lv_; }

    bool is_visited() { return is_visited_; }

    bool is_parent(TreeNode* child) {
        auto child_key = child->Key();
        for (auto& col : key_) {
            if (std::find(child_key.begin(), child_key.end(), col) == child_key.end()) {
                return false;
            }
        }
        return true;
    }

    void add_child(TreeNode* child) { children_.push_back(child); }

    void Visit() { is_visited_ = true; }

    void Reset() { is_visited_ = false; }

   private:
    Attribute key_;
    std::vector<TreeNode*> children_;
    int lv_;
    bool is_visited_;
};

void print_key(const Attribute& key) {
    std::string out;
    for (auto& i : key) {
        out = out + std::to_string(i) + " ";
    }
    husky::base::log_msg(out);
}

void measure(Tuple& key_value, const Attribute& group_attributes, Attribute& select, Attribute& key_attributes,
             DimMap& key_dim_map, DimMap& msg_dim_map, int uid_dim, TVIterator begin, TVIterator end) {
    int count = end - begin;
    std::sort(begin, end, [uid_dim](const Tuple& a, const Tuple& b) { return a[uid_dim] < b[uid_dim]; });
    int unique = 1;
    for (TVIterator it = begin; it != end; ++it) {
        TVIterator next_it = it + 1;
        if (next_it != end && (*it)[uid_dim] != (*next_it)[uid_dim]) {
            ++unique;
        }
    }

    // Output
    std::string out;
    for (auto& attr : select) {
        // Attribute is in key
        // Output key value
        if (std::find(key_attributes.begin(), key_attributes.end(), attr) != key_attributes.end()) {
            out = out + key_value[key_dim_map[attr]] + "\t";
        } else {
            if (std::find(group_attributes.begin(), group_attributes.end(), attr) != group_attributes.end()) {
                out = out + (*begin)[msg_dim_map[attr]] + "\t";
            } else {
                out += "*\t";
            }
        }
    }

    out = out + std::to_string(count) + "\t" + std::to_string(unique);
    ;

    husky::base::log_msg(out);
}

int next_partition_dim(const Attribute& parent_key, const Attribute& child_key, DimMap& dim_map) {
    for (auto& attr : child_key) {
        if (std::find(parent_key.begin(), parent_key.end(), attr) == parent_key.end()) {
            return dim_map[attr];
        }
    }
    // error
    return -1;
}

void partition(TVIterator begin, TVIterator end, int dim, std::vector<int>& out_partition_result) {
    std::sort(begin, end, [dim](const Tuple& a, const Tuple& b) { return a[dim] < b[dim]; });
    int i = 0;
    out_partition_result.resize(1);
    TVIterator next_tuple;
    for (TVIterator it = begin; it != end; ++it) {
        out_partition_result[i]++;
        next_tuple = it + 1;
        if (next_tuple != end && (*it)[dim] != (*next_tuple)[dim]) {
            ++i;
            out_partition_result.resize(i + 1);
        }
    }
}

void BUC(TreeNode* cur_node, TupleVector& table, Tuple& key_value, Attribute& select, Attribute& key_attributes,
         DimMap& key_dim_map, DimMap& msg_dim_map, int uid_dim, int dim, TVIterator begin, TVIterator end) {
    // Measure current group
    measure(key_value, cur_node->Key(), select, key_attributes, key_dim_map, msg_dim_map, uid_dim, begin, end);

    // Process children
    for (auto& child : cur_node->Children()) {
        if (!child->is_visited()) {
            child->Visit();
            // Partition table by next column
            int next_dim = next_partition_dim(cur_node->Key(), child->Key(), msg_dim_map);
            // TODO(Ruihao): handle error if next_dim == -1
            std::vector<int> next_partition_result = {};
            partition(begin, end, next_dim, next_partition_result);

            TVIterator k = begin;
            for (int i = 0; i < next_partition_result.size(); ++i) {
                int count = next_partition_result[i];
                BUC(child, table, key_value, select, key_attributes, key_dim_map, msg_dim_map, uid_dim, next_dim, k,
                    k + count);
                k += count;
            }
        }
    }
}

void reset_lattice(TreeNode* root) {
	root->Reset();
	for (auto child : root->Children()) {
		if (child->is_visited()) {
			reset_lattice(child);
		}
	}
}

void cube_buc() {
    // Get group set masks
    std::string schema_conf = husky::Context::get_param("schema");
    std::string select_conf = husky::Context::get_param("select");
    std::string group_conf = husky::Context::get_param("group_sets");
    int part_factor = std::stoi(husky::Context::get_param("partition_factor"));
    boost::char_separator<char> comma_sep(",");
    boost::char_separator<char> colon_sep(":");
    boost::tokenizer<boost::char_separator<char>> schema_tok(schema_conf, comma_sep);
    boost::tokenizer<boost::char_separator<char>> select_tok(select_conf, comma_sep);
    boost::tokenizer<boost::char_separator<char>> group_set_tok(group_conf, colon_sep);
    // Convert select to indices
    Attribute select;
    for (auto& s : select_tok) {
        auto it = std::find(schema_tok.begin(), schema_tok.end(), s);
        if (it != schema_tok.end()) {
            select.push_back(std::distance(schema_tok.begin(), it));
        }
        // TODO(Ruihao): Throw expection if input is wrong?
    }
    std::sort(select.begin(), select.end());
    // Convert group sets to tree nodes
    TreeNode* root;
    int min_lv = INT_MAX;
    int max_lv = INT_MIN;
    std::unordered_map<int, std::vector<TreeNode*>> tree_map;
    size_t group_set_size = std::distance(group_set_tok.begin(), group_set_tok.end());
    for (auto& group : group_set_tok) {
        boost::tokenizer<boost::char_separator<char>> colomn_tok(group, comma_sep);
        Attribute tree_key = {};
        for (auto column : colomn_tok) {
            auto it = std::find(schema_tok.begin(), schema_tok.end(), column);
            if (it != schema_tok.end()) {
                tree_key.push_back(std::distance(schema_tok.begin(), it));
            }
            // TODO(Ruihao): Throw expection if input is wrong?
        }
        TreeNode* node = new TreeNode(std::move(tree_key));
        tree_map[node->Level()].push_back(node);
        if (node->Level() < min_lv) {
            min_lv = node->Level();
            root = node;
        }
        if (node->Level() > max_lv) {
            max_lv = node->Level();
        }
    }
    husky::base::log_msg("Min level: " + std::to_string(min_lv) + "\tMax level: " + std::to_string(max_lv));
    // Build group set lattice
    for (int i = min_lv; i < max_lv; ++i) {
        if (tree_map[i].empty()) {
            throw husky::base::HuskyException("Empty level in the tree");
        }
        for (auto& tn : tree_map[i]) {
            for (auto& next_tn : tree_map[i + 1]) {
                if (tn->is_parent(next_tn)) {
                    tn->add_child(next_tn);
                }
            }
        }
    }
    husky::base::log_msg("Finished constructing lattice.");

    // {key} union {msg} = {select}
    // {key} intersect {msg} = empty
    Attribute key_attributes = root->Key();
    Attribute msg_attributes;
    for (auto& s : select) {
        if (std::find(key_attributes.begin(), key_attributes.end(), s) == key_attributes.end()) {
            msg_attributes.push_back(s);
        }
    }

    // Mapping of attributes in the message table
    // It should be consistent with the order of attributes in messages
    DimMap msg_dim_map;
    for (int i = 0; i < msg_attributes.size(); ++i) {
        msg_dim_map[msg_attributes[i]] = i;
    }

    // Mapping of attributes in key
    DimMap key_dim_map;
    for (int i = 0; i < key_attributes.size(); ++i) {
        key_dim_map[key_attributes[i]] = i;
    }

    int uid_index = -1;
    auto uid_it = std::find(schema_tok.begin(), schema_tok.end(), "fuid");
    if (uid_it != schema_tok.end()) {
    	uid_index =  std::distance(schema_tok.begin(), uid_it);
    }

    // std::queue<TreeNode*> q;
    // q.push(root);
    // root->Visit();
    // while (!q.empty()) {
    //     TreeNode* cur_node = q.front();
    //     q.pop();
    //     print_key(cur_node->Key());
    //     for (auto& child : cur_node->Children()) {
    //         if (!child->is_visited()) {
    //             q.push(child);
    //             child->Visit();
    //         }
    //     }
    // }

    // Load input and emit key -> uid
    husky::io::HDFSLineInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));

    auto& group_list = husky::ObjListFactory::create_objlist<Group>();
    auto& ch = husky::ChannelFactory::create_push_channel<Tuple>(infmt, group_list);

    auto parser = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        boost::char_separator<char> sep("\t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        std::string key = "";
        Tuple msg = {};
        std::string fuid;
        int j = 0;
        for (auto& col : tok) {
            if (std::find(key_attributes.begin(), key_attributes.end(), j) != key_attributes.end()) {
                key = key + col + "\t";
            } else if (std::find(msg_attributes.begin(), msg_attributes.end(), j) != msg_attributes.end()) {
                msg.push_back(col);
            } else if (j == uid_index) {
                fuid = col;
            }
            ++j;
        }
        msg.push_back(fuid);
        int bucket = std::stoi(fuid) % part_factor;
        key += std::to_string(bucket);
        ch.push(msg, key);

    };
    husky::load(infmt, parser);

    // Receive
    husky::list_execute(group_list, [&](Group & g) {
        auto& msgs = ch.get(g);
        TupleVector table(std::move(const_cast<TupleVector&>(msgs)));
        int uid_dim = msg_attributes.size();
        boost::char_separator<char> sep("\t");
        boost::tokenizer<boost::char_separator<char>> tok(g.id(), sep);
        std::vector<std::string> key_value(tok.begin(), tok.end());
        // Remove the hash value
        key_value.pop_back();

        BUC(root, table, key_value, select, key_attributes, key_dim_map, msg_dim_map, uid_dim, 0, table.begin(),
            table.end());
        reset_lattice(root);
    });
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    args.push_back("schema");
    args.push_back("select");
    args.push_back("group_sets");
    args.push_back("partition_factor");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(cube_buc);
        return 0;
    }
    return 1;
}
