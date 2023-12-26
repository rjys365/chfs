#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork() {
        // Your code goes here
        std::map<std::string, std::vector<std::string>> key_val;
        std::map<std::string, std::string> key_val_res;
        for(auto &file:files){
            auto lookup_res=chfs_client->lookup(1,file);
            if(lookup_res.is_err()){
                continue;
            }
            auto inode_id=lookup_res.unwrap();
            auto type_attr_res=chfs_client->get_type_attr(inode_id);
            if(type_attr_res.is_err()){
                continue;
            }
            auto type_attr=type_attr_res.unwrap();
            auto file_size=type_attr.second.size;
            auto read_res=chfs_client->read_file(inode_id,0,file_size);
            if(read_res.is_err()){
                continue;
            }
            auto content=read_res.unwrap();
            std::string content_str(content.begin(),content.end());
            auto map_res=mapReduce::Map(content_str);
            for(auto &key_val_pair:map_res){
                key_val[key_val_pair.key].push_back(key_val_pair.val);
            }
        }
        for(auto &key_val_pair:key_val){
            auto reduce_res=mapReduce::Reduce(key_val_pair.first,key_val_pair.second);
            key_val_res[key_val_pair.first]=reduce_res;
        }
        std::stringstream output_stream;
        for(auto &key_val_pair:key_val_res){
            output_stream<<key_val_pair.first<<" "<<key_val_pair.second<<" ";
        }
        std::string output_str=output_stream.str();
        auto new_file_id=chfs_client->lookup(1,outPutFile);
        if(new_file_id.is_err()){
            return;
        }
        std::vector<uint8_t> output_vec(output_str.begin(),output_str.end());
        chfs_client->write_file(new_file_id.unwrap(),0,output_vec);
    }
}