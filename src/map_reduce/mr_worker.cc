#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>

#include "map_reduce/protocol.h"

namespace mapReduce {

    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
    }

    void Worker::doMap(int index, const std::string &filename, int nReduce) {
        // Lab4: Your code goes here.
        std::vector<std::unordered_map<std::string,std::vector<std::string>>> key_vals(nReduce);
        auto lookup_res=chfs_client->lookup(1,filename);
        if(lookup_res.is_err()){
            return;
        }
        auto inode_id=lookup_res.unwrap();
        auto type_attr_res=chfs_client->get_type_attr(inode_id);
        if(type_attr_res.is_err()){
            return;
        }
        auto type_attr=type_attr_res.unwrap();
        auto file_size=type_attr.second.size;
        auto read_res=chfs_client->read_file(inode_id,0,file_size);
        if(read_res.is_err()){
            return;
        }
        auto content=read_res.unwrap();
        std::string content_str(content.begin(),content.end());
        auto map_res=mapReduce::Map(content_str);
        for(auto &key_val_pair:map_res){
            auto reduce_idx=std::hash<std::string>{}(key_val_pair.key)%nReduce;
            key_vals[reduce_idx][key_val_pair.key].push_back(key_val_pair.val);
        }
        for(int i=0;i<nReduce;i++){
            std::stringstream output_stream;
            output_stream<<key_vals[i].size()<<" ";
            for(auto &key_val_pair:key_vals[i]){
                output_stream<<key_val_pair.first<<" ";
                output_stream<<key_val_pair.second.size()<<" ";
                for(auto &val:key_val_pair.second){
                    output_stream<<val<<" ";
                }
            }
            std::string output_str=output_stream.str();
            auto new_file_id=chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR,1,"mr_intermediate"+std::to_string(i)+"_"+std::to_string(index));
            if(new_file_id.is_err()){
                continue;
            }
            std::vector<uint8_t> output_vec(output_str.begin(),output_str.end());
            chfs_client->write_file(new_file_id.unwrap(),0,output_vec);
        }
    }

    void Worker::doReduce(int index, int nMapFiles) {
        // Lab4: Your code goes here.
        std::map<std::string,std::vector<std::string>> key_vals;
        for(int i=0;i<nMapFiles;i++){
            auto lookup_res=chfs_client->lookup(1,"mr_intermediate"+std::to_string(index)+"_"+std::to_string(i));
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
            std::stringstream stringstream(content_str);
            int key_cnt;
            stringstream>>key_cnt;
            for(int j=0;j<key_cnt;j++){
                std::string key;
                stringstream>>key;
                int val_cnt;
                stringstream>>val_cnt;
                for(int k=0;k<val_cnt;k++){
                    std::string val;
                    stringstream>>val;
                    key_vals[key].push_back(val);
                }
            }
        }
        std::map<std::string,std::string> key_val_res;
        for(auto &key_val_pair:key_vals){
            auto reduce_res=mapReduce::Reduce(key_val_pair.first,key_val_pair.second);
            key_val_res[key_val_pair.first]=reduce_res;
        }
        std::stringstream output_stream;
        for(auto &key_val_pair:key_val_res){
            output_stream<<key_val_pair.first<<" "<<key_val_pair.second<<" ";
        }
        std::string output_str=output_stream.str();
        auto new_file_id=chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR,1,"mr_out_"+std::to_string(index));
        if(new_file_id.is_err()){
            return;
        }
        std::vector<uint8_t> output_vec(output_str.begin(),output_str.end());
        chfs_client->write_file(new_file_id.unwrap(),0,output_vec);
    }

    void Worker::doSubmit(int taskType, int index) {
        // Lab4: Your code goes here.
        mr_client->call(SUBMIT_TASK,taskType,index);
    }

    void Worker::stop() {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork() {
        while (!shouldStop) {
            // Lab4: Your code goes here.
            std::cerr<<std::this_thread::get_id()<<"\tworker do work"<<std::endl;
            auto rpc_res=mr_client->call(ASK_TASK);
            if(rpc_res.is_err()){
                continue;
            }
            auto task=rpc_res.unwrap()->as<Task>();
            if(task.taskType==1){
                std::cerr<<std::this_thread::get_id()<<"\tworker do map "<<task.mapFileIdx<<" "<<task.mapFileName<<std::endl;
                doMap(task.mapFileIdx,task.mapFileName,task.reduceFileCnt);
                doSubmit(task.taskType,task.mapFileIdx);
            }
            else if(task.taskType==2){
                std::cerr<<std::this_thread::get_id()<<"\tworker do reduce "<<task.reduceIdx<<std::endl;
                doReduce(task.reduceIdx,task.mapFileCnt);
                doSubmit(task.taskType,task.reduceIdx);
            }
            else if(task.taskType==3){
                std::cerr<<std::this_thread::get_id()<<"\tworker do combine"<<std::endl;
                doCombine(task.reduceFileCnt, task.combineFileName);
                doSubmit(task.taskType,0);
            }
            else{
                std::cerr<<std::this_thread::get_id()<<"\tworker no task"<<std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }

    void Worker::doCombine(int nReduce, std::string outputFile){
        std::map<std::string,std::string> key_val_res;
        for(int i=0;i<nReduce;i++){
            auto lookup_res=chfs_client->lookup(1,"mr_out_"+std::to_string(i));
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
            std::stringstream content_stream(content_str);
            std::string key,val;
            while(content_stream>>key>>val){
                key_val_res[key]=val;
            }
        }
        std::stringstream output_stream;
        for(auto &key_val_pair:key_val_res){
            output_stream<<key_val_pair.first<<" "<<key_val_pair.second<<" ";
        }
        std::string output_str=output_stream.str();
        auto new_file_id=chfs_client->lookup(1,outputFile);
        if(new_file_id.is_err()){
            return;
        }
        std::vector<uint8_t> output_vec(output_str.begin(),output_str.end());
        chfs_client->write_file(new_file_id.unwrap(),0,output_vec);
    }
}