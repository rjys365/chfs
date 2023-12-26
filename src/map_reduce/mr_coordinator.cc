#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>
#include <thread>
#include <iostream>

#include "map_reduce/protocol.h"

namespace mapReduce {
    Task Coordinator::askTask() {
        // Lab4 : Your code goes here.
        // Free to change the type of return value.
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        std::cerr<<"coordinator askTask"<<std::endl;
        if(this->isFinished)return {};
        if(this->current_stage==0){
            std::cerr<<"coordinator askTask stage 0, map_allocated: ";
            for(auto &allocated:this->map_allocated){
                std::cerr<<(int)allocated<<" ";
            }
            std::cerr<<std::endl;
            for(int i=0;i<this->files.size();i++){
                if(this->map_allocated[i]==0){
                    this->map_allocated[i]=1;
                    std::cerr<<"allocating map task "<<i<<std::endl;
                    return {1,this->files[i],i,-1,-1,this->nReduce,""};
                }
            }
            return {};
        }
        else if(this->current_stage==1){
            for(int i=0;i<this->nReduce;i++){
                if(this->reduce_allocated[i]==0){
                    this->reduce_allocated[i]=1;
                    return {2,"",-1,i,static_cast<int>(this->files.size()),-1,""};
                }
            }
            return {};
        }
        else if(this->current_stage==2){
            return {3,"",-1,-1,-1,this->nReduce,this->outPutFile};
        }
        return {};
    }

    int Coordinator::submitTask(int taskType, int index) {
        // Lab4 : Your code goes here.
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        if(taskType==1){
            if(this->current_stage!=0)return 0;
            this->map_finished[index]=1;
            bool stage_finished=true;
            for(auto &finished:this->map_finished){
                if(finished==0){
                    stage_finished=false;
                    break;
                }
            }
            if(stage_finished){
                this->current_stage=1;
            }
        }
        else if(taskType==2){
            if(this->current_stage!=1)return 0;
            this->reduce_finished[index]=1;
            bool stage_finished=true;
            for(auto &finished:this->reduce_finished){
                if(finished==0){
                    stage_finished=false;
                    break;
                }
            }
            if(stage_finished){
                this->current_stage=2;
            }
        }
        else if(taskType==3){
            if(this->current_stage!=2)return 0;
            this->isFinished=true;
        }
        return 0;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
        this->files = files;
        this->isFinished = false;
        // Lab4: Your code goes here (Optional).
        this->map_allocated.resize(files.size());
        this->map_finished.resize(files.size());
        this->reduce_allocated.resize(nReduce);
        this->reduce_finished.resize(nReduce);
        this->nReduce=nReduce;
        this->outPutFile=config.resultFile;
    
        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this]() { return this->askTask(); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
        rpc_server->run(true, 1);
    }
}