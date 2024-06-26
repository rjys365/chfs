#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // Append the new directory entry to `src`.
  src+=("/"+filename+":"+std::to_string(id));
  
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  list.clear();
  std::size_t pos=0;
  if(src[0]=='/')pos++;
  while(true){
    std::size_t colon_pos=src.find(':',pos);
    if(colon_pos==std::string::npos)return;
    std::string name=src.substr(pos,colon_pos-pos);
    pos=colon_pos+1;
    std::size_t slash_pos=src.find('/',pos);
    std::string inode_id_str;
    if(slash_pos==std::string::npos)inode_id_str=src.substr(pos);
    else inode_id_str=src.substr(pos,slash_pos-pos);
    inode_id_t inode_id=std::stoull(inode_id_str);
    list.push_back((DirectoryEntry){std::move(name),inode_id});
    if(slash_pos==std::string::npos)return;
    pos=slash_pos+1;
  }

}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // Remove the directory entry from `src`.
  std::size_t pos=0;
  if(src[0]=='/')pos++;
  bool first_file=true;
  while(true){
    std::size_t colon_pos=src.find(':',pos);
    if(colon_pos==std::string::npos)break;
    std::string name=src.substr(pos,colon_pos-pos);
    pos=colon_pos+1;
    std::size_t slash_pos=src.find('/',pos);
    std::string inode_id_str;
    if(slash_pos==std::string::npos)inode_id_str=src.substr(pos);
    else inode_id_str=src.substr(pos,slash_pos-pos);
    if(name!=filename){
      if(first_file){
        res=name+":"+inode_id_str;
        first_file=false;
      }
      else res+="/"+name+":"+inode_id_str;
    }
    if(slash_pos==std::string::npos)break;
    pos=slash_pos+1;
  }

  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  auto read_res=fs->read_file(id);
  if(read_res.is_err()){
    return ChfsNullResult(read_res.unwrap_error());
  }
  auto data=read_res.unwrap();
  auto data_str=std::string(data.begin(),data.end());
  parse_directory(data_str,list);

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  auto read_res=this->read_file(id);
  if(read_res.is_err()){
    return ChfsResult<inode_id_t>(read_res.unwrap_error());
  }
  auto data=read_res.unwrap();
  auto data_str=std::string(data.begin(),data.end());
  parse_directory(data_str,list);
  for(const auto &entry:list){
    if(entry.name==name)return ChfsResult<inode_id_t>(entry.id);
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  auto lookup_res = this->lookup(id,name);
  if(lookup_res.is_err()&&lookup_res.unwrap_error()!=ErrorType::NotExist){
    return ChfsResult<inode_id_t>(lookup_res.unwrap_error());
  }
  if(lookup_res.is_ok()){
    return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
  }
  auto alloc_res = this->alloc_inode(type);
  if(alloc_res.is_err()){
    return ChfsResult<inode_id_t>(lookup_res.unwrap_error());
  }
  auto new_inode_id=alloc_res.unwrap();
  auto read_res=this->read_file(id);
  if(read_res.is_err()){
    return ChfsResult<inode_id_t>(read_res.unwrap_error());
  }
  auto data=read_res.unwrap();
  auto data_str=std::string(data.begin(),data.end());
  auto data_str_new=append_to_directory(data_str,name,new_inode_id);
  auto data_new=std::vector<u8>(data_str_new.begin(),data_str_new.end());
  auto write_res=this->write_file(id,data_new);
  if(write_res.is_err()){
    return ChfsResult<inode_id_t>(write_res.unwrap_error());
  }

  return ChfsResult<inode_id_t>(new_inode_id);
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  auto lookup_res=this->lookup(parent,name);
  if(lookup_res.is_err()){
    return ChfsNullResult(lookup_res.unwrap_error());
  }
  auto child_inode_id=lookup_res.unwrap();
  this->remove_file(child_inode_id);
  auto read_res=this->read_file(parent);
  if(read_res.is_err()){
    return ChfsNullResult(read_res.unwrap_error());
  }
  auto data=read_res.unwrap();
  auto data_str=std::string(data.begin(),data.end());
  auto data_str_new=rm_from_directory(data_str,name);
  auto data_new=std::vector<u8>(data_str_new.begin(),data_str_new.end());
  auto write_res=this->write_file(parent,data_new);
  if(write_res.is_err()){
    return ChfsNullResult(write_res.unwrap_error());
  }

  return KNullOk;
}

} // namespace chfs
