#include <ctime>

#include "filesystem/operations.h"

namespace chfs {

// {Your code here}
auto FileOperation::alloc_inode(InodeType type) -> ChfsResult<inode_id_t> {
  // 1. Allocate a block for the inode.
  auto block_allocation_result = this->block_allocator_->allocate();
  if (block_allocation_result.is_err())
    return ChfsResult<inode_id_t>(ErrorType::OUT_OF_RESOURCE);
  block_id_t block_id = block_allocation_result.unwrap();
  // 2. Allocate an inode.
  // 3. Initialize the inode block
  //    and write the block back to block manager.
  auto inode_allocation_result =
      this->inode_manager_->allocate_inode(type, block_id);
  if (inode_allocation_result.is_err())
    return ChfsResult<inode_id_t>(ErrorType::OUT_OF_RESOURCE);
  inode_id_t inode_id = inode_allocation_result.unwrap();

  return ChfsResult<inode_id_t>(inode_id);
}

auto FileOperation::getattr(inode_id_t id) -> ChfsResult<FileAttr> {
  return this->inode_manager_->get_attr(id);
}

auto FileOperation::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  return this->inode_manager_->get_type_attr(id);
}

auto FileOperation::gettype(inode_id_t id) -> ChfsResult<InodeType> {
  return this->inode_manager_->get_type(id);
}

auto calculate_block_sz(u64 file_sz, u64 block_sz) -> u64 {
  return (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
}

auto FileOperation::write_file_w_off(inode_id_t id, const char *data, u64 sz,
                                     u64 offset) -> ChfsResult<u64> {
  auto read_res = this->read_file(id);
  if (read_res.is_err()) {
    return ChfsResult<u64>(read_res.unwrap_error());
  }

  auto content = read_res.unwrap();
  if (offset + sz > content.size()) {
    content.resize(offset + sz);
  }
  memcpy(content.data() + offset, data, sz);

  auto write_res = this->write_file(id, content);
  if (write_res.is_err()) {
    return ChfsResult<u64>(write_res.unwrap_error());
  }
  return ChfsResult<u64>(sz);
}

// {Your code here}
auto FileOperation::write_file(inode_id_t id, const std::vector<u8> &content)
    -> ChfsNullResult {
  auto error_code = ErrorType::DONE;
  const auto block_size = this->block_manager_->block_size();
  usize old_block_num = 0;
  usize new_block_num = 0;
  u64 original_file_sz = 0;

  // 1. read the inode
  std::vector<u8> inode(block_size);
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto inlined_blocks_num = 0;

  auto inode_res = this->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    error_code = inode_res.unwrap_error();
    // I know goto is bad, but we have no choice
    goto err_ret;
  } else {
    inlined_blocks_num = inode_p->get_direct_block_num();
  }

  if (content.size() > inode_p->max_file_sz_supported()) {
    std::cerr << "file size too large: " << content.size() << " vs. "
              << inode_p->max_file_sz_supported() << std::endl;
    error_code = ErrorType::OUT_OF_RESOURCE;
    goto err_ret;
  }

  // 2. make sure whether we need to allocate more blocks
  original_file_sz = inode_p->get_size();
  old_block_num = calculate_block_sz(original_file_sz, block_size);
  new_block_num = calculate_block_sz(content.size(), block_size);

  if (new_block_num > old_block_num) {
    // If we need to allocate more blocks.
    block_id_t indirect_block_id = 0;
    block_id_t *indirect_block_p = nullptr;
    for (usize idx = old_block_num; idx < new_block_num; ++idx) {
      // 1. Allocate a block.
      auto block_allocation_res = block_allocator_->allocate();
      if (block_allocation_res.is_err()) {
        error_code = block_allocation_res.unwrap_error();
        goto err_ret;
      }
      auto block_id = block_allocation_res.unwrap();
      // 2. Fill the allocated block id to the inode.
      //    You should pay attention to the case of indirect block.
      //    You may use function `get_or_insert_indirect_block`
      //    in the case of indirect block.
      if (inode_p->is_direct_block(idx)) {
        (*inode_p)[idx] = block_id;
      } else {
        if (!indirect_block.size()) {
          auto get_indirect_res =
              inode_p->get_or_insert_indirect_block(block_allocator_);
          if (get_indirect_res.is_err()) {
            error_code = get_indirect_res.unwrap_error();
            goto err_ret;
          }
          indirect_block_id = get_indirect_res.unwrap();
          indirect_block.resize(block_size);
          auto read_res = block_manager_->read_block(indirect_block_id,
                                                     indirect_block.data());
          if (read_res.is_err()) {
            error_code = read_res.unwrap_error();
            goto err_ret;
          }
          indirect_block_p =
              reinterpret_cast<block_id_t *>(indirect_block.data());
        }
        indirect_block_p[idx - inlined_blocks_num] = block_id;
      }
    }
    // if(indirect_block_id){
    //   auto
    //   write_res=block_manager_->write_block(indirect_block_id,indirect_block.data());
    //   if (write_res.is_err()) {
    //     error_code = write_res.unwrap_error();
    //     goto err_ret;
    //   }
    // }

  } else {
    // We need to free the extra blocks.
    block_id_t *indirect_block_p =
        reinterpret_cast<block_id_t *>(indirect_block.data());
    for (usize idx = new_block_num; idx < old_block_num; ++idx) {
      if (inode_p->is_direct_block(idx)) {
        // Free the direct extra block.
        block_allocator_->deallocate((*inode_p)[idx]);

      } else {
        // Free the indirect extra block.
        if (!indirect_block.size()) {
          auto get_indirect_res = block_manager_->read_block(
              inode_p->get_indirect_block_id(), indirect_block.data());
          if (get_indirect_res.is_err()) {
            error_code = get_indirect_res.unwrap_error();
            goto err_ret;
          }
          indirect_block_p =
              reinterpret_cast<block_id_t *>(indirect_block.data());
        }
        block_allocator_->deallocate(
            indirect_block_p[idx - inlined_blocks_num]);
      }
    }

    // If there are no more indirect blocks.
    if (old_block_num > inlined_blocks_num &&
        new_block_num <= inlined_blocks_num && true) {
      auto res =
          this->block_allocator_->deallocate(inode_p->get_indirect_block_id());
      if (res.is_err()) {
        error_code = res.unwrap_error();
        goto err_ret;
      }
      indirect_block.clear();
      inode_p->invalid_indirect_block_id();
    }
  }

  // 3. write the contents
  inode_p->inner_attr.size = content.size();
  inode_p->inner_attr.mtime = time(0);

  {
    auto block_idx = 0;
    u64 write_sz = 0;
    block_id_t *indirect_block_p =
        reinterpret_cast<block_id_t *>(indirect_block.data());

    while (write_sz < content.size()) {
      auto sz = ((content.size() - write_sz) > block_size)
                    ? block_size
                    : (content.size() - write_sz);
      std::vector<u8> buffer(block_size);
      memcpy(buffer.data(), content.data() + write_sz, sz);
      block_id_t block_id = 0;

      if (inode_p->is_direct_block(block_idx)) {
        block_id = (*inode_p)[block_idx];

      } else {
        if (!indirect_block.size()) {
          auto get_indirect_res = block_manager_->read_block(
              inode_p->get_indirect_block_id(), indirect_block.data());
          if (get_indirect_res.is_err()) {
            error_code = get_indirect_res.unwrap_error();
            goto err_ret;
          }
          indirect_block_p =
              reinterpret_cast<block_id_t *>(indirect_block.data());
        }
        block_id = indirect_block_p[block_idx - inlined_blocks_num];
      }

      // Write to current block.
      ChfsNullResult write_res = KNullOk;
      if (sz < block_size)
        write_res =
            block_manager_->write_partial_block(block_id, buffer.data(), 0, sz);
      else
        write_res = block_manager_->write_block(block_id, buffer.data());
      if (write_res.is_err()) {
        error_code = write_res.unwrap_error();
        goto err_ret;
      }

      write_sz += sz;
      block_idx += 1;
    }
  }

  // finally, update the inode
  {
    inode_p->inner_attr.set_all_time(time(0));

    auto write_res =
        this->block_manager_->write_block(inode_res.unwrap(), inode.data());
    if (write_res.is_err()) {
      error_code = write_res.unwrap_error();
      goto err_ret;
    }
    if (indirect_block.size() != 0) {
      write_res =
          inode_p->write_indirect_block(this->block_manager_, indirect_block);
      if (write_res.is_err()) {
        error_code = write_res.unwrap_error();
        goto err_ret;
      }
    }
  }

  return KNullOk;

err_ret:
  // std::cerr << "write file return error: " << (int)error_code << std::endl;
  return ChfsNullResult(error_code);
}

// {Your code here}
auto FileOperation::read_file(inode_id_t id) -> ChfsResult<std::vector<u8>> {
  auto error_code = ErrorType::DONE;
  std::vector<u8> content;

  const auto block_size = this->block_manager_->block_size();

  // 1. read the inode
  std::vector<u8> inode(block_size);
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  block_id_t *indirect_block_p = nullptr;
  u64 file_sz = 0;
  u64 read_sz = 0;
  u64 direct_sz = 0;

  auto inode_res = this->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    error_code = inode_res.unwrap_error();
    // I know goto is bad, but we have no choice
    goto err_ret;
  }

  file_sz = inode_p->get_size();
  content.reserve(file_sz);

  // Now read the file
  while (read_sz < file_sz) {
    auto sz = ((inode_p->get_size() - read_sz) > block_size)
                  ? block_size
                  : (inode_p->get_size() - read_sz);
    std::vector<u8> buffer(block_size);

    // Get current block id.
    block_id_t block_id;
    if (inode_p->is_direct_block(read_sz / block_size)) {
      block_id = (*inode_p)[read_sz / block_size];
    } else {
      if (!indirect_block_p) {
        direct_sz = read_sz;
        indirect_block.resize(block_size);
        auto indirect_block_id = inode_p->get_indirect_block_id();
        auto read_indirect_block_res = block_manager_->read_block(
            indirect_block_id, indirect_block.data());
        if (read_indirect_block_res.is_err()) {
          error_code = read_indirect_block_res.unwrap_error();
          goto err_ret;
        }
        indirect_block_p =
            reinterpret_cast<block_id_t *>(indirect_block.data());
      }
      block_id = indirect_block_p[(read_sz - direct_sz) / block_size];
      // block_manager_->read_block(indirect_block_id,inode.data());
    }

    // Read from current block and store to `content`.
    auto read_block_res = block_manager_->read_block(block_id, buffer.data());
    if (read_block_res.is_err()) {
      error_code = read_block_res.unwrap_error();
      goto err_ret;
    }
    content.insert(content.end(), buffer.begin(), buffer.begin() + sz);

    read_sz += sz;
  }

  return ChfsResult<std::vector<u8>>(std::move(content));

err_ret:
  return ChfsResult<std::vector<u8>>(error_code);
}

auto FileOperation::read_file_w_off(inode_id_t id, u64 sz, u64 offset)
    -> ChfsResult<std::vector<u8>> {
  auto res = read_file(id);
  if (res.is_err()) {
    return res;
  }

  auto content = res.unwrap();
  return ChfsResult<std::vector<u8>>(
      std::vector<u8>(content.begin() + offset, content.begin() + offset + sz));
}

auto FileOperation::resize(inode_id_t id, u64 sz) -> ChfsResult<FileAttr> {
  auto attr_res = this->getattr(id);
  if (attr_res.is_err()) {
    return ChfsResult<FileAttr>(attr_res.unwrap_error());
  }

  auto attr = attr_res.unwrap();
  auto file_content = this->read_file(id);
  if (file_content.is_err()) {
    return ChfsResult<FileAttr>(file_content.unwrap_error());
  }

  auto content = file_content.unwrap();

  if (content.size() != sz) {
    content.resize(sz);

    auto write_res = this->write_file(id, content);
    if (write_res.is_err()) {
      return ChfsResult<FileAttr>(write_res.unwrap_error());
    }
  }

  attr.size = sz;
  return ChfsResult<FileAttr>(attr);
}

}  // namespace chfs
