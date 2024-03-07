#include <stdexcept>
#include <sys/stat.h>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage  *temp;
  temp=reinterpret_cast<DiskFileMetaPage *>(meta_data_);  //读取元信息
  temp->num_allocated_pages_++; //总页数加1
  BitmapPage<PAGE_SIZE> *bitmap;
  uint32_t i;
  for(i=0;i<temp->num_extents_;i++){  //遍历所有extent找空闲页
    page_id_t p_bitpage;  //bitmap的物理页号
    p_bitpage=i*(bitmap->GetMaxSupportedSize()+1)+1;
    char bittemp[PAGE_SIZE]{};
    ReadPhysicalPage(p_bitpage,bittemp);    //读取这个bitmap
    BitmapPage<PAGE_SIZE> *bit_p;
    bit_p=reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bittemp);  //转换成bitmap类
    uint32_t index_in_bitmap;
    if(bit_p->AllocatePage(index_in_bitmap)==true){ //如果这个分区分配空闲页成功
      temp->extent_used_page_[i]++; //该区的使用页数加1
      WritePhysicalPage(p_bitpage,reinterpret_cast<char*>(bit_p));
      return (i)*(bit_p->GetMaxSupportedSize())+index_in_bitmap; //返回这个空闲页的逻辑页号
    }
  }

  //现有的分区都没有空闲页
  page_id_t p_bitpage;
  p_bitpage=temp->num_extents_*(bitmap->GetMaxSupportedSize()+1)+1;
  char bittemp[PAGE_SIZE]{};
  ReadPhysicalPage(p_bitpage,bittemp);    //读取这个bitmap
  BitmapPage<PAGE_SIZE> *bit_p;
  bit_p=reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bittemp);  //转换成bitmap类
  uint32_t index_in_bitmap;
  bit_p->AllocatePage(index_in_bitmap);
  temp->extent_used_page_[temp->num_extents_]++; //该区的使用页数加1
  temp->num_extents_++; //分区数量加1
  WritePhysicalPage(p_bitpage,reinterpret_cast<char*>(bit_p));
  return (temp->num_extents_-1)*(bit_p->GetMaxSupportedSize())+index_in_bitmap; //返回这个空闲页的逻辑页号
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  DiskFileMetaPage  *temp;
  BitmapPage<PAGE_SIZE> *bitmap;
  temp=reinterpret_cast<DiskFileMetaPage *>(meta_data_);  //读取元信息
  temp->num_allocated_pages_--; //总页数减1
  LOG(INFO)<<temp->num_allocated_pages_<<std::endl;
  uint32_t extent_num;  //这个逻辑页所在的分区
  extent_num=logical_page_id/(bitmap->GetMaxSupportedSize()); 
  page_id_t p_bitpage;  //bitmap的物理页号
  p_bitpage=extent_num*(bitmap->GetMaxSupportedSize()+1)+1;  //计算输入的逻辑页号 是属于的bitmap的物理页

  char bittemp[PAGE_SIZE]{};
  ReadPhysicalPage(p_bitpage,bittemp);    //读取这个bitmap
  BitmapPage<PAGE_SIZE> *bit_p;
  bit_p=reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bittemp);

  uint32_t index_in_bitmap;   
  index_in_bitmap=logical_page_id%(bit_p->GetMaxSupportedSize());  //这个逻辑页号 在这个bitmap中的 位置 

  if(!IsPageFree(logical_page_id)){ //执行删除成功
    bit_p->DeAllocatePage(index_in_bitmap);
    temp->extent_used_page_[extent_num]--;  //该分区的页数减1
  }
  WritePhysicalPage(p_bitpage,reinterpret_cast<char*>(bit_p));
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  uint32_t extent_num;
  BitmapPage<PAGE_SIZE> *bitmap;
  extent_num=logical_page_id/(bitmap->GetMaxSupportedSize()); 
  page_id_t p_bitpage;
  p_bitpage=extent_num*(bitmap->GetMaxSupportedSize()+1)+1;   //计算输入的逻辑页号 是属于哪个bitmap

  char bittemp[PAGE_SIZE]{};
  ReadPhysicalPage(p_bitpage,bittemp);    //读取这个bitmap

  BitmapPage<PAGE_SIZE> *bit_p;
  bit_p=reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bittemp);

  uint32_t index_in_bitmap;   
  index_in_bitmap=logical_page_id%(bitmap->GetMaxSupportedSize());  //这个逻辑页号 在这个bitmap中的 位置 
  if(bit_p->IsPageFree(index_in_bitmap)==false) return false;
  else return true;
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  uint32_t num_bitmap_before;
  num_bitmap_before=logical_page_id/(PAGE_SIZE)+1;  //这个逻辑页前面有多少个bitmap
  return logical_page_id+num_bitmap_before+1; 
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}