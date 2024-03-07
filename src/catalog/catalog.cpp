#include "catalog/catalog.h"
#include "catalog/table.h"
#include "catalog/indexes.h"
#include "storage/table_heap.h"
#include "glog/logging.h"
void CatalogMeta::SerializeTo(char *buf) const {
  uint32_t size0= sizeof(CATALOG_METADATA_MAGIC_NUM);
  std::map<table_id_t, page_id_t>::const_iterator iter;
  std::map<index_id_t, page_id_t>::const_iterator it; 
  memcpy(buf,&CATALOG_METADATA_MAGIC_NUM,size0);
  uint32_t location=0;
  uint32_t size1=table_meta_pages_.size();
  memcpy(buf+size0,&size1,sizeof(table_meta_pages_.size()));
  location+=sizeof(size1);
  for(iter=table_meta_pages_.begin();iter!=table_meta_pages_.end();iter++){
    memcpy(buf+size0+location,&iter->first,sizeof(table_id_t));
    location+=sizeof(table_id_t);
    memcpy(buf+size0+location,&iter->second,sizeof(page_id_t));
    location+=sizeof(page_id_t);
  }
  
  uint32_t size2=index_meta_pages_.size();
  memcpy(buf+size0+location,&size2,sizeof(size2));
  location+=sizeof(size2);
  for(it=index_meta_pages_.begin();it!=index_meta_pages_.end();it++){
    memcpy(buf+size0+location,&it->first,sizeof(index_id_t));
    location+=sizeof(index_id_t);
    memcpy(buf+size0+location,&it->second,sizeof(page_id_t));
    location+=sizeof(page_id_t);
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  uint32_t magicnum=MACH_READ_UINT32(buf);
  std::map<table_id_t,page_id_t> tablemetapages;    //将会被序列化到某个数据页上
  std::map<index_id_t,page_id_t> indexmetapages;
  CatalogMeta *p=NewInstance(heap);
  uint32_t location=0;
  uint32_t size0=MACH_READ_UINT32(buf+sizeof(magicnum));
  location=sizeof(magicnum)+sizeof(size0);
  table_id_t tableid;
  page_id_t pageid;
  index_id_t indexid;
  for(uint32_t i=0;i<size0;i++){
    tableid=MACH_READ_UINT32(buf+location);
    location+=sizeof(tableid);
    pageid=MACH_READ_INT32(buf+location);
    location+=sizeof(pageid);
    p->table_meta_pages_.insert(make_pair(tableid,pageid));
  }
  uint32_t size1=MACH_READ_UINT32(buf+location);
  location+=sizeof(size1);
  for(uint32_t i=0;i<size1;i++){
    indexid=MACH_READ_UINT32(buf+location);
    location+=sizeof(indexid);
    pageid=MACH_READ_INT32(buf+location);
    location+=sizeof(pageid);
    p->index_meta_pages_.insert(make_pair(indexid,pageid));
  }
  return p;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  return 2*sizeof(uint32_t)+ sizeof(CATALOG_METADATA_MAGIC_NUM)+(sizeof(table_id_t)+sizeof(page_id_t))*table_meta_pages_.size()+(sizeof(index_id_t)+sizeof(page_id_t))*index_meta_pages_.size();
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  // ASSERT(false, "Not Implemented yet");
      if(init)
      {
        CatalogMeta *catalog;
        catalog_meta_ =catalog->NewInstance(heap_);
      }
      else
      {
        //CreateTable();
        Page *metapage=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);//CreateIndex();
        char *pagedata=metapage->GetData();
        catalog_meta_=CatalogMeta::DeserializeFrom(pagedata,heap_);
        next_index_id_ = catalog_meta_->GetNextIndexId();
        next_table_id_ = catalog_meta_->GetNextTableId();
        for(auto it=catalog_meta_->table_meta_pages_.begin();it!=catalog_meta_->table_meta_pages_.end();it++)
        {
          
          if(it->second<0)
          continue;
          metapage=buffer_pool_manager_->FetchPage(it->second);
          pagedata=metapage->GetData();
          TableMetadata *tablemeta;
          TableMetadata::DeserializeFrom(pagedata,tablemeta,heap_);
          TableInfo *tableinfo=nullptr;
          tableinfo= TableInfo::Create(heap_);
          TableHeap *tableheap=TableHeap::Create(buffer_pool_manager_,tablemeta->GetSchema(),nullptr,log_manager_, lock_manager_, heap_);
          tableinfo->Init(tablemeta,tableheap);
          LOG(INFO)<<tablemeta->GetTableName()<<std::endl;
          table_names_[tablemeta->GetTableName()]=tableinfo->GetTableId();
          tables_[tablemeta->GetTableId()]=tableinfo;
        }
        LOG(INFO)<<"abc"<<std::endl;
        for(auto iter=catalog_meta_->index_meta_pages_.begin();iter!=catalog_meta_->index_meta_pages_.end();iter++)
        {
          LOG(INFO)<<catalog_meta_->index_meta_pages_.size();
          LOG(INFO)<<iter->second;
          if(iter->second<0)
          continue;
          
          metapage=buffer_pool_manager_->FetchPage(iter->second);
          pagedata=metapage->GetData();
          IndexMetadata *indexmeta=nullptr;
          indexmeta->DeserializeFrom(pagedata,indexmeta,heap_);
          IndexInfo *indexinfo=nullptr;
          LOG(INFO)<<"abc"<<std::endl;
          indexinfo= IndexInfo::Create(heap_);
          TableInfo *tableinfo=tables_[indexmeta->GetTableId()];
          LOG(INFO)<<indexmeta->GetTableId();
          indexinfo->Init(indexmeta,tableinfo,buffer_pool_manager_);
          
          indexes_[indexmeta->GetIndexId()]=indexinfo;
          
          index_names_[tableinfo->GetTableName()][indexmeta->GetIndexName()] = indexmeta->GetIndexId();
        }
          
      }
}

CatalogManager::~CatalogManager() {
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.count(table_name) != 0)
      return DB_TABLE_ALREADY_EXIST;
  table_info=TableInfo::Create(heap_);
  TableMetadata *tablemeta=nullptr;
  page_id_t page;
  Page *newpage=buffer_pool_manager_->NewPage(page);
  page_id_t rootpageid=page;
  table_id_t tableid=next_table_id_++;
  tablemeta=TableMetadata::Create(tableid,table_name,rootpageid,schema,heap_);
  TableHeap *tableheap=TableHeap::Create(buffer_pool_manager_,schema,txn,log_manager_,lock_manager_,table_info->GetMemHeap());
  table_info->Init(tablemeta,tableheap);
  table_names_.insert(make_pair(table_name,tableid));
  tables_.insert(make_pair(tableid,table_info));
  catalog_meta_->table_meta_pages_[tableid] = rootpageid;
  catalog_meta_->table_meta_pages_[next_table_id_] = -1;
  uint32_t length=tablemeta->GetSerializedSize();
  char temp[length+1];
  tablemeta->SerializeTo(temp);
  char* pagedata=newpage->GetData();
  memset(pagedata,0,PAGE_SIZE);
  memcpy(pagedata,temp,length);
  //LOG(INFO)<<page;
  length=catalog_meta_->GetSerializedSize();
  char meta[length+1];
  catalog_meta_->SerializeTo(meta);
  //LOG(INFO)<<"abc";
  pagedata=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
  //LOG(INFO)<<"abc";
  memset(pagedata,0,PAGE_SIZE);
  memcpy(pagedata,meta,length);
  index_names_.insert({table_name, std::unordered_map<std::string, index_id_t>()});
  if(tablemeta!=nullptr&&tableheap!=nullptr)
  return DB_SUCCESS;
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  std::unordered_map<std::string, table_id_t>::iterator iter;
  iter=table_names_.find(table_name);
  if(iter!=table_names_.end()){
  std::unordered_map<table_id_t, TableInfo *>::iterator it;
  it=tables_.find(iter->second);
  table_info=it->second;
  return DB_SUCCESS;
  }
  else
  return DB_TABLE_NOT_EXIST;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  std::unordered_map<table_id_t, TableInfo *>::const_iterator it;
  for(it=tables_.begin();it!=tables_.end();it++)
  {
      tables.push_back(it->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // ASSERT(false, "Not Implemented yet");
  if(index_names_.count(table_name)<=0)
  return DB_TABLE_NOT_EXIST;
  auto it = index_names_[table_name];
  if(it.count(index_name)>0) 
  return DB_INDEX_ALREADY_EXIST;
  index_info=IndexInfo::Create(heap_);
  
  std::unordered_map<std::string, table_id_t>::iterator iter;
  iter=table_names_.find(table_name);
  if(iter!=table_names_.end()){
  std::unordered_map<table_id_t, TableInfo *>::iterator it;
  it=tables_.find(iter->second);
  TableSchema *schema=it->second->GetSchema();
  uint32_t index=0;
  std::vector<std::string>::const_iterator i;
  std::vector<std::uint32_t> keymap;
  for(i=index_keys.begin();i!=index_keys.end();i++)
  {
  if(schema->GetColumnIndex(*i,index)==DB_SUCCESS)
  {
    keymap.push_back(index);
  }
  else
  return DB_COLUMN_NAME_NOT_EXIST;
  }
  index_id_t indexid=next_index_id_++;
  IndexMetadata *indexmeta=IndexMetadata::Create(indexid,index_name,iter->second,keymap,index_info->GetMemHeap());
  index_info->Init(indexmeta,it->second,buffer_pool_manager_);
  page_id_t page;
  Page *newpage=buffer_pool_manager_->NewPage(page);
  catalog_meta_->index_meta_pages_[indexid] = page;
  catalog_meta_->index_meta_pages_[next_index_id_] = -1;
  index_names_[table_name][index_name] = indexid;
  indexes_.insert(make_pair(indexid,index_info));
  char *pagedata=newpage->GetData();
  indexmeta->SerializeTo(pagedata);
  pagedata=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
  memset(pagedata,0,PAGE_SIZE);
  catalog_meta_->SerializeTo(pagedata);
  return DB_SUCCESS;
  }
  return DB_TABLE_NOT_EXIST;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>>::const_iterator iter;
  iter=index_names_.find(table_name);
  if(iter==index_names_.end())
  {
    return DB_INDEX_NOT_FOUND;
  }
  std::unordered_map<std::string, index_id_t>::const_iterator it;
  it=iter->second.find(index_name);
  std::unordered_map<index_id_t, IndexInfo *>::const_iterator i;
  i=indexes_.find(it->second);
  index_info=i->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>>::const_iterator iter;
  iter=index_names_.find(table_name);
  if(iter==index_names_.end())
  {
    return DB_INDEX_NOT_FOUND;
  }
  else{
    std::unordered_map<std::string, index_id_t>::const_iterator it;
    std::unordered_map<index_id_t, IndexInfo *>::const_iterator i;
    for(it=iter->second.begin();it!=iter->second.end();it++)
    {
      i=indexes_.find(it->second);
      indexes.push_back(i->second);
    }
    return DB_SUCCESS;
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
 std::unordered_map<std::string, table_id_t>::iterator iter;
  iter=table_names_.find(table_name);
  if(iter!=table_names_.end()){
  table_id_t tableid=iter->second;
  table_names_.erase(iter);
  std::unordered_map<table_id_t, TableInfo *>::iterator it;
  it=tables_.find(tableid);
  tables_.erase(it);
  std::map<table_id_t, page_id_t>::iterator i;
  i=catalog_meta_->table_meta_pages_.find(tableid);
  buffer_pool_manager_->DeletePage(i->second);
  catalog_meta_->table_meta_pages_.erase(i);
  return DB_SUCCESS;
  }
  else
  return DB_FAILED;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  auto it = index_names_.find(table_name);
  if(it == index_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it2 = it->second.find(index_name);
  if(it2 == it->second.end()) return DB_INDEX_NOT_FOUND;
  
  index_id_t index_id = it2->second;
  page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
  catalog_meta_->index_meta_pages_.erase(index_id);
  // IndexInfo* iinfo = indexes_[index_id];
  // delete iinfo;//因为iinfo中所有成员变量空间都是有heap_分配的，所以删除heap_即可
  it->second.erase(index_name);
  indexes_.erase(index_id);
  buffer_pool_manager_->DeletePage(page_id);

  auto len = catalog_meta_->GetSerializedSize();
  char meta[len+1];
  catalog_meta_->SerializeTo(meta);
  char* p = buffer_pool_manager_->FetchPage(0)->GetData();
  memset(p, 0, PAGE_SIZE);
  memcpy(p,meta,len);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  
  Page *catalogmeta = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  memset(catalogmeta->GetData(),0, PAGE_SIZE);
  catalog_meta_->SerializeTo(catalogmeta->GetData());
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  std::map<table_id_t, page_id_t>::const_iterator it;
  it=catalog_meta_->table_meta_pages_.find(table_id);
  if(it==catalog_meta_->table_meta_pages_.end()){
  catalog_meta_->table_meta_pages_.insert(make_pair(table_id,page_id));
  return DB_SUCCESS;
  }
  else
  return DB_FAILED;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  std::map<index_id_t, page_id_t>::const_iterator it;
  it=catalog_meta_->index_meta_pages_.find(index_id);
  if(it==catalog_meta_->index_meta_pages_.end()){
  catalog_meta_->index_meta_pages_.insert(make_pair(index_id,page_id));
  return DB_SUCCESS;
  }
  else
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  std::unordered_map<table_id_t, TableInfo *>::const_iterator iter;
  iter=tables_.find(table_id);
  if(iter!=tables_.end())
  {
    table_info=iter->second;
    return DB_SUCCESS;
  }
  else
  return DB_TABLE_NOT_EXIST;
}