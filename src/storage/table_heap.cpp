#include "storage/table_heap.h"

#include "glog/logging.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  //LOG(INFO)<<"-1"<<std::endl;
  /**
   * Insert a tuple into the table. If the tuple is too large (>= page_size), return false.
   * @param[in/out] row Tuple Row to insert, the rid of the inserted tuple is wrapped in object row
   * @param[in] txn The transaction performing the insert
   * @return true iff the insert is successful
   */
  if(row.GetSerializedSize(this->schema_)>=PAGE_SIZE){
    return false;
  }
  //LOG(INFO)<<"-2"<<std::endl;
  page_id_t this_page_id;
  this_page_id=this->GetFirstPageId();
  //LOG(INFO)<<"-3"<<std::endl;
  TablePage *this_page = reinterpret_cast<TablePage *>(this->buffer_pool_manager_->FetchPage(this_page_id));
  // LOG(INFO)<<this_page->GetPageId()<<std::endl;
  // LOG(INFO)<<this_page->GetNextPageId()<<std::endl;
  // LOG(INFO)<<"-4"<<std::endl;
  while((this_page->InsertTuple(row,this->schema_,txn,this->lock_manager_,this->log_manager_))!=true){
    //LOG(INFO)<<"-5"<<std::endl;
    this_page_id=this_page->GetPageId();
    if(this_page->GetNextPageId()!=INVALID_PAGE_ID){  //数据页没有到头
      page_id_t next_page_id;
      next_page_id=this_page->GetNextPageId();
      this_page= reinterpret_cast<TablePage *>(this->buffer_pool_manager_->FetchPage(next_page_id));
      continue;
    }
    //LOG(INFO)<<"-6"<<std::endl;
    if(this_page->GetNextPageId()==INVALID_PAGE_ID){     //所有数据页都试过了，新建一个页
      page_id_t next_page_id;
      this_page=reinterpret_cast<TablePage *>(this->buffer_pool_manager_->NewPage(next_page_id));
      //LOG(INFO)<<"-7"<<std::endl;
      this_page->Init(next_page_id,this_page_id,this->log_manager_,txn);
      //LOG(INFO)<<"-8"<<std::endl;
      this_page->SetPrevPageId(this_page_id);
      //LOG(INFO)<<"-9"<<std::endl;
      this_page->SetNextPageId(INVALID_PAGE_ID);
      //LOG(INFO)<<"-10"<<std::endl;
      this_page=reinterpret_cast<TablePage *>(this->buffer_pool_manager_->FetchPage(this_page_id));
      //LOG(INFO)<<"-11"<<std::endl;
      this_page->SetNextPageId(next_page_id);
      //LOG(INFO)<<"-12"<<std::endl;
      this_page=reinterpret_cast<TablePage *>(this->buffer_pool_manager_->FetchPage(next_page_id));
      //LOG(INFO)<<"-13"<<std::endl;
      this_page->InsertTuple(row,this->schema_,txn,this->lock_manager_,this->log_manager_);
      //LOG(INFO)<<"-14"<<std::endl;
      return true;
    }

  }
  //LOG(INFO)<<"-15"<<std::endl;
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  /**
   * if the new tuple is too large to fit in the old page, return false (will delete and insert)
   * @param[in] row Tuple of new row
   * @param[in] rid Rid of the old tuple
   * @param[in] txn Transaction performing the update
   * @return true is update is successful.
   */
  if(row.GetSerializedSize(schema_)>=PAGE_SIZE){
    return false;
  }
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  page->WLatch();
  int flag;
  Row *old_row = new Row(rid);
  while((flag=page->UpdateTuple(row,old_row,schema_,txn,lock_manager_,log_manager_)!=1)){
    if(flag==-1) return false;
    else if(flag==-2) return false;
    else if(flag==-3){
      page_id_t current_page_id=page->GetPageId();
      page_id_t next_page_id=page->GetNextPageId();
      if(next_page_id!=INVALID_PAGE_ID){
        page=reinterpret_cast<TablePage *>(this->buffer_pool_manager_->FetchPage(next_page_id));
        continue;
      }
      else{
        page_id_t new_page_id;
        page=reinterpret_cast<TablePage *>(this->buffer_pool_manager_->NewPage(new_page_id));
        page->Init(new_page_id,current_page_id,this->log_manager_,txn);
        
        page->SetPrevPageId(current_page_id);
        page->SetNextPageId(INVALID_PAGE_ID);
        page=reinterpret_cast<TablePage *>(this->buffer_pool_manager_->FetchPage(current_page_id));
        page->SetNextPageId(new_page_id);

        page=reinterpret_cast<TablePage *>(this->buffer_pool_manager_->FetchPage(new_page_id));
        page->InsertTuple(row,this->schema_,txn,this->lock_manager_,this->log_manager_);
        return true;
      }
    }
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
  return true;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page==nullptr){
    return;
  }
  page->WLatch();
  page->ApplyDelete(rid,txn,log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
    /**
   * Free table heap and release storage in disk file
   */
  // page_id_t temp=first_page_id_;
  // buffer_pool_manager_->DeletePage(temp);
  // for (auto iter = this->Begin(NULL); iter != this->End(); iter++) {
      
  // }
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  assert(page!=nullptr);
  while(page->GetNextPageId()!=INVALID_PAGE_ID){
    page_id_t next_page_id;
    next_page_id=page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
    buffer_pool_manager_->DeletePage(page->GetTablePageId());
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
  }
  delete buffer_pool_manager_;
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  RowId rid_temp;
  rid_temp=row->GetRowId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid_temp.GetPageId()));
   if (page == nullptr) {
    return false;
  }
  page->RLatch();
  if(page->GetTuple(row,schema_,txn,lock_manager_)==true){
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      return true;
  }
  else return false;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  auto first_page_id_temp=first_page_id_;    //第一个page的id
  RowId first_row_id; //第一个row的id
  while(first_page_id_temp!=INVALID_PAGE_ID){    ///从第一个page开始找，找到第一个row
    auto page=static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_temp));
    auto flag=page->GetFirstTupleRid(&first_row_id);
    buffer_pool_manager_->UnpinPage(first_page_id_temp,false);
    if(flag)  break;
    first_page_id_temp=page->GetNextPageId();   //这个页没找到，就继续在后面的页找
  }
  return TableIterator(this,first_row_id);
}

TableIterator TableHeap::End() {
  return TableIterator(this,RowId(INVALID_ROWID));
}
