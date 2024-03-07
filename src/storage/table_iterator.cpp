#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() {

}

TableIterator::~TableIterator() {

}

TableIterator::TableIterator(TableHeap *_table_heap, RowId _rid)
    : table_heap_(_table_heap),row_(new Row(_rid)){
      //Row row_(_rid);
  if (row_->GetRowId().GetPageId() != INVALID_PAGE_ID) {
    table_heap_->GetTuple(row_,nullptr);
    //row=&row_;
  }
}

bool TableIterator::operator==(const TableIterator &itr) const {    //id相同
  return (itr.row_->GetRowId()==this->row_->GetRowId());
}

bool TableIterator::operator!=(const TableIterator &itr) const {    //id不相同
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  // ASSERT(false, "Not implemented yet.");
  // page_id_t page_id = this->current_page_id_;
  // TablePage *page = reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(page_id));
  // RowId FirstTupleId;
  // ASSERT(page->GetFirstTupleRid(&FirstTupleId), "No valid tuple in this page!");
  // for (size_t i = 0; i < this->current_slot_id_; i++) {
  //   RowId next_rid;
  //   ASSERT(page->GetNextTupleRid(FirstTupleId, &next_rid), "No valid tuple in this page!");
  //   FirstTupleId = next_rid;
  // }
  // Row *row_new = new Row(FirstTupleId);
  return *row_;
}

Row *TableIterator::operator->() {
  // page_id_t page_id = this->current_page_id_;
  // TablePage *page = reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(page_id));
  // RowId FirstTupleId;
  // ASSERT(page->GetFirstTupleRid(&FirstTupleId), "No valid tuple in this page!");
  // for (size_t i = 0; i < this->current_slot_id_; i++) {
  //   RowId next_rid;
  //   ASSERT(page->GetNextTupleRid(FirstTupleId, &next_rid), "No valid tuple in this page!");
  //   FirstTupleId = next_rid;
  // }
  ASSERT(*this!=table_heap_->End(),"at end");
  return row_;
}

TableIterator &TableIterator::operator++() {
  // page_id_t page_id = this->current_page_id_;
  // TablePage *page = reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(page_id));
  // if (this->current_slot_id_ == page->GetSlotCount()) {
  //   this->current_slot_id_ = 0;
  //   page_id_t next_page = page->GetNextPageId();
  //   if (next_page == INVALID_PAGE_ID) {
  //     this->current_page_id_ = INVALID_PAGE_ID;
  //     return *this;
  //   }
  // } else {
  //   this->current_slot_id_++;
  // }
  BufferPoolManager *buffer_pool_manager=table_heap_->buffer_pool_manager_;//得到buffer
  //先读取这个row所在的page
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(row_->GetRowId().GetPageId())); 
  RowId next_row_id;//下一个row的rid
  bool flag;
  flag=page->GetNextTupleRid(row_->GetRowId(),&next_row_id);
  if(flag==false){
    while(page->GetNextPageId()!=INVALID_PAGE_ID){      //这个页中找不到，则继续在下面的页中找
      auto next_page=static_cast<TablePage *>(buffer_pool_manager->FetchPage(page->GetNextPageId()));
      buffer_pool_manager->UnpinPage(page->GetTablePageId(),false);
      page=next_page;
      if(page->GetFirstTupleRid(&next_row_id)){   //如果找到了，则结束
        break;
      }
    }
  }
  delete row_;//删掉旧的
  row_= new Row(next_row_id);
  if(*this!=table_heap_->End()){//不是末尾
    Transaction *txn=nullptr;
    table_heap_->GetTuple(row_,txn);
  }
  buffer_pool_manager->UnpinPage(page->GetTablePageId(),false);
  return *this;
}


TableIterator TableIterator::operator++(int) {
  //return TableIterator();
  TableIterator itr(*this);
  ++(*this);
  return itr;
}