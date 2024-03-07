#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

//构造函数

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(int index, B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page_,BufferPoolManager *buf ) {
  this->buffer_pool_manager_=buf;
  this->index_=index;
  this->leaf_page_=leaf_page_;
}

//析构函数
INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  page_id_t page_id;
  page_id=this->leaf_page_->GetPageId();
  this->buffer_pool_manager_->UnpinPage(page_id,false);
}

//访问当前迭代器所在位置的
INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  return leaf_page_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if(this->index_+1!=leaf_page_->GetSize()){
    this->index_++;
    return *this;
  }
  else if(this->index_+1==leaf_page_->GetSize()){
    this->index_++;
    if(leaf_page_->GetNextPageId()!=INVALID_PAGE_ID){
      page_id_t next_page_id;
      next_page_id=this->leaf_page_->GetNextPageId();
      buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(),false);
      Page* next_page;
      next_page=buffer_pool_manager_->FetchPage(next_page_id);
      this->leaf_page_=reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page->GetData());
      this->leaf_page_=reinterpret_cast<leaf_page*>(next_page->GetData());
      this->index_=0;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  if(itr.leaf_page_ == this->leaf_page_ && itr.index_ == this->index_){
    return true;
  }
  else return false;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  if(itr.leaf_page_ == this->leaf_page_ && itr.index_ == this->index_){
    return false;
  }
  else return true;
}

template
    class IndexIterator<int, int, BasicComparator<int>>;

template
    class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
    class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
    class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
    class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
    class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
