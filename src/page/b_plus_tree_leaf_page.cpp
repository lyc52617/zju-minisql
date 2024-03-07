#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageId(page_id);
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetMaxSize(max_size);
  this->SetSize(0);
  this->SetParentPageId(parent_id);
  this->SetNextPageId(INVALID_PAGE_ID);

}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return this->next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  this->next_page_id_=next_page_id;
}

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int i;
  for(i=0;i<this->GetSize();i++){
    if(comparator(key,array_[i].first)<=0){
      break;
    }
  }
  return i;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key=array_[index].first;
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int i;
  for(i=0;i<this->GetSize();i++){
    if(comparator(array_[i].first,key)==0){  //如果已经存在，不再插入
      std::cout<<"return directly"<<std::endl;
      return this->GetSize();
    }
  }

  for(i=0;i<this->GetSize();i++){
    if(comparator(array_[i].first,key)>0){  //找到key该放的位置
      break;
    }
  }
  if(i==this->GetSize()){
    int temp=this->GetSize();
    array_[temp]=std::make_pair(key,value);
    this->IncreaseSize(1);
  }
  else{
    int j;
    for(j=this->GetSize();j>i;j--){
      array_[j]=array_[j-1];
    }
    this->IncreaseSize(1);
    array_[i]=std::make_pair(key,value);
  }

  //std::cout <<"leaf success"<<std::endl;

  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient,BufferPoolManager *buffer_pool_manager) {
  int i;
  for(i=(this->GetSize()-this->GetSize()/2);i<this->GetSize();i++){
    recipient->array_[recipient->GetSize()]=this->array_[i];
    recipient->IncreaseSize(1);
  }
  this->SetSize(this->GetSize()-this->GetSize()/2);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  int i;
  for(i=0;i<size;i++){
    this->array_[i]=items[i];
  }
  this->IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {

  //std::cout <<"start lookup"<<std::endl;

  if(GetSize() <= 0)
  {
    //std::cout << "fail0"<<std::endl;
    return false;
  }

  if(comparator(key,KeyAt(0))<0)
  {
    //std::cout << "fail"<<std::endl;
    return false;
  }
  if(comparator(key,KeyAt(this->GetSize()-1))>0)
  {
    //std::cout <<"fail2"<<std::endl;
    return false;
  }


  int i;
  for(i=0;i<this->GetSize();i++){
    if(comparator(key,this->array_[i].first)==0){
      value=this->array_[i].second;
      return true;
    }
  }
  return false;

  /*
  int target_index = KeyIndex(key, comparator);                                  // 查找第一个>=key的的下标
  if (target_index == GetSize() || comparator(key, KeyAt(target_index)) != 0) {  // =key的下标不存在（只有>key的下标）
    // LOG_INFO("leaf node Lookup FAILURE key>all not ==");
    return false;
  }
  // LOG_INFO("leaf node Lookup SUCCESS index=%d", target_index);
  value = array_[target_index].second;  // value是传出参数
  return true;
  */
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator,BufferPoolManager *buffer_pool_manager) {
  int i;
  for(i=0;i<this->GetSize();i++){
    if(comparator(key,this->array_[i].first)==0){
      break;
    }
  }
  if(i!=this->GetSize()){
    int j;
    for(j=i;j<this->GetSize();j++){
      this->array_[j]=this->array_[j+1];
    }
    this->IncreaseSize(-1);
  }
  return this->GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  recipient->CopyNFrom(this->array_,this->GetSize());
  recipient->SetNextPageId(this->GetNextPageId());
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  recipient->array_[recipient->GetSize()]=this->array_[0];
  recipient->IncreaseSize(1);
  int i;
  for(i=0;i<this->GetSize()-1;i++){
    this->array_[i]=this->array_[i+1];
  }
  this->IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  this->array_[this->GetSize()]=item;
  this->IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  int i;
  for(i=recipient->GetSize();i>0;i--){
    recipient->array_[i]=recipient->array_[i-1];
  }
  recipient->array_[0]=this->array_[this->GetSize()-1];
  recipient->IncreaseSize(1);
  this->IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  int i;
  for(i=this->GetSize();i>0;i--){
    this->array_[i]=this->array_[i-1];
  }
  this->array_[0]=item;
  this->IncreaseSize(1);
}

template
    class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
    class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
    class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
    class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
    class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
    class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;