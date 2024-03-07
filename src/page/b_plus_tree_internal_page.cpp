#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"


/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {

  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  //BufferPoolManager->UnpinPage(page_id, true);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  //if(index > GetMaxSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for(int i = 0;i < GetSize();i++)
  {
    if(array_[i].second == value)
      return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  /*int i = 0;
  //while(1) {
    for (i = 1; i < GetSize()-1; i++) {
      if (comparator(key, array_[i + 1].first) == 0) {
        return array_[i].second;
      }
      else if (comparator(key, array_[i + 1].first) == -1) {
        return array_[i].second;
      }
    }
    if (comparator(key, array_[i].first)) return array_[i].second;
    return INVALID_PAGE_ID;
  //}*/


  int left = 1;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(KeyAt(mid), key) > 0) {  // 下标还需要减小
      right = mid - 1;
    } else {  // 下标还需要增大
      left = mid + 1;
    }
  }  // upper_bound
  int target_index = left;
  assert(target_index - 1 >= 0);
  // 注意，返回的value下标要减1，这样才能满足key(i-1) <= subtree(value(i)) < key(i)
  return ValueAt(target_index - 1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int i = 0;
  int size = GetSize();
  for(i = 0;i < size;i++)
  {
    if(array_[i].second == old_value)
    {
      //if(i == size - 1)
      //{
      //break;
      //return -1;
      //}
      for(int j = GetSize();j > i + 1;j--)
      {
        array_[j] = array_[j-1];
      }
      array_[i+1].second = new_value;
      array_[i+1].first = new_key;
      SetSize(size+1);
    }
  }
  return size+1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS  //!!!!
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                               BufferPoolManager *buffer_pool_manager) {
  auto size = recipient->GetSize();
  for(int i = (GetSize()+1)/2;i < GetSize();i++)
  {
    recipient->array_[i-((GetSize()+1)/2)] = array_[i];
    //free(array_[i]);
  }
  SetSize((GetSize()+1)/2);
  recipient->SetSize(size+GetSize()-(GetSize()+1)/2);
  buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
  for(int i = 0;i < recipient->GetSize();i++){
    auto *page = buffer_pool_manager->FetchPage(recipient->ValueAt(i));
    if(page != nullptr)
    {
      auto *page_ = reinterpret_cast<BPlusTreePage *>(page->GetData());
      page_->SetParentPageId(recipient->GetPageId());
      buffer_pool_manager->UnpinPage(recipient->ValueAt(i),true);
    }
  }
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS   //!!!
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  page_id_t page_id = GetPageId();
  page_id_t page_id_;
  auto *page = buffer_pool_manager->FetchPage(page_id);
  if (page != nullptr) {
    auto *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    int i = 0;
    for(i = 0;i < size;i++)
    {
      node->array_[i] = items[i];
      page_id_ = node->array_[i].second;
      auto *pagetoupdate = buffer_pool_manager->FetchPage(page_id_);
      if(pagetoupdate != nullptr)
      {
        auto *nodetoupdate = reinterpret_cast<BPlusTreeInternalPage *>(pagetoupdate->GetData());
        nodetoupdate->SetParentPageId(page_id);
      }
      buffer_pool_manager->UnpinPage(page_id_,true);
    }

    buffer_pool_manager->UnpinPage(page_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int i = 0;
  for(i = index;i < GetSize()-1;i++)
  {
    array_[i] = array_[i+1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  SetSize(0);
  return array_[0].second;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS  //!!!
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                              BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0,middle_key);
  recipient->CopyNFrom(array_,GetSize(),buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS    //!!!
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                     BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0,middle_key);
  recipient->CopyLastFrom(array_[0],buffer_pool_manager);
  Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS  //!!!
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  page_id_t page_id = GetPageId();
  auto *page = buffer_pool_manager->FetchPage(page_id);
  if (page != nullptr) {
    auto *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    int size = GetSize();
    node->array_[size] = pair;
    page_id_t page_id_ = pair.second;
    auto *pagetoupdate = buffer_pool_manager->FetchPage(page_id_);
    if(pagetoupdate != nullptr)
    {
      auto *nodetoupdate = reinterpret_cast<BPlusTreeInternalPage *>(pagetoupdate->GetData());
      nodetoupdate->SetParentPageId(page_id);
    }
    buffer_pool_manager->UnpinPage(page_id_,true);
    buffer_pool_manager->UnpinPage(page_id, true);
  }
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->SetKeyAt(0,middle_key);
  recipient->CopyFirstFrom(array_[GetSize()-1],buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  page_id_t page_id = GetPageId();
  auto *page = buffer_pool_manager->FetchPage(page_id);
  if (page != nullptr) {
    auto *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    int size = node->GetSize();
    for(int i = size;i > 0;i--)
    {
      node->array_[i] = node->array_[i-1];
    }
    node->array_[0] = pair;
    page_id_t page_id_ = pair.second;
    auto *pagetoupdate = buffer_pool_manager->FetchPage(page_id_);
    if(pagetoupdate != nullptr)
    {
      auto *nodetoupdate = reinterpret_cast<BPlusTreeInternalPage *>(pagetoupdate->GetData());
      nodetoupdate->SetParentPageId(page_id);
    }
    buffer_pool_manager->UnpinPage(page_id_,true);
    buffer_pool_manager->UnpinPage(page_id, true);
  }
  IncreaseSize(1);
}

template
    class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
    class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
    class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
    class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
    class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
    class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;