#include "page/b_plus_tree_page.h"

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
bool BPlusTreePage::IsLeafPage() const { //如果是叶子页类型，返回true；否则返回false
  if(page_type_ == IndexPageType::LEAF_PAGE) return true;
  else return false;
}

bool BPlusTreePage::IsRootPage() const { //如果parent无效，说明这个是根节点
  if (parent_page_id_ == INVALID_PAGE_ID) return true;
  else return false;
}

void BPlusTreePage::SetPageType(IndexPageType page_type) { //给当前页的类型赋值
  page_type_ = page_type;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const { //直接返回size_
  return size_;
}

void BPlusTreePage::SetSize(int size) { //直接把size_设成size???
  size_ = size;
}

void BPlusTreePage::IncreaseSize(int amount) { //直接把size_加上amount???
  size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
int BPlusTreePage::GetMaxSize() const { //直接返回max_size_
  return max_size_;
}

void BPlusTreePage::SetMaxSize(int size) { //直接修改max_size_????
  max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
int BPlusTreePage::GetMinSize() const { //手动加上了向上取整
  if(max_size_ % 2 == 1) return max_size_ / 2 + 1;
  else return max_size_ / 2;
}

/*
 * Helper methods to get/set parent page id
 */
page_id_t BPlusTreePage::GetParentPageId() const { //直接返回parent_page_id_
  return parent_page_id_;
}

void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) { //直接修改parent_page_id_ = parent_page_id
  parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const { //直接返回page_id_
  return page_id_;
}

void BPlusTreePage::SetPageId(page_id_t page_id) { //直接修改
  page_id_ = page_id;
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { //额 这个不知道是啥 但是不用改
  lsn_ = lsn;
}