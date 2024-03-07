#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  char *buffer = buf;
  int len = fields_.size();
  for(int i=0;i<len;i++){
    auto* tmp = this->fields_[i];//取出row中的每个Field指
    // std::cout<<i<<std::endl;
    buffer+=tmp->SerializeTo(buffer);
  }
  return buffer-buf;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  // replace with your code here
  
  uint32_t len=schema->GetColumnCount();
  MemHeap *heap = new SimpleMemHeap();
  char *buffer = buf;
  for(int i=0;i<int(len);i++){
    Field *f; 
    buffer+=f->Field::DeserializeFrom(buffer, schema->GetColumn(i)->GetType(), &f, false, heap);
    fields_.push_back(f);
  }
  return buffer-buf;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here
  uint32_t ans=0;
  int len = fields_.size();
  for(int i=0;i<len;i++){
    auto tmp = this->fields_[i];//取出row中的每个Field指针
    ans+=tmp->GetSerializedSize();
  }
  return ans;
}