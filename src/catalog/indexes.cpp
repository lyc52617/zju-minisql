#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  char* buffer = buf;
  MACH_WRITE_TO(uint32_t, buffer, INDEX_METADATA_MAGIC_NUM);
  buffer+=sizeof(uint32_t);

  MACH_WRITE_TO(index_id_t, buffer,GetIndexId());
  buffer+=sizeof(uint32_t);

  uint32_t len = index_name_.size();
  memcpy(buffer, &len, sizeof(uint32_t));
  memcpy(buffer + sizeof(uint32_t), index_name_.c_str(), len);
  buffer = buffer + len + sizeof(uint32_t);

  MACH_WRITE_TO(table_id_t, buffer,GetTableId());
  buffer+=sizeof(uint32_t);

  len = key_map_.size();//获取vector的长度
  MACH_WRITE_TO(uint32_t, buffer, len);
  buffer+=sizeof(uint32_t);
  for(int i=0;i<int(len);i++){
    MACH_WRITE_TO(uint32_t, buffer,key_map_[i]);
    buffer+=sizeof(uint32_t);
  }
  return buffer-buf;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  uint32_t ans=0;
  uint32_t l1 = index_name_.size();
  uint32_t l2 = key_map_.size();
  ans = l1+4*l2+4*5;
  return ans;
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  index_id_t index_id;
  string index_name;
  table_id_t table_id;
  vector<uint32_t> key_map;
  char* buffer = buf;
  [[maybe_unused]]uint32_t val = MACH_READ_FROM(uint32_t, buffer); buffer+=4;//magic number
  index_id = MACH_READ_FROM(uint32_t, buffer); buffer+=4;

  uint32_t name_len = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
  index_name.append(buffer,name_len);
  buffer+=name_len;
  table_id = MACH_READ_FROM(uint32_t, buffer); buffer+=4;

  uint32_t map_len = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
  for(int i=0;i<int(map_len);i++){
    uint32_t tmp = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
    key_map.push_back(tmp);
  }
  index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map, heap);
  return buffer-buf;
}