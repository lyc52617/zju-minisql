#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  /*0. Define the offset of the buffer during serialization*/
  uint32_t buf_ofs = 0;
  /*1. Serialize the SCHEMA_MAGIC_NUM*/
  MACH_WRITE_UINT32(buf + buf_ofs, SCHEMA_MAGIC_NUM);
  buf_ofs += sizeof(uint32_t);
  /*2. Serialize the Columns*/
  /*2.1 Serialize the number of columns*/
  MACH_WRITE_UINT32(buf + buf_ofs, columns_.size());
  buf_ofs += sizeof(uint32_t);
  /*2.2 Serialize the data of columns*/
  for (uint32_t i = 0; i < columns_.size(); i++) buf_ofs += columns_[i]->SerializeTo(buf + buf_ofs);
  return buf_ofs;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t buf_ofs = 0;
  buf_ofs += 2 * sizeof(uint32_t);
  for (uint32_t i = 0; i < columns_.size(); i++) buf_ofs += columns_[i]->GetSerializedSize();
  return buf_ofs;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  /*0. Define the offset of the buffer during deserialization*/
  uint32_t buf_ofs = 0;
  /*1. Deserialize the MAGINC_NUM*/
  uint32_t magic_num = MACH_READ_UINT32(buf + buf_ofs);
  ASSERT(magic_num == Schema::SCHEMA_MAGIC_NUM, "SCHEMA_MAGIC_NUM is wrong");  // Report wrong message
  buf_ofs += sizeof(uint32_t);
  /*2. Deserialize the Columns*/
  /*2.1 Deserialize the number of columns*/
  uint32_t columns_size = MACH_READ_UINT32(buf + buf_ofs);
  buf_ofs += sizeof(uint32_t);
  /*2.2 Deserialize the columns*/
  std::vector<Column *> columns;
  columns.resize(columns_size);
  for (uint32_t i = 0; i < columns_size; i++) buf_ofs += Column::DeserializeFrom(buf+buf_ofs, columns[i], heap);
  schema = ALLOC_P(heap, Schema)(columns);
  LOG(INFO) << schema->GetColumnCount();
  return buf_ofs;
}