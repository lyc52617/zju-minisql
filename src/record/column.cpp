#include "record/column.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  /*0. Define the offset of the buffer during serialization*/
  uint32_t buf_ofs = 0;
  /*1. Serialize the MAGIC_NUM*/
  MACH_WRITE_UINT32(buf + buf_ofs, COLUMN_MAGIC_NUM);
  buf_ofs += sizeof(uint32_t);
  /*2. Serialize the Column_name*/
  /*2.1 Serialize the length of the column_name*/
  MACH_WRITE_UINT32(buf + buf_ofs, name_.length());
  buf_ofs += sizeof(uint32_t);
  /*2.2 Serialize the data of the column_name*/
  MACH_WRITE_STRING(buf + buf_ofs, name_);
  buf_ofs += name_.length();
  /*3. Serialize the typde of the data*/
  MACH_WRITE_UINT32(buf + buf_ofs, type_);
  buf_ofs += sizeof(uint32_t);
  /*4. Serialize the length of the data*/
  MACH_WRITE_UINT32(buf + buf_ofs, len_);
  buf_ofs += sizeof(uint32_t);
  /*5. Serialize the index of the column*/
  MACH_WRITE_UINT32(buf + buf_ofs, table_ind_);
  buf_ofs += sizeof(uint32_t);
  /*6. Serialize the nullabe status of the column*/
  MACH_WRITE_TO(bool, buf + buf_ofs, nullable_);
  buf_ofs += sizeof(bool);
  /*7. Serialize the unique status of the column*/
  MACH_WRITE_TO(bool, buf + buf_ofs, unique_);
  buf_ofs += sizeof(bool);
  return buf_ofs;
}

uint32_t Column::GetSerializedSize() const {
  uint32_t buf_ofs = 0;
  buf_ofs += 4 * sizeof(uint32_t) + 2 * sizeof(bool) + MACH_STR_SERIALIZED_SIZE(name_);
  return buf_ofs;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  /*0. Define the offset of the buffer during serialization*/
  uint32_t buf_ofs = 0;
  /*1. Deserialize the MAGIC_NUM*/
  uint32_t magic_num = MACH_READ_UINT32(buf + buf_ofs);
  ASSERT(magic_num == Column::COLUMN_MAGIC_NUM, "COLUMN_MAGIC_NUM is wrong");  // Report wrong message
  buf_ofs += sizeof(uint32_t);
  /*2. Deserialize the Column_Name*/
  /*2.1 Deserialize the length of the column name*/
  uint32_t column_name_len = MACH_READ_UINT32(buf + buf_ofs);
  buf_ofs += sizeof(uint32_t);
  /*2.2 Deserialize the column name*/
  char *c_name = new char[column_name_len];
  memcpy(c_name, buf+buf_ofs, column_name_len);
  std::string name(c_name);
  buf_ofs += column_name_len;
  /*3. Deserialize the type of the column*/
  TypeId type = (TypeId)MACH_READ_UINT32(buf + buf_ofs);
  buf_ofs += sizeof(uint32_t);
  /*4. Deserialize the length of the column*/
  uint32_t len = MACH_READ_UINT32(buf + buf_ofs);
  buf_ofs += sizeof(uint32_t);
  /*5. Deserialize the position of the column*/
  uint32_t table_ind = MACH_READ_UINT32(buf + buf_ofs);
  buf_ofs += sizeof(uint32_t);
  /*6. Deserialize the nullalbe status of the column*/
  bool nullable = MACH_READ_FROM(bool, buf + buf_ofs);
  buf_ofs += sizeof(bool);
  /*7. Deserialize the unique status of the column*/
  bool unique = MACH_READ_FROM(bool, buf + buf_ofs);
  buf_ofs += sizeof(bool);
  if (type != TypeId::kTypeChar)
    column = ALLOC_P(heap, Column)(name, type, table_ind, nullable, unique);
  else
    column = ALLOC_P(heap, Column)(name, type, len, table_ind, nullable, unique);
  return buf_ofs;
}
