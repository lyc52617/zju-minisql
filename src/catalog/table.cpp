#include "catalog/table.h"
#include "glog/logging.h"
#include "record/column.h"
#include "record/schema.h"
uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t size0=sizeof(TABLE_METADATA_MAGIC_NUM);
  uint32_t size1=sizeof(table_id_);
  uint32_t size2=table_name_.size();
  uint32_t size3=sizeof(root_page_id_);
  //LOG(INFO)<<size2<<std::endl;
  memcpy(buf,&TABLE_METADATA_MAGIC_NUM,size0);
  memcpy(buf+size0,&table_id_,size1);
  memcpy(buf+size0+size1,&size2,sizeof(size2));
  memcpy(buf+size0+size1+sizeof(size2),table_name_.c_str(),size2);
  memcpy(buf+size0+size1+size2+sizeof(size2),&root_page_id_,size3);
  //LOG(INFO)<<schema_->GetColumnCount();
  uint32_t size4=schema_->SerializeTo(buf+size0+size1+size2+size3+sizeof(size2));
  //LOG(INFO)<<size4;
  return (size0+size1+sizeof(size2)+size2+size3+size4);
}

uint32_t TableMetadata::GetSerializedSize() const {
  return sizeof(TABLE_METADATA_MAGIC_NUM)+sizeof(table_id_)+sizeof(uint32_t)+table_name_.size()+sizeof(root_page_id_)+schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  uint32_t magicnum=MACH_READ_INT32(buf);
  table_id_t tableid=MACH_READ_UINT32(buf+sizeof(magicnum));
  uint32_t length=MACH_READ_UINT32(buf+sizeof(magicnum)+sizeof(tableid));
  std::string tablename;
  tablename.append(buf+sizeof(magicnum)+sizeof(tableid)+sizeof(length),length);
  LOG(INFO)<<magicnum<<length<<tablename<<std::endl;
  page_id_t rootpageid=MACH_READ_INT32(buf+sizeof(magicnum)+sizeof(tableid)+length+sizeof(length));
  uint32_t vectors = MACH_READ_UINT32(buf+sizeof(magicnum)+sizeof(tableid)+length+sizeof(length)+sizeof(rootpageid));
  Schema news(std::vector<Column*> columns);
  Schema *schema=new Schema(std::vector<Column *>(vectors));
  uint32_t offset=Schema::DeserializeFrom(buf+sizeof(magicnum)+sizeof(tableid)+length+sizeof(length)+sizeof(rootpageid),schema,heap);
  LOG(INFO)<<schema->GetColumnCount();
  table_meta=Create(tableid,tablename,rootpageid,schema,heap);
  //LOG(INFO)<<offset;
  return sizeof(magicnum)+sizeof(tableid)+sizeof(length)+length+sizeof(rootpageid)+offset;
};

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
    : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
