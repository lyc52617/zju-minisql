#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"


class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  explicit TableIterator();

  TableIterator(const TableIterator &other){
    // current_page_id_=other.current_page_id_;
    // current_slot_id_=other.current_slot_id_;
    table_heap_=other.table_heap_;
    // txn_=other.txn_;
    row_=new Row (*other.row_);
  }

  TableIterator(TableHeap *_table_heap, RowId _rid);


  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

  TableIterator& operator=(const TableIterator &other);

private:
  // add your own private member variables here
  // page_id_t current_page_id_ ;
  // uint32_t current_slot_id_;
  TableHeap *table_heap_;
  // Transaction *txn_;
  Row* row_;
};

#endif //MINISQL_TABLE_ITERATOR_H
