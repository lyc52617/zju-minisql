#include "executor/execute_engine.h"
#include "glog/logging.h"
#include <iostream>
#include <sstream>
#include<parser/syntax_tree_printer.h>
#include<sys/time.h>
ExecuteEngine::ExecuteEngine() {

}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  ofstream ofs("test.dot",ios_base::out);
  SyntaxTreePrinter ptr(ast);
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:{
    ptr.PrintTree(ofs);
      return ExecuteCreateDatabase(ast, context);
    }
    case kNodeDropDB:
    ptr.PrintTree(ofs);
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
    ptr.PrintTree(ofs);
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
    ptr.PrintTree(ofs);
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
    ptr.PrintTree(ofs);
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
    ptr.PrintTree(ofs);
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
    ptr.PrintTree(ofs);
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
    ptr.PrintTree(ofs);
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
    ptr.PrintTree(ofs);
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
    ptr.PrintTree(ofs);
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
    ptr.PrintTree(ofs);
      return ExecuteSelect(ast, context);
    case kNodeInsert:
    ptr.PrintTree(ofs);
      return ExecuteInsert(ast, context);
    case kNodeDelete:
    ptr.PrintTree(ofs);
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
    ptr.PrintTree(ofs);
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
    ptr.PrintTree(ofs);
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
    ptr.PrintTree(ofs);
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
    ptr.PrintTree(ofs);
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
    ptr.PrintTree(ofs);
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
    ptr.PrintTree(ofs);
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  ofs.close();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  pSyntaxNode temp=ast->child_;
     std::string dbname=temp->val_;

    std::unordered_map<std::string, DBStorageEngine *>::const_iterator it;
    for(it=dbs_.begin();it!=dbs_.end();it++){
      if(it->first==dbname){
        return DB_FAILED;
      }
    }
  DBStorageEngine* db = new DBStorageEngine(ast->child_->val_);
  dbs_[ast->child_->val_]=db;
  cout<<"Create database success"<<endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  pSyntaxNode temp=NULL;
  temp=ast->child_;
  if(temp->type_!=kNodeIdentifier){
    cout<<"drop database failed"<<endl;
    return DB_FAILED;
  }
  else{
    std::string dbname=temp->val_;

    std::unordered_map<std::string, DBStorageEngine *>::const_iterator it;
    for(it=dbs_.begin();it!=dbs_.end();it++){
      if(it->first==dbname){
        if(dbname==current_db_)
        current_db_.clear();
        dbs_.erase(it);
        std::cout<<"Drop database success"<<std::endl;
        return DB_SUCCESS;
      }
    }
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  std::unordered_map<std::string, DBStorageEngine *>::const_iterator it;
  std::cout<<" Show Databases:"<<std::endl;
  for(it=dbs_.begin();it!=dbs_.end();it++)
  {
    std::cout<<it->first<<std::endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  pSyntaxNode temp=NULL;
  temp=ast->child_;
  if(temp->type_!=kNodeIdentifier)
    return DB_FAILED;
  else{
    std::string dbname=temp->val_;
    std::unordered_map<std::string, DBStorageEngine *>::const_iterator it;
    for(it=dbs_.begin();it!=dbs_.end();it++)
    {
      if(dbname==it->first)
      {
        current_db_=it->first;
        cout<<"Use database "<<dbname<<std::endl;
        return DB_SUCCESS;
      }
    }
    std::cout<<"No such database"<<std::endl;
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if(current_db_.empty())
  {
    cout<<"Haven't chosen database"<<std::endl;
    return DB_FAILED;
  }
  cout<<"------Tables------"<<endl;
  vector<TableInfo* > tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for(auto p=tables.begin();p<tables.end();p++){
    cout<<(*p)->GetTableName()<<endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
if(current_db_.empty())
  {
    cout<<"Haven't chosen database"<<std::endl;
    return DB_FAILED;
  }
   pSyntaxNode temp1=ast->child_;
  if(temp1->type_!=kNodeIdentifier)
    return DB_FAILED;
  std::string tablename=temp1->val_;
  // std::string currentdb=current_db_;
  // std::unordered_map<std::string, DBStorageEngine *>::iterator iter;
  // iter=dbs_.find(currentdb);
  // DBStorageEngine *current=iter->second;
  TableInfo *tableinfo=nullptr;
  pSyntaxNode temp2=temp1->next_;
 // std::cout<<"1"<<endl;
  if(temp2->type_!=kNodeColumnDefinitionList)
    return DB_FAILED;
  temp2=temp2->child_;    //指向第一个kNodeColumnDefinition
  std::vector<Column *> columns;
  //std::cout<<"2"<<endl;
  //cout<<"table_name:"<<table_name<<endl;
  vector<Column*>vec_col;
  while(temp2!=nullptr&&temp2->type_==kNodeColumnDefinition){
    bool is_unique = false;
    if (temp2->val_!=nullptr){
      string s = temp2->val_;
      is_unique = (s=="unique");
    }
    string column_name =  temp2->child_->val_;
    //cout<<"column_name:"<<column_name<<endl;
    string column_type =  temp2->child_->next_->val_;
    //cout<<"column_type:"<<column_type<<endl;
    int cnt = 0;
    Column *now;
    if(column_type=="int"){
      now = new Column(column_name,kTypeInt,cnt,true,is_unique);
    }
    else if(column_type=="char"){
      string len =  temp2->child_->next_->child_->val_;
      //cout<<"len:"<<len<<endl;
      long unsigned int a = -1;
      if (len.find('.')!=a){
        cout<<"Semantic Error, String Length Can't be a Decimal!"<<endl;
        return DB_FAILED;
      }
      int length = atoi( temp2->child_->next_->child_->val_);
      if (length<0){
        cout<<"Semantic Error, String Length Can't be Negative!"<<endl;
        return DB_FAILED;
      }
      now = new Column(column_name,kTypeChar,length,cnt,true,is_unique);
    }
    else if(column_type=="float"){
      now = new Column(column_name,kTypeFloat,cnt,true,is_unique);
    }
    else{
      cout<<"Error Column Type!"<<endl;
      return DB_FAILED;
    }
    //cout<<"is_unique:"<<is_unique<<endl;
    vec_col.push_back(now);
    temp2 =  temp2->next_;
    cnt++;
  }
  Schema *schema = new Schema(vec_col);
  //cout<<"SUCCEED!"<<endl;
  dberr_t IsCreate=dbs_[current_db_]->catalog_mgr_->CreateTable(tablename,schema,nullptr,tableinfo);
  if(IsCreate==DB_TABLE_ALREADY_EXIST){
    cout<<"Table Already Exist!"<<endl;
    return IsCreate;
  }
  if ( temp2!=nullptr){
    cout<<"primary key's ";
    pSyntaxNode key_pointer = temp2->child_;
    vector <string>primary_keys;
    while(key_pointer!=nullptr){
      string key_name = key_pointer->val_ ;
      cout<<"name:"<<key_name<<endl;
      primary_keys.push_back(key_name);
      key_pointer = key_pointer->next_;
    }
    CatalogManager* current_catalog=dbs_[current_db_]->catalog_mgr_;
    IndexInfo* indexinfo=nullptr;
    string index_name = tablename + "_primarykey";
    cout<<"index_name:"<<index_name<<endl;
    current_catalog->CreateIndex(tablename,index_name,primary_keys,nullptr,indexinfo);
  }
  // for (auto r = vec_col.begin() ; r != vec_col.end(); r ++ ){
  //   if ((*r)->GetUnique()){
  //     string unique_index_name = tablename +(*r)->GetName()+"_unique";
  //     CatalogManager* current_catalog=dbs_[current_db_]->catalog_mgr_;
  //     vector <string>unique_attribute_name = {(*r)->GetName()};
  //     IndexInfo* indexinfo=nullptr;
  //     current_catalog->CreateIndex(tablename,unique_index_name,unique_attribute_name,nullptr,indexinfo);
  //   }
  // }
  cout<<"Create table success"<<std::endl;
  return IsCreate;
 
  // while(temp2!=NULL&&temp2->type_==kNodeColumnDefinition){  //不为空并且不是primary key的情况
  //   std::cout<<"3"<<endl;
  //   bool unique_flag=false;
  //   if(temp2->val_!=nullptr){
  //     string s_temp=temp2->val_;  //转换为string
  //     if(s_temp=="unique"){
  //         std::cout<<"unique"<<endl;
  //         unique_flag=true;
  //     }
  //   }
  //   std::string columnname;
  //   TypeId columntype;
  //   pSyntaxNode temp3=temp2->child_;    //knodeIdentifier
  //   if(temp3->type_!=kNodeIdentifier)
  //     return DB_FAILED;
  //   columnname=temp3->val_;
  //   pSyntaxNode temp4=temp3->next_;     //knodeColumnType
  //   if(temp4->type_!=kNodeColumnType)
  //     return DB_FAILED;
  //   if(strcmp(temp4->val_,"int")==0){    //根据type来设置new column
  //     columntype=kTypeInt;
  //     std::cout<<"int"<<endl;
  //   }
  //   if(strcmp(temp4->val_,"char")==0){
  //     columntype=kTypeChar;
  //     std::cout<<"char"<<endl;
  //   }
  //   if(strcmp(temp4->val_,"float")==0){
  //     columntype=kTypeFloat;
  //     std::cout<<"float"<<endl;
  //   }
  //   if(columntype!=kTypeChar){
  //     std::cout<<"push"<<endl;
  //     std::cout<<columntype<<endl;
  //     Column newcolumn= new Column(columnname,columntype,tableindex,true,unique_flag);
  //     columns.push_back(&newcolumn);
  //   }
  //   else{     //char类型的长度
  //     std::cout<<"push_c"<<endl;
  //     temp4=temp4->child_;
  //     if(temp4->type_!=kNodeNumber)
  //     return DB_FAILED;
  //     uint32_t length=0,len=strlen(temp4->val_);
  //     for(uint32_t i=0;i<len;i++)
  //     length+=(temp4->val_[i]-'0')*pow(10,len-i-1);
  //     std::cout<<columntype<<endl;
  //     Column newcolumn=new Column(columnname,columntype,length,tableindex,true,unique_flag);
  //     columns.push_back(&newcolumn);
  //   }
  //   tableindex++;
  //   temp2=temp2->next_;
  // }
  // std::cout<<"before create"<<endl;
  // Schema *schema = new Schema(columns);
  // //auto schema = std::make_shared<Schema>(columns);
  // dberr_t table_flag;    //创建table
  // Transaction txn;
  // table_flag=dbs_[current_db_]->catalog_mgr_->CreateTable(tablename,schema,&txn,tableinfo);
  // std::cout<<"after create"<<endl;
  // if(table_flag==DB_TABLE_ALREADY_EXIST){ //判断是否已经存在这个table了
  //   std::cout<<"Already have this table"<<endl;
  //   return table_flag;
  // }
  // if(temp2==nullptr){   //即是到头结束的情况， 返回成功
  //   return DB_SUCCESS;
  // }
  // else{   //不是到头结束，即是有kNodeColumnList, 处理primary key
  //   pSyntaxNode primary_key_temp;
  //   primary_key_temp=temp2->child_;
  //   vector<string> keys;  //保存所有的key
  //   while(primary_key_temp!=nullptr){
  //     string one_key=primary_key_temp->val_;
  //     keys.push_back(one_key);
  //     primary_key_temp=primary_key_temp->next_;
  //   }
  //   string index_name="primary_key_of_"+tablename;
  //   std::cout<<"primary keys :"<<index_name<<endl;
  //   IndexInfo *indexinfo;
  //   dbs_[current_db_]->catalog_mgr_->CreateIndex(tablename,index_name,keys,nullptr,indexinfo);
  // }
  // return table_flag;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  pSyntaxNode child_node;
  child_node=ast->child_;
  if(current_db_.empty())
  {
    cout<<"Haven't chosen database"<<std::endl;
    return DB_FAILED;
  }
  if(child_node->type_!=kNodeIdentifier){
    return DB_FAILED;
  }
  std::unordered_map<std::string, DBStorageEngine *>::const_iterator it;
  for(it=dbs_.begin();it!=dbs_.end();it++){
    if(it->first==current_db_){
      string tablename=child_node->val_;
      dberr_t flag;
      flag=it->second->catalog_mgr_->DropTable(tablename);
      if(flag!=DB_TABLE_NOT_EXIST){
        std::cout<<"Drop table success"<<endl;
        return flag;
      }
      else{
        std::cout<<"table does not exist"<<endl;
        return flag;
      }
    }
  }
  std::cout<<"database does not exist"<<endl;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
if(current_db_.empty())
  {
    cout<<"Haven't chosen database"<<std::endl;
    return DB_FAILED;
  }
  std::unordered_map<std::string, DBStorageEngine *>::const_iterator it;
  for(it=dbs_.begin();it!=dbs_.end();it++){
    if(it->first==current_db_){     //找到数据库
      std::cout<<"Show Indexes:"<<endl;
      vector<TableInfo *>table_temp;
      it->second->catalog_mgr_->GetTables(table_temp);
      while(!table_temp.empty()){
        string tablename;
        TableInfo* one_table=table_temp.back();
        tablename=one_table->GetTableName();
        vector<IndexInfo *>indexes;
        it->second->catalog_mgr_->GetTableIndexes(tablename,indexes);
        while(!indexes.empty()){
          std::cout<<"In table: "<<tablename<<" is ";
          IndexInfo * one_index=indexes.back();
          string indexname=one_index->GetIndexName();
          std::cout<<indexname<<endl;
          indexes.pop_back();
        }
        table_temp.pop_back();
      }
    }
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  //std::cout<<"0"<<endl;
  if(current_db_.empty())
  {
    cout<<"Haven't chosen database"<<std::endl;
    return DB_FAILED;
  }
  pSyntaxNode temp1;
  temp1=ast->child_;
  if(temp1->type_!=kNodeIdentifier){
    return DB_FAILED;
  }
  string index_name;
  index_name=temp1->val_;  //获取index的名字 

  pSyntaxNode temp2;
  temp2=temp1->next_;
  if(temp2->type_!=kNodeIdentifier){
    return DB_FAILED;
  }
  string table_name;
  table_name=temp2->val_;       //获取table的名字
  TableInfo * table_info=nullptr;
  dbs_[current_db_]->catalog_mgr_->GetTable(table_name,table_info); //有了table名字，继而或缺table info

  pSyntaxNode temp3;
  temp3=temp2->next_;
  if(temp3->type_!=kNodeColumnList){
    return DB_FAILED;
  }
  
  pSyntaxNode unique_index_key;
  unique_index_key=temp3->child_;     //获取基于的column

  if(unique_index_key->type_!=kNodeIdentifier){
    return DB_FAILED;
  }
  while(unique_index_key!=nullptr){     //判断是否有这个属性，或者这个属性是否为unique
    uint32_t index_temp;
    dberr_t flag;
    flag=table_info->GetSchema()->GetColumnIndex(unique_index_key->val_,index_temp);
    if(flag==DB_COLUMN_NAME_NOT_EXIST){
      std::cout<<"not exist "<< unique_index_key->val_<<endl;
      return DB_FAILED;
    }
    const Column * unique_flag=table_info->GetSchema()->GetColumn(index_temp);
    if(unique_flag->GetUnique()==false){
      std::cout<<"not unique index"<<endl;
      return DB_FAILED;
    }
    unique_index_key=unique_index_key->next_;
  }
  //std::cout<<"create index testpoint 1"<<endl;
  vector<string> indexs_key;
  unique_index_key=temp3->child_;
  while(unique_index_key!=nullptr){
    indexs_key.push_back(unique_index_key->val_);
    unique_index_key=unique_index_key->next_;
  }
  IndexInfo *index_info=nullptr;
  dberr_t result;
  result=dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name,index_name,indexs_key,nullptr,index_info);
  if(result==DB_INDEX_ALREADY_EXIST){
    std::cout<<"Already have this index"<<endl;
    return result;
  }
  else if(result==DB_TABLE_NOT_EXIST){
    std::cout<<"Don't have this table"<<endl;
  }
  std::cout<<"Create index success"<<endl;

  // //更新现有的index
  // TableHeap* tableheap = table_info->GetTableHeap();
  // vector<uint32_t>index_of_column;    //从index的名字获取它们在column中的位置
  // for (auto i= indexs_key.begin(); i!= indexs_key.end() ; i++ ){
  //   uint32_t j ;
  //   table_info->GetSchema()->GetColumnIndex(*i,j);
  //   index_of_column.push_back(j);
  // }
  // //然后遍历所有的row,将index更新到b_plus_tree_index
  // for (auto it=tableheap->Begin(nullptr) ; it!= tableheap->End(); it++) {
  //   const Row &row = *it;
  //   vector<Field> index_fields; //获取所有index在这个row中对应的field
  //   for (auto i=index_of_column.begin();i!=index_of_column.end();i++){
  //     index_fields.push_back(*(row.GetField(*i)));
  //   }
  //   Row index_row(index_fields);
  //   index_info->GetIndex()->InsertEntry(index_row,row.GetRowId(),nullptr);
  // }
  TableHeap* tableheap = table_info->GetTableHeap();
  vector<uint32_t>index_column_number;
  for (auto r = indexs_key.begin(); r != indexs_key.end() ; r++ ){
    uint32_t index ;
    table_info->GetSchema()->GetColumnIndex(*r,index);
    index_column_number.push_back(index);
  }
  vector<Field>fields;
  for (auto iter=tableheap->Begin(nullptr) ; iter!= tableheap->End(); iter++) {
    const Row &it_row = *iter;
    vector<Field> index_fields;
    for (auto m=index_column_number.begin();m!=index_column_number.end();m++){
      index_fields.push_back(*(it_row.GetField(*m)));
    }
    Row index_row(index_fields);
    index_info->GetIndex()->InsertEntry(index_row,it_row.GetRowId(),nullptr);
  }
  return result;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  pSyntaxNode temp1;
  temp1=ast->child_;
  if(temp1->type_!=kNodeIdentifier){
    return DB_FAILED;
  }
  if(current_db_.empty())
  {
    cout<<"Haven't chosen database"<<std::endl;
    return DB_FAILED;
  }
  vector<TableInfo* > tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for(auto p=tables.begin();p<tables.end();p++){
    //cout<<"Indexes of Table "<<(*p)->GetTableName()<<":"<<endl;
    vector<IndexInfo*> indexes;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes((*p)->GetTableName(),indexes);
    string index_name=ast->child_->val_;
    for(auto q=indexes.begin();q<indexes.end();q++){
      if((*q)->GetIndexName()==index_name){
        dberr_t IsDrop=dbs_[current_db_]->catalog_mgr_->DropIndex((*p)->GetTableName(),index_name);
        if(IsDrop==DB_TABLE_NOT_EXIST){
          cout<<"Table Not Exist!"<<endl;
        }
        if(IsDrop==DB_INDEX_NOT_FOUND){
          cout<<"Index Not Found!"<<endl;
        }
        return IsDrop;
      }
    }
  }
  cout<<"Index Not Found!"<<endl;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  struct timeval t1,t2;
  double timeuse;
  

  if(current_db_.empty())
  {
    cout<<"Haven't chosen database"<<std::endl;
    return DB_FAILED;
  }
  pSyntaxNode temp1;  //kNodeAllColumns或者kNodeColumnList
  temp1=ast->child_;
  pSyntaxNode temp2;
  temp2=temp1->next_; //存有table name
  string tablename=temp2->val_;   //table name

  TableInfo *tableinfo;
  dberr_t get_table_info=dbs_[current_db_]->catalog_mgr_->GetTable(tablename,tableinfo);

  if(get_table_info==DB_TABLE_NOT_EXIST){   //没有这个table
    std::cout<<"No such table"<<endl;
    return DB_FAILED;
  }

  //如果是有条件的输出
  vector<uint32_t> output_columns_index;
  if(temp1->type_==kNodeColumnList){
    pSyntaxNode output_column;    //选择要输出的 column
    output_column=temp1->child_;
    while(output_column!=nullptr){    //将所有要输出的column读取他们的index
      dberr_t get_index_flag;
      uint32_t index_temp;
      get_index_flag=tableinfo->GetSchema()->GetColumnIndex(output_column->val_,index_temp);
      if(get_index_flag==DB_SUCCESS){
        output_columns_index.push_back(index_temp);
        output_column=output_column->next_;
        continue;
      }
      std::cout<<"No such column"<<endl;  //没找到要输出的这个column
      return DB_FAILED;
    }
  }
  //如果是select *情况
  else if(temp1->type_==kNodeAllColumns){
    //将所有的column保存
    for(uint32_t i=0;i<tableinfo->GetSchema()->GetColumnCount();i++){
      output_columns_index.push_back(i);
    }
  }
  //先输出第一行所有的column的名字
  for(auto i=output_columns_index.begin();i!=output_columns_index.end();i++){
    std::cout<<tableinfo->GetSchema()->GetColumn((*i))->GetName()<<"  |  ";
  }
  cout<<endl;

  pSyntaxNode temp3;  //判断是否有 where
  temp3=temp2->next_;
  if(temp3!=nullptr&&temp3->type_==kNodeConditions){  //有where 条件
    pSyntaxNode temp4;  //knodeConnector或者直接是knodeCompareOperator
    temp4=temp3->child_;
    vector <Row*> all_rows;
    //将所有row存入
    for(auto i=tableinfo->GetTableHeap()->Begin(nullptr);i!=tableinfo->GetTableHeap()->End();i++){
      Row *row_temp=new Row(*i);
      all_rows.push_back(row_temp);
    }
    gettimeofday(&t1,NULL);
    auto selet_row=selectrow(temp4,all_rows,tableinfo,dbs_[current_db_]->catalog_mgr_);
    gettimeofday(&t2,NULL);
    timeuse = (t2.tv_sec - t1.tv_sec) + (double)(t2.tv_usec - t1.tv_usec)/1000.0;
    cout<<"time = "<<timeuse<<"mx"<<endl;
    for(auto i=selet_row.begin();i!=selet_row.end();i++){
      for(uint32_t j=0;j<output_columns_index.size();j++){
        if((*i)->GetField(output_columns_index[j])->IsNull()){
          std::cout<<"NULL   ";
        }
        (*i)->GetField(output_columns_index[j])->output();
        std::cout<<"  ";
      }
      cout<<endl;
    }
    std::cout<<"Select success"<<endl;
    cout<<"affect "<<selet_row.size()<<"rows"<<endl;
    
    return DB_SUCCESS;
  }
  else if(temp3==nullptr){    //没有where限制
    vector <Row*> all_rows;
    //将所有row存入
    gettimeofday(&t1,NULL);
    for(auto i=tableinfo->GetTableHeap()->Begin(nullptr);i!=tableinfo->GetTableHeap()->End();i++){
      Row *row_temp=new Row(*i);
      all_rows.push_back(row_temp);
    }
    for(auto i=all_rows.begin();i!=all_rows.end();i++){
      for(uint32_t j=0;j<output_columns_index.size();j++){
        if((*i)->GetField(output_columns_index[j])->IsNull()){
          std::cout<<"NULL   ";
        }
        (*i)->GetField(output_columns_index[j])->output();
        std::cout<<"  ";
      }
      cout<<endl;
    }
    std::cout<<"select success"<<endl;
    std::cout<< "affect "<<all_rows.size()<<"rows"<<endl;
    gettimeofday(&t2,NULL);
    timeuse = (t2.tv_sec - t1.tv_sec) + (double)(t2.tv_usec - t1.tv_usec)/1000.0;
    cout<<"time = "<<timeuse<<"mx"<<endl;
    
    return DB_SUCCESS;
  }
  return DB_FAILED; //没有这个数据库
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  pSyntaxNode temp1;
  temp1=ast->child_;
  string tablename=temp1->val_;   //要插入的那个table的名字
  if(current_db_.empty())
  {
    cout<<"Haven't chosen database"<<std::endl;
    return DB_FAILED;
  }
  TableInfo *tableinfo; //获取table
  if(dbs_[current_db_]->catalog_mgr_->GetTable(tablename,tableinfo)==DB_TABLE_NOT_EXIST){
    std::cout<<"Not exist such table"<<endl;
    return DB_FAILED;
  }

  pSyntaxNode temp2;
  temp2=temp1->next_; //kNodeColumnValues
  pSyntaxNode temp3;
  temp3=temp2->child_;  //kNodeNumber

  int column_num=tableinfo->GetSchema()->GetColumnCount();  //table中的column的数量
  //下面将这条数据的row的所有field设置好
  vector<Field>fields_in_row;
  for(int i=0;i<column_num;i++){
    if(temp3==nullptr){   //给的数据node到头了
      for(int j=i;j<column_num;j++){  //将后续的类型保存下来
        Field field_temp(tableinfo->GetSchema()->GetColumn(j)->GetType());
        fields_in_row.push_back(field_temp);
      }
      break;
    }
    else if(temp3->type_==kNodeNull){ //空值
      Field field_temp(tableinfo->GetSchema()->GetColumn(i)->GetType());
      fields_in_row.push_back(field_temp);
    }
    else{ //有数值
      if(tableinfo->GetSchema()->GetColumn(i)->GetType()==kTypeChar){ //char*类型下
      if(temp3->type_!=kNodeString)
      {
        cout<<"Type does not match"<<endl;
        return DB_FAILED; 
      }
        string s_temp=temp3->val_;
        uint32_t length=s_temp.length();
        Field field_temp(kTypeChar,temp3->val_,length,true);
        fields_in_row.push_back(field_temp);
      }
      else if(tableinfo->GetSchema()->GetColumn(i)->GetType()==kTypeInt){ //int类型下
      if(temp3->type_!=kNodeNumber)
      {
        cout<<"Type does not match"<<endl;
        return DB_FAILED; 
      }
        Field field_temp(kTypeInt,atoi(temp3->val_));
        fields_in_row.push_back(field_temp);
      }
      else if(tableinfo->GetSchema()->GetColumn(i)->GetType()==kTypeFloat){
        if(temp3->type_!=kNodeNumber)
      {
        cout<<"Type does not match"<<endl;
        return DB_FAILED; 
      }
        Field field_temp(kTypeFloat,float(atof(temp3->val_)));
        fields_in_row.push_back(field_temp);
      }
    }
    temp3=temp3->next_; //下一个数值
  }
  if(temp3!=nullptr){
    std::cout<<"Don't match the number of column"<<endl;
    return DB_FAILED;
  }
  Row new_row(fields_in_row);
  TableHeap *table_heap;  //要插入的tableheap
  table_heap=tableinfo->GetTableHeap();
  if(table_heap->InsertTuple(new_row,nullptr)==false){  //插入到table失败
    std::cout<<"Fail to insert into table"<<endl;
    return DB_FAILED;
  }

  //插入table中成功后，插入到index
  //因为row的多个field要存入到不同的index中
  vector<IndexInfo*>indexes_temp; //table的所有index的indexinfo
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(tablename,indexes_temp);
  for(auto i=indexes_temp.begin();i!=indexes_temp.end();i++){   //所有的indexinfo
    vector<Field> fields_in_index;
    for(auto it:(*i)->GetIndexKeySchema()->GetColumns()){ //对其中的所有column
      index_id_t tmp;
      if(tableinfo->GetSchema()->GetColumnIndex(it->GetName(),tmp)==DB_SUCCESS){  //获取该colum的index
        fields_in_index.push_back(fields_in_row[tmp]);  //将row中的对应index的field存入
      }
    }
    Row new_row_i(fields_in_index);
    if((*i)->GetIndex()->InsertEntry(new_row_i,new_row.GetRowId(),nullptr)==DB_FAILED){
      cout<<"Fail to insert into index"<<endl;
      for(auto j=indexes_temp.begin();j!=i;j++){    //因为插入到index失败了，所以把它从table中删除
        (*j)->GetIndex()->RemoveEntry(new_row,new_row.GetRowId(),nullptr);
      }
      table_heap->MarkDelete(new_row.GetRowId(),nullptr);
      return DB_FAILED;
    }
  }
  std::cout<<"Insert success"<<endl;
  return DB_SUCCESS;
}

vector<Row*> ExecuteEngine::selectrow(pSyntaxNode ast, std::vector<Row*>& r, TableInfo* t, CatalogManager* c){
  if(ast == nullptr) return r;
  if(ast->type_ == kNodeConnector){
    
    vector<Row*> ans;
    if(strcmp(ast->val_,"and") == 0){
      auto r1 = selectrow(ast->child_,r,t,c);
      ans = selectrow(ast->child_->next_,r1,t,c);
      return ans;
    }
    else if(strcmp(ast->val_,"or") == 0){
      auto r1 = selectrow(ast->child_,r,t,c);
      auto r2 = selectrow(ast->child_->next_,r,t,c);
      for(uint32_t i=0;i<r1.size();i++){
        ans.push_back(r1[i]);        
      }
      for(uint32_t i=0;i<r2.size();i++){
        int flag=1;//û���ظ�
        for(uint32_t j=0;j<r1.size();j++){
          int f=1;
          for(uint32_t k=0;k<r1[i]->GetFieldCount();k++){
            if(!r1[i]->GetField(k)->CompareEquals(*r2[j]->GetField(k))){
              f=0;break;
            }
          }
          if(f==1){
            flag=0;//���ظ�
            break;}
        }
        if(flag==1) ans.push_back(r2[i]);        
      } 
      return ans;
    }
  }
  if(ast->type_ == kNodeCompareOperator){
    string op = ast->val_;//operation type
    string col_name = ast->child_->val_;//column name
    string val = ast->child_->next_->val_;//compare value
    uint32_t keymap;
    vector<Row*> ans;
    if(t->GetSchema()->GetColumnIndex(col_name, keymap)!=DB_SUCCESS){
      cout<<"column not found"<<endl;
      return ans;
    }
    const Column* key_col = t->GetSchema()->GetColumn(keymap);
    TypeId type =  key_col->GetType();

    if(op == "="){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        vector<Field> vect_benchmk;
        vect_benchmk.push_back(benchmk);

        vector <IndexInfo*> indexes;
        c->GetTableIndexes(t->GetTableName(),indexes);
        for(auto p=indexes.begin();p<indexes.end();p++){
            if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
              if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
                // cout<<"--select using index--"<<endl;
                Row tmp_row(vect_benchmk);
                vector<RowId> result;
                (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
                for(auto q:result){
                  if(q.GetPageId()<0) continue;
                  Row *tr = new Row(q);
                  t->GetTableHeap()->GetTuple(tr,nullptr);
                  ans.push_back(tr);
                }
                return ans;
              }
            }
        }   
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }

      else if(type==kTypeChar){
        char *ch = new char[val.size()];
        strcpy(ch,val.c_str());//input compare object
        // cout<<"ch "<<sizeof(ch)<<endl;
        Field benchmk = Field(TypeId::kTypeChar, const_cast<char *>(ch), val.size(), true);
        // Field benchmk(kTypeChar,ch,key_col->GetLength(),true);
        vector<Field> vect_benchmk;
        vect_benchmk.push_back(benchmk);

        vector <IndexInfo*> indexes;
        c->GetTableIndexes(t->GetTableName(),indexes);
        for(auto p=indexes.begin();p<indexes.end();p++){
            if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
              if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
                
                cout<<"--select using index--"<<endl;
                Row tmp_row(vect_benchmk);
                vector<RowId> result;
                (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
                for(auto q:result){
                  if(q.GetPageId()<0) continue;
                  // cout<<"index found"<<endl;
                  Row *tr = new Row(q);
                  t->GetTableHeap()->GetTuple(tr,nullptr);
                  ans.push_back(tr);
                }
                return ans;
              }
            }
        }
        for(uint32_t i=0;i<r.size();i++){
          bool equal=true;
          const char* test = r[i]->GetField(keymap)->GetData();
          uint32_t l_num=strlen(ch);
          for(uint32_t q = 0;q<l_num;q++){
            //std::cout<<"contex[q]:"<<int(context[q])<<" ch[q]:"<<int(ch[q])<<endl;
            if(test[q]!=ch[q]) equal=false;
          }
          if(strlen(test)!=strlen(ch)){
            if(strlen(ch)==5&&strlen(test)==6&&(test[5]<'0'||test[5]>'9')){
              equal=equal;
            }
            else
              equal=false;
          }
          if(equal){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<"){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()+2];
        strcpy(ch,val.c_str());
        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          if(strcmp(test,ch)<0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == ">"){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//�Ƚ�Ŀ�����ch��

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          
          if(strcmp(test,ch)>0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<="){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//�Ƚ�Ŀ�����ch��

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          
          if(strcmp(test,ch)<=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == ">="){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//�Ƚ�Ŀ�����ch��

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
        
          if(strcmp(test,ch)>=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<>"){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareNotEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareNotEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//�Ƚ�Ŀ�����ch��

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          
          if(strcmp(test,ch)!=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    return ans;
  }
  return r; 
}


dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  pSyntaxNode temp1=ast->child_;
  if(current_db_.empty())
  {
    cout<<"Haven't chosen database"<<std::endl;
    return DB_FAILED;
  }
  if(temp1->type_!=kNodeIdentifier)
  return DB_FAILED;
  TableInfo *tableinfo=nullptr;
  std::unordered_map<std::string, DBStorageEngine *>::iterator it;
  std::string tablename=temp1->val_;
  it=dbs_.find(current_db_);
  dberr_t gettable=it->second->catalog_mgr_->GetTable(tablename,tableinfo);
  if(gettable==DB_TABLE_NOT_EXIST){
  std::cout<<"The table not exists!"<<std::endl;
  return DB_FAILED;
  }
  pSyntaxNode temp2=temp1->next_;
  std::vector<Row*> rows;
  std::vector<Row*> selectrows;
  if(temp2==NULL)
  {
    for(auto iter = tableinfo->GetTableHeap()->Begin(nullptr);iter!=tableinfo->GetTableHeap()->End();iter++){
      Row* newrow=new Row(*iter);
      rows.push_back(newrow);
  }
  for(auto iter :rows)
  {
      tableinfo->GetTableHeap()->ApplyDelete(iter->GetRowId(),nullptr);
  }
  cout<<"Delete success"<<endl;
  return DB_SUCCESS;
  }
  if(temp2->type_!=kNodeConditions)
  return DB_FAILED;
  pSyntaxNode temp3=temp2->child_;
  std::vector<Row*> allrows;
  for(auto iter = tableinfo->GetTableHeap()->Begin(nullptr);iter!=tableinfo->GetTableHeap()->End();iter++){
      Row* newrow=new Row(*iter);
      allrows.push_back(newrow);
  }
  selectrows=selectrow(temp3,allrows,tableinfo,it->second->catalog_mgr_);
  for(auto iter :selectrows)
  {
      tableinfo->GetTableHeap()->ApplyDelete(iter->GetRowId(),nullptr);
  }
  cout<<"Delete success"<<endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  if(current_db_.empty())
  {
    cout<<"Haven't chosen database"<<std::endl;
    return DB_FAILED;
  }
  pSyntaxNode t1=ast->child_;
  std::string tablename=t1->val_;
  pSyntaxNode t2=t1->next_;
  TableInfo *tableinfo=nullptr;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(tablename,tableinfo)==DB_TABLE_NOT_EXIST)
  {
    cout<<"Table does not exist!"<<std::endl;
    return DB_FAILED;
  }
  pSyntaxNode update1=t2->child_;
  vector<Row*> allrows;
  if(t2->next_!=NULL)
  {
    if(t2->next_->type_!=kNodeConditions)
    return DB_FAILED;
    else{
      vector<Row*> selectrows;
      for(auto iter = tableinfo->GetTableHeap()->Begin(nullptr);iter!=tableinfo->GetTableHeap()->End();iter++){
        Row* newrow=new Row(*iter);
        selectrows.push_back(newrow);
      }
      allrows=selectrow(t2->next_->child_,selectrows,tableinfo,dbs_[current_db_]->catalog_mgr_);
    }
  }
  else if(t2->next_==  NULL)
  {
    for(auto iter = tableinfo->GetTableHeap()->Begin(nullptr);iter!=tableinfo->GetTableHeap()->End();iter++){
      Row* newrow=new Row(*iter);
      allrows.push_back(newrow);
    }
  }
  uint32_t index;
  while(update1!=NULL&&update1->type_==kNodeUpdateValue){
    std::string columnname=update1->child_->val_;
    std::string value=update1->child_->next_->val_;
    if(tableinfo->GetSchema()->GetColumnIndex(columnname,index)!=DB_SUCCESS)
    {
      cout<<"column"<<columnname<<"not found"<<std::endl;
    }
    TypeId type=tableinfo->GetSchema()->GetColumn(index)->GetType();
    if(type==kTypeInt)
    {
      int val=atoi(value.c_str());
        Field *newfield= new Field(type,val);
        for(auto it:allrows)
        {
          it->GetFields()[index]=newfield;
        }
      }
      else if(type==kTypeChar)
      {
      uint32_t length = tableinfo->GetSchema()->GetColumn(index)->GetLength();
      char* ch = new char[length];
      strcpy(ch,value.c_str());
      Field* field1 = new Field(kTypeChar,ch,length,true);
      for(auto it:allrows){
        it->GetFields()[index] = field1;
      }
      
      }
      else if(type==kTypeFloat)
      {
        float val=atof(value.c_str());
        Field *newfield= new Field(type,val);
        for(auto it:allrows)
        {
          it->GetFields()[index]=newfield;
        }
      }
      update1=update1->next_;
      }
    for(auto it:allrows){
    tableinfo->GetTableHeap()->UpdateTuple(*it,it->GetRowId(),nullptr);
    }
    cout<<"Update Success"<<endl;
    cout<<"Affects "<<allrows.size()<<" Record!"<<endl;
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  pSyntaxNode temp1;
  temp1=ast->child_;
  string filename_temp;
  filename_temp=temp1->val_;
  filename_temp="/mnt/e/myminisql/minisql/testfile/"+filename_temp;   //获取文件名
  std::cout<<"Execute the file: "<<filename_temp<<endl;

  ifstream fp;
  fp.open(filename_temp.data());  //打开这个文件
  if (fp.is_open()){      //打开成功
    const int buf_size = 1024;
    char cmd_from_file[buf_size];   //用来每一行的命令
    while(fp.getline(cmd_from_file,buf_size)){    //读取每一行的命令
      YY_BUFFER_STATE bp = yy_scan_string(cmd_from_file);     //创建buffer
      if (bp == nullptr) {
        LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
        exit(1);
      }
      yy_switch_to_buffer(bp);        //参考main.cpp给的过程,不确定对不对
      MinisqlParserInit();
      yyparse();

      ExecuteContext context_temp;     //这里不确定是新创建一个context，还是直接使用函数的参数。我觉得应该是新的
      Execute(MinisqlGetParserRootNode(), &context_temp);
    }
    return DB_SUCCESS;
  }
  else{
    cout<<"open file error"<<endl;
    return DB_FAILED;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
