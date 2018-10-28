
#include "rm.h"
#include "../rbf/rbfm.h"

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{
  //initiate attribute vectors for table and column
    Attribute tableID;
    tableID.name = "table-id";
    tableID.type = TypeInt;
    tableID.length = 4;
    Attribute tableName;
    tableName.name = "table_name";
    tableName.type = TypeVarChar;
    tableName.length = 50;
    Attribute fileName;
    fileName.name = "file_name";
    fileName.type = TypeVarChar;
    fileName.length = 50;
    tableAttr.push_back(tableID);
    tableAttr.push_back(tableName);
    tableAttr.push_back(fileName);
    Attribute columnName = tableName;
    columnName.name = "column_name";
    Attribute columnType;
    columnType.name = "column_type";
    columnType.type = TypeInt;
    columnType.length = 4;
    Attribute columnLength = columnType;
    columnLength.name = "column_length";
    Attribute columnPosition = columnType;
    columnPosition.name = "column_position";
    columnAttr.push_back(tableID);
    columnAttr.push_back(columnName);
    columnAttr.push_back(columnType);
    columnAttr.push_back(columnLength);
    columnAttr.push_back(columnPosition);
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    cout<<"createCatalog"<<endl;
    RC rc1 = rbfm.createFile(tableFileName);
    RC rc2 = rbfm.createFile(columnFileName);
    if(rc1==SUCCESS && rc2==SUCCESS){
        void* data = malloc(1000);
        memset(data, 0, 1000);
        int length = 0;
        RID rid;
        //cout<<"fuck1"<<endl;
        RC rct = generateTableRecord(1, "Tables", tableFileName, data, length);
        if(rct != SUCCESS) return -1;
        //rbfm.printRecord(tableAttr, data);
        insertTupleHelper(tableFileName, tableAttr, data, rid);
        length = 0;memset(data, 0, 1000);
        rct = generateTableRecord(2, "Columns", columnFileName, data, length);
        //cout<<"fuck2"<<endl;
        if(rct != SUCCESS) return -1;
        insertTupleHelper(tableFileName, tableAttr, data, rid);
        length = 0;memset(data, 0, 1000);
        //cout<<"fuck3"<<endl;
        RC rcc = generateColumnRecord(1, "table_id", TypeInt, 4, 1, data, length);
        if(rcc != SUCCESS) return -1;
        //cout<<"hyx"<<endl;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 1000);
        //cout<<"fuck4"<<endl;
        rcc = generateColumnRecord(1, "table_name", TypeVarChar, 50, 2, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 1000);
        rcc = generateColumnRecord(1, "file_name", TypeVarChar, 50, 3, data, length);
        if(rcc != SUCCESS) return -1;
        //cout<<"fuck5"<<endl;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 1000);
        rcc = generateColumnRecord(2, "table_id", TypeInt, 4, 1, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 1000);
        rcc = generateColumnRecord(2, "column_name", TypeVarChar, 50, 2, data, length);
        if(rcc != SUCCESS) return -1;
        //cout<<"fuck6"<<endl;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 1000);
        rcc = generateColumnRecord(2, "column_type", TypeInt, 4, 3, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 1000);
        rcc = generateColumnRecord(2, "column_length", TypeInt, 4, 4, data, length);
        if(rcc != SUCCESS) return -1;
        //cout<<"fuck7"<<endl;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 1000);
        rcc = generateColumnRecord(2, "column_position", TypeInt, 4, 5, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        free(data);
        return SUCCESS;
    } else{
        return -1;
    }
}

RC RelationManager::deleteCatalog()
{
    RC rc1 = rbfm.destroyFile(tableFileName);
    RC rc2 = rbfm.destroyFile(columnFileName);
    if(rc1==SUCCESS && rc2==SUCCESS){
        return SUCCESS;
    } else{
        return -1;
    }
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    rbfm.createFile(tableName);
    //scan to get next tableId
    unordered_set<int> usedIds;
    //cout<<"fuck"<<endl;
    rbfm.openFile(tableFileName, fileHandle);
    RBFM_ScanIterator rbfmIter;// = RBFM_ScanIterator();
    //cout<<"2333"<<endl;
    //rbfm.openFile(tableFileName, fileHandle);
    vector<string> targetAttr;
    targetAttr.push_back("table_id");
    //cout<<"fuck0"<<endl;
    RC rc = rbfm.scan(fileHandle, tableAttr, "", NO_OP, NULL, targetAttr, rbfmIter);
    RID rid;
    cout<<"fuck1"<<endl;
    void* data = malloc(1000);
    memset(data, 0, 1000);
    while(!rbfmIter.getNextRecord(rid, data)){
        int id;
        memcpy(&id, (char*)data+1, sizeof(int));
        //cout<<"userId: "<<id<<endl;
        usedIds.insert(id);
        memset(data, 0, 1000);
    }
    cout<<"fuck2"<<endl;
    int tableId;
    for(int i=1;i<INT_MAX;i++){
        if(usedIds.find(i) == usedIds.end()){
            tableId = i;
            break;
        }
    }
    rbfm.closeFile(fileHandle);
    cout<<"fuck3"<<endl;
    int length = 0;
    rc = generateTableRecord(tableId, tableName, tableName, data, length);
    if(rc != SUCCESS) return -1;
    insertTupleHelper(tableFileName, tableAttr, data, rid);
    length = 0;
    for(int i=0;i<attrs.size();i++){
        Attribute attr = attrs[i];
        length = 0;
        rc = generateColumnRecord(tableId, attrs[i].name, attr.type, attr.length, i+1, data, length);
        if(rc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
    }
    free(data);
    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    //delete file
    rbfm.destroyFile(tableName);
    //delete items in table
    RBFM_ScanIterator rbfmIter;
    rbfm.openFile(tableFileName, fileHandle);
    vector<string> tableAttrStr;
    for(int i=0;i<tableAttr.size();i++)
        tableAttrStr.push_back(tableAttr[i].name);
    RC rc = rbfm.scan(fileHandle, tableAttr, "", NO_OP, NULL, tableAttrStr, rbfmIter);
    if(rc != SUCCESS) return -1;
    RID rid;
    void* data;
    int table_id = -1;
    while(rbfmIter.getNextRecord(rid, data)){
        rbfm.readAttribute(fileHandle, tableAttr, rid, tableName, data);
        char* d = (char*)data;
        int length;
        memcpy(&length, d+1, sizeof(int));
        bool isTableSame = true;
        if(tableName.size() == length){
            for(int i=0;i<length;i++){
                if(*(d+i) != tableName[i]){
                    isTableSame = false;
                    break;
                }
            }
        } else{
            isTableSame = false;
        }
        if(isTableSame){
            free(data);
            rbfm.readAttribute(fileHandle, tableAttr, rid, "table_id", data);
            d = (char*)data;
            memcpy(&table_id, d+1, sizeof(int));
            free(data);
            rbfm.deleteRecord(fileHandle, tableAttr, rid);
            break;
        }
        free(data);
    }
    rbfm.closeFile(fileHandle);
    rbfm.openFile(columnFileName, fileHandle);
    vector<string> attrs;
    attrs.push_back("table_id");
    rc = rbfm.scan(fileHandle, columnAttr, "", NO_OP, NULL, attrs, rbfmIter);
    if(rc != SUCCESS) return -1;
    while(rbfmIter.getNextRecord(rid, data)){
        int id;
        char* d = (char*)data;
        memcpy(&id, d+1, sizeof(int));
        if(id == table_id){
            rbfm.deleteRecord(fileHandle, columnAttr, rid);
            free(data);
        }
    }
    rbfm.closeFile(fileHandle);
    return SUCCESS;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RBFM_ScanIterator rbfmIter;
    vector<string> tableAttrStr;
    for(int i=0;i<tableAttr.size();i++){
        tableAttrStr.push_back(tableAttr[i].name);
    }
    RC rc = rbfm.scan(fileHandle, tableAttr, "", NO_OP, NULL, tableAttrStr, rbfmIter);
    if(rc != SUCCESS) return -1;
    void* data;
    RID rid;
    int pos = 1;
    int tableId = -1;
    while(rbfmIter.getNextRecord(rid, data)){
        char* d = (char*)data;
        int id;
        memcpy(&id, d+pos, sizeof(int));
        pos += 4;
        int length;
        memcpy(&length, d+pos, sizeof(int));
        pos += 4;
        string name;
        for(int i=0;i<length;i++){
            name.push_back(*(d+pos));
            pos++;
        }
        if(name == tableName){
            tableId = id;
            break;
        }
        free(data);
    }
    if(tableId <= 0){
        return -1;
    }
    rbfm.closeFile(fileHandle);
    rbfm.openFile(columnFileName, fileHandle);
    vector<string> columnAttrStr;
    for(int i=0;i<columnAttr.size();i++){
        columnAttrStr.push_back(columnAttr[i].name);
    }
    rc = rbfm.scan(fileHandle, columnAttr, "", NO_OP, NULL, columnAttrStr, rbfmIter);
    if(rc!=SUCCESS) return -1;
    map<int, Attribute> attrMap;
    while(rbfmIter.getNextRecord(rid, data)){
        pos = 1;
        char* d = (char*)data;
        int id;
        memcpy(&id, d+pos, sizeof(int));
        pos+=4;
        if(id == tableId){
            int length;
            memcpy(&length, d+pos, sizeof(int));
            pos += 4;
            string colName;
            for(int i=0;i<length;i++){
                colName.push_back(*(d+pos));
                pos++;
            }
            int type;
            memcpy(&type, d+pos, sizeof(int));
            pos += 4;
            int l;
            memcpy(&l, d+pos, sizeof(int));
            pos += 4;
            int position;
            memcpy(&position, d+pos, sizeof(int));
            pos += 4;
            Attribute attr;
            attr.name = colName;
            AttrType at = AttrType::TypeVarChar;
            if(type==0){
                at = AttrType::TypeInt;
            } else if(type==1){
                at = AttrType::TypeReal;
            }
            attr.type = at;
            attr.length = l;
            attrMap[position] = attr;
        }
        free(data);
    }
    for(auto it=attrMap.begin();it!=attrMap.end();it++){
        attrs.push_back(it->second);
    }
    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;
    rc = rbfm.openFile(tableName, fileHandle);
    if(rc != SUCCESS) return rc;
    rc = rbfm.insertRecord(fileHandle, attrs, data, rid);
    if(rc != SUCCESS) return rc;
    //free(data);
    rc = rbfm.closeFile(fileHandle);
    if(rc != SUCCESS) return rc;
    return SUCCESS;
}

RC RelationManager::insertTupleHelper(const string &tableName, vector<Attribute>& attributes, const void* data, RID& rid){
    RC rc = rbfm.openFile(tableName, fileHandle);
    if(rc != SUCCESS) return -1;
    //cout<<"f1"<<endl;
    //cout<<tableName<<endl;
    //rbfm.printRecord(attributes, data);
    rc = rbfm.insertRecord(fileHandle, attributes, data, rid);
    //cout<<"f2"<<endl;
    //free(data);
    if(rc != SUCCESS) return -1;
    rc = rbfm.closeFile(fileHandle);
    if(rc != SUCCESS) return -1;
    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;
    rc = rbfm.openFile(tableName, fileHandle);
    if(rc != SUCCESS) return rc;
    rc = rbfm.deleteRecord(fileHandle, attrs, rid);
    if(rc != SUCCESS) return rc;
    rc = rbfm.closeFile(fileHandle);
    return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;
    rc = rbfm.openFile(tableName, fileHandle);
    if(rc != SUCCESS) return rc;
    rc = rbfm.updateRecord(fileHandle, attrs, data, rid);
    if(rc != SUCCESS) return rc;
    rc = rbfm.closeFile(fileHandle);
    return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;
    rc = rbfm.openFile(tableName, fileHandle);
    if(rc != SUCCESS) return rc;
    rc = rbfm.readRecord(fileHandle, attrs, rid, data);
    if(rc != SUCCESS) return rc;
    rc = rbfm.closeFile(fileHandle);
    return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RC rc = rbfm.printRecord(attrs, data);
    return rc;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RC rc = rbfm.openFile(tableName, fileHandle);
    if(rc != SUCCESS) return rc;
    vector<Attribute> attrs;
    rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;
    rc = rbfm.readAttribute(fileHandle, attrs, rid, attributeName, data);
    if(rc != SUCCESS) return rc;
    rc = rbfm.closeFile(fileHandle);
    return rc;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}


RC RelationManager::generateTableRecord(int tableId, string tableName, string fileName, void* data, int& length){
    if(tableName.size()>50 || fileName.size()>50){
        return -1;
    } else{
        int dataSize = 1+4+4+tableName.size()+4+fileName.size();
        char nullInd = 0;//11100000
        memcpy((char*)data+length, &nullInd, sizeof(char));
        length += 1;
        memcpy((char*)data+length, &tableId, sizeof(int));
        length += 4;
        int tableNameLength = tableName.size();
        memcpy((char*)data+length, &tableNameLength, sizeof(int));
        length += 4;
        memcpy((char*)data+length, tableName.data(), tableName.size());
        length += tableName.size();
        int fileLength = fileName.size();
        memcpy((char*)data+length, &fileLength, sizeof(int));
        length += 4;
        memcpy((char*)data+length, fileName.data(), fileName.size());
        length += fileName.size();
        cout<<"generateTableRecord"<<endl;
        //cout<<tableName<<endl;
        //rbfm.printRecord(tableAttr, data);
        return SUCCESS;
    }
}

RC RelationManager::generateColumnRecord(int tableId, string columnName, int columnType, int columnLength, int columnPos, void* data, int& length){
    if(columnName.size()>50){
        return -1;
    } else{
        int dataSize = 1+4+4+columnName.size()+4+4+4;
        char nullInd = 0;
        memcpy((char*)data+length, &nullInd, sizeof(char));
        length += 1;
        memcpy((char*)data+length, &tableId, sizeof(int));
        length += 4;
        int columnLength = columnName.size();
        memcpy((char*)data+length, &columnLength, sizeof(int));
        length += 4;
        memcpy((char*)data+length, columnName.data(), columnName.size());
        length += columnName.size();
        memcpy((char*)data+length, &columnType, sizeof(int));
        length += 4;
        memcpy((char*)data+length, &columnLength, sizeof(int));
        length += 4;
        memcpy((char*)data+length, &columnPos, sizeof(int));
        length += 4;
        return SUCCESS;
    }
}