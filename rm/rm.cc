
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
    rbfm = RecordBasedFileManager::instance();
    Attribute tableID;
    tableID.name = "table-id";
    tableID.type = TypeInt;
    tableID.length = 4;
    Attribute tableName;
    tableName.name = "table-name";
    tableName.type = TypeVarChar;
    tableName.length = 50;
    Attribute fileName;
    fileName.name = "file-name";
    fileName.type = TypeVarChar;
    fileName.length = 50;
    tableAttr.push_back(tableID);
    tableAttr.push_back(tableName);
    tableAttr.push_back(fileName);
    Attribute columnName = tableName;
    columnName.name = "column-name";
    Attribute columnType;
    columnType.name = "column-type";
    columnType.type = TypeInt;
    columnType.length = 4;
    Attribute columnLength = columnType;
    columnLength.name = "column-length";
    Attribute columnPosition = columnType;
    columnPosition.name = "column-position";
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
    //cout<<"createCatalog"<<endl;
    RC rc1 = rbfm->createFile(tableFileName);
    RC rc2 = rbfm->createFile(columnFileName);
    if(rc1==SUCCESS && rc2==SUCCESS){
        void* data = malloc(4000);
        memset(data, 0, 4000);
        int length = 0;
        RID rid;
        //cout<<"fuck1"<<endl;
        RC rct = generateTableRecord(1, "Tables", tableFileName, data, length);
        if(rct != SUCCESS) return -1;
        //rbfm->printRecord(tableAttr, data);
        insertTupleHelper(tableFileName, tableAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rct = generateTableRecord(2, "Columns", columnFileName, data, length);
        //cout<<"fuck2"<<endl;
        if(rct != SUCCESS) return -1;
        insertTupleHelper(tableFileName, tableAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        //cout<<"fuck3"<<endl;
        RC rcc = generateColumnRecord(1, "table-id", TypeInt, 4, 1, data, length);
        if(rcc != SUCCESS) return -1;
        //cout<<"hyx"<<endl;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        //cout<<"fuck4"<<endl;
        rcc = generateColumnRecord(1, "table-name", TypeVarChar, 50, 2, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(1, "file-name", TypeVarChar, 50, 3, data, length);
        if(rcc != SUCCESS) return -1;
        //cout<<"fuck5"<<endl;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(2, "table-id", TypeInt, 4, 1, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(2, "column-name", TypeVarChar, 50, 2, data, length);
        if(rcc != SUCCESS) return -1;
        //cout<<"fuck6"<<endl;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(2, "column-type", TypeInt, 4, 3, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(2, "column-length", TypeInt, 4, 4, data, length);
        if(rcc != SUCCESS) return -1;
        //cout<<"fuck7"<<endl;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(2, "column-position", TypeInt, 4, 5, data, length);
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
    RC rc1 = rbfm->destroyFile(tableFileName);
    RC rc2 = rbfm->destroyFile(columnFileName);
    if(rc1==SUCCESS && rc2==SUCCESS){
        return SUCCESS;
    } else{
        return -1;
    }
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    rbfm->createFile(tableName);
    //scan to get next tableId
    unordered_set<int> usedIds;
    //cout<<"fuck"<<endl;
    FileHandle fileHandle;
    rbfm->openFile(tableFileName, fileHandle);
    RBFM_ScanIterator rbfmIter;// = RBFM_ScanIterator();
    //cout<<"2333"<<endl;
    vector<string> targetAttr;
    for(int i=0;i<tableAttr.size();i++){
        targetAttr.push_back(tableAttr[i].name);
    }
    //cout<<"fuck0"<<endl;
    RC rc = rbfm->scan(fileHandle, tableAttr, "", NO_OP, NULL, targetAttr, rbfmIter);
    RID rid;
    //cout<<"fuck1"<<endl;
    void* data = malloc(4000);
    memset(data, 0, 4000);
    while(!rbfmIter.getNextRecord(rid, data)){
        //rbfm->printRecord(tableAttr, data);
        int id;
        memcpy(&id, (char*)data+1, sizeof(int));
        //cout<<"userId: "<<id<<endl;
        usedIds.insert(id);
        memset(data, 0, 4000);
    }
    //cout<<"fuck2"<<endl;
    //cout<<"userId size: "<<usedIds.size()<<endl;
    int tableId;
    for(int i=1;i<INT_MAX;i++){
        if(usedIds.find(i) == usedIds.end()){
            tableId = i;
            break;
        }
    }
    rbfm->closeFile(fileHandle);
    //cout<<"fuck3"<<endl;
    int length = 0;
    rc = generateTableRecord(tableId, tableName, tableName, data, length);
    if(rc != SUCCESS) return -1;
    insertTupleHelper(tableFileName, tableAttr, data, rid);
    length = 0;
    //cout<<tableName<<": "<<attrs.size()<<endl;
    for(int i=0;i<attrs.size();i++){
        Attribute attr = attrs[i];
       // cout<<attr.length<<endl;
        length = 0;
        memset(data,0,4000);
        rc = generateColumnRecord(tableId, attr.name, attr.type, (int)attr.length, i+1, data, length);
        if(rc != SUCCESS) return -1;
        //rbfm->printRecord(columnAttr, data);
        insertTupleHelper(columnFileName, columnAttr, data, rid);
    }
    rbfmIter.close();
    free(data);
    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    if(tableName==tableFileName || tableName==columnFileName){
        return -1;
    }
    //delete file
    rbfm->destroyFile(tableName);
    //delete items in table
    RBFM_ScanIterator rbfmIter;
    FileHandle fileHandle;
    rbfm->openFile(tableFileName, fileHandle);
    vector<string> tableAttrStr;
    for(int i=0;i<tableAttr.size();i++)
        tableAttrStr.push_back(tableAttr[i].name);
    RC rc = rbfm->scan(fileHandle, tableAttr, "", NO_OP, NULL, tableAttrStr, rbfmIter);
    if(rc != SUCCESS) return -1;
    RID rid;
    void* data = malloc(4000);
    int table_id = -1;
    //cout<<"dt1"<<endl;
    while(rbfmIter.getNextRecord(rid, data) != RM_EOF){
        int length;
        int pos = 1;
        memcpy(&table_id, (char*)data+pos, sizeof(int));
        pos += 4;
        memcpy(&length, (char*)data+pos, sizeof(int));
        pos += 4;
        //cout<<"length: "<<length<<endl;
        string targetTableName;
        targetTableName.resize(length);
        memcpy((char*)targetTableName.data(),data+pos,length);
        pos += length;
//cout<<"targetTableName: "<<targetTableName<<endl;
        bool isTableSame = (targetTableName==tableName);
        if(isTableSame){
            //cout<<"table is same"<<endl;
            rc = rbfm->deleteRecord(fileHandle, tableAttr, rid);
            if(rc != SUCCESS){
                return -1;
            }
            break;
        }
        memset(data, 0, 4000);
    }
    if(table_id<0){
        return table_id;
    }
    //cout<<"del2"<<endl;
    cout<<"victim table id: "<<table_id<<endl;
    rbfm->closeFile(fileHandle);
    rbfm->openFile(columnFileName, fileHandle);
    rbfmIter.close();
    vector<string> attrs;
    for(int i=0;i<columnAttr.size();i++)
        attrs.push_back(columnAttr[i].name);
    rc = rbfm->scan(fileHandle, columnAttr, "", NO_OP, NULL, attrs, rbfmIter);
    if(rc != SUCCESS) return -1;
    //cout<<"del3"<<endl;
    memset(data,0,4000);
    //cout<<"dc1"<<endl;
    while(!rbfmIter.getNextRecord(rid, data)){
        //cout<<"dc2"<<endl;
        int id;
        memcpy(&id, (char*)data+1, sizeof(int));
        //cout<<"id: "<<id<<endl;
        if(id == table_id){
            cout<<"delete table id: "<<id<<endl;
            rbfm->deleteRecord(fileHandle, columnAttr, rid);
        }
        memset(data,0,4000);
    }
    //cout<<"finish delete"<<endl;
    free(data);
    rbfm->closeFile(fileHandle);
    rbfmIter.close();
    return SUCCESS;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    FileHandle fileHandle;
    RBFM_ScanIterator rbfmIter;
    vector<string> tableAttrStr;
    for(int i=0;i<tableAttr.size();i++){
        tableAttrStr.push_back(tableAttr[i].name);
    }
    rbfm->openFile(tableFileName, fileHandle);
    RC rc = rbfm->scan(fileHandle, tableAttr, "", NO_OP, NULL, tableAttrStr, rbfmIter);
    //cout<<"getA1"<<endl;
    if(rc != SUCCESS) {cout<<"fail"<<endl;return -1;}
    //cout<<"getA2"<<endl;
    void* data = malloc(4000);
    memset(data,0,4000);
    RID rid;
    int tableId = -1;
    cout<<"getAttr1"<<endl;
    while(rbfmIter.getNextRecord(rid, data) != RM_EOF){
        cout<<"getAttr nextrecord"<<endl;
        int pos = 1;
        int id;
        memcpy(&id, (char*)data+pos, sizeof(int));
        cout<<"table_id: "<<id<<endl;
        pos += 4;
        int length;
        memcpy(&length, (char*)data+pos, sizeof(int));
        pos += 4;
        string name;
        name.resize(length);
        memcpy((char*)name.data(),data+pos,length);
        pos += length;
        cout<<"tableName: "<<name<<", "<<tableName<<endl;
        if(name == tableName){
            tableId = id;
            break;
        }
        memset(data,0,4000);
    }
    cout<<"tableId: "<<tableId<<endl;
    if(tableId <= 0){
        free(data);
        return -1;
    }
    rbfm->closeFile(fileHandle);
    rbfm->openFile(columnFileName, fileHandle);
    vector<string> columnAttrStr;
    for(int i=0;i<columnAttr.size();i++){
        columnAttrStr.push_back(columnAttr[i].name);
    }
    //cout<<"getA3"<<endl;
    rbfmIter.close();
    rc = rbfm->scan(fileHandle, columnAttr, "", NO_OP, NULL, columnAttrStr, rbfmIter);
    if(rc!=SUCCESS) {cout<<"fail"<<endl;return -1;}
    //cout<<"getA4"<<endl;
    map<int, Attribute> attrMap;
    while(!rbfmIter.getNextRecord(rid, data)){
        int pos = 1;
        //char* d = (char*)data;
        int id;
        memcpy(&id, (char*)data+pos, sizeof(int));
        pos+=4;
       // rbfm->printRecord(columnAttr, data);
        if(id == tableId){
            int length;
            memcpy(&length, (char*)data+pos, sizeof(int));
            pos += 4;
            string colName;
            for(int i=0;i<length;i++){
                colName.push_back(*((char*)data+pos));
                pos++;
            }
            int type;
            memcpy(&type, (char*)data+pos, sizeof(int));
            pos += 4;
            int l;
            memcpy(&l, (char*)data+pos, sizeof(int));
            pos += 4;
            int position;
            memcpy(&position, (char*)data+pos, sizeof(int));
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
    }
    free(data);
    rbfm->closeFile(fileHandle);
    rbfmIter.close();
    for(auto it=attrMap.begin();it!=attrMap.end();it++){
        attrs.push_back(it->second);
    }
    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    vector<Attribute> attrs;
    //cout<<"t1"<<endl;
    RC rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;
    //cout<<"t2"<<endl;
    //cout<<"tableName: "<<tableName<<endl;
    //cout<<"t3"<<endl;
    //for(int i=0;i<attrs.size();i++){
    //    cout<<attrs[i].name<<": "<<attrs[i].type<<", "<<attrs[i].length<<endl;
    //}
    //rbfm->printRecord(attrs, data);
    FileHandle fileHandle;
    rc = rbfm->openFile(tableName, fileHandle);
    //cout<<"i1"<<endl;
    if(rc != SUCCESS) return rc;
    //cout<<"i2"<<endl;
    rc = rbfm->insertRecord(fileHandle, attrs, data, rid);
    //cout<<rc<<endl;
    if(rc != SUCCESS) return rc;
    //cout<<"t4"<<endl;
    //free(data);
    rc = rbfm->closeFile(fileHandle);
    if(rc != SUCCESS) return rc;
    return SUCCESS;
}

RC RelationManager::insertTupleHelper(const string &tableName, vector<Attribute>& attributes, const void* data, RID& rid){
    FileHandle fileHandle;
    //cout<<"f"<<endl;
    RC rc = rbfm->openFile(tableName, fileHandle);
    if(rc != SUCCESS) return -1;
    //cout<<"f1"<<endl;
    //cout<<tableName<<endl;
    //rbfm->printRecord(attributes, data);
    rc = rbfm->insertRecord(fileHandle, attributes, data, rid);
    //cout<<"f2"<<endl;
    if(rc != SUCCESS) return -1;
    rc = rbfm->closeFile(fileHandle);
    if(rc != SUCCESS) return -1;
    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    FileHandle fileHandle;
    vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;
    cout<<"getAttribute successful"<<endl;
    rc = rbfm->openFile(tableName, fileHandle);
    if(rc != SUCCESS) return rc;
    cout<<"openfile successful"<<endl;
    rc = rbfm->deleteRecord(fileHandle, attrs, rid);
    if(rc != SUCCESS) return rc;
    cout<<"delete successful"<<endl;
    rc = rbfm->closeFile(fileHandle);
    return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    FileHandle fileHandle;
    vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;
    rc = rbfm->openFile(tableName, fileHandle);
    if(rc != SUCCESS) return rc;
    rc = rbfm->updateRecord(fileHandle, attrs, data, rid);
    if(rc != SUCCESS) return rc;
    rc = rbfm->closeFile(fileHandle);
    return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    FileHandle fileHandle;
    vector<Attribute> attrs;
    cout<<"readtuple"<<endl;
    RC rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;
    cout<<"getAttr"<<endl;
    rc = rbfm->openFile(tableName, fileHandle);
    if(rc != SUCCESS) return rc;
    cout<<"openfile"<<endl;
    rc = rbfm->readRecord(fileHandle, attrs, rid, data);
    if(rc != SUCCESS) return rc;
    rc = rbfm->closeFile(fileHandle);
    return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RC rc = rbfm->printRecord(attrs, data);
    return rc;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    FileHandle fileHandle;
    vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    rc = rbfm->openFile(tableName, fileHandle);
    if(rc != SUCCESS) return rc;
    if(rc != SUCCESS) return rc;
    rc = rbfm->readAttribute(fileHandle, attrs, rid, attributeName, data);
    if(rc != SUCCESS) return rc;
    rc = rbfm->closeFile(fileHandle);
    return rc;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    /*FileHandle fileHandle;
    rm_ScanIterator.rm = RelationManager::instance();
    rm_ScanIterator.rbfmIter.rbfm = rbfm;
    vector<Attribute> recordDescriptor;
    rm_ScanIterator.rm->getAttributes(tableName, recordDescriptor);
    rbfm->openFile(tableName, fileHandle);
    rm_ScanIterator.rbfmIter.fileHandle = &fileHandle;
    rm_ScanIterator.rbfmIter.recordDescriptor = recordDescriptor;
    rm_ScanIterator.rbfmIter.conditionAttribute = conditionAttribute;
    rm_ScanIterator.rbfmIter.compOp = compOp;
    rm_ScanIterator.rbfmIter.value = value;
    rm_ScanIterator.rbfmIter.attributeNames = attributeNames;
    //cout<<"c1"<<endl;
    rm_ScanIterator.rbfmIter.numPages = rm_ScanIterator.rbfmIter.fileHandle->getNumberOfPages();
    //cout<<"c3: "<<rbfm_ScanIterator.fileHandle->getNumberOfPages()<<endl;
    rm_ScanIterator.rbfmIter.nextRid.pageNum = 0;
    rm_ScanIterator.rbfmIter.loadedPage = malloc(PAGE_SIZE); //TODO: free when close()
    //cout<<"c4"<<endl;
    //cout<<rbfm_ScanIterator.nextRid.pageNum<<endl;
    RC rc = rm_ScanIterator.rbfmIter.fileHandle->readPage(rm_ScanIterator.rbfmIter.nextRid.pageNum, rm_ScanIterator.rbfmIter.loadedPage);
    if (rc != SUCCESS) {
        //cout<<"fail: "<<rc<<endl;
        return rc;
    }
    rbfm->closeFile(fileHandle);
    //cout<<"c5"<<endl;
    rm_ScanIterator.rbfmIter.numSlots = rm_ScanIterator.rbfmIter.rbfm->getNumSlots(rm_ScanIterator.rbfmIter.loadedPage);
    rm_ScanIterator.rbfmIter.nextRid.slotNum = 1;*/

    rm_ScanIterator.rm = RelationManager::instance();
    //rm_ScanIterator.rbfmIter.rbfm = rbfm;
    vector<Attribute> recordDescriptor;
    rm_ScanIterator.rm->getAttributes(tableName, recordDescriptor);
    //cout<<"getAttributes: "<<recordDescriptor.size()<<endl;
    //FileHandle fileHandle;
    rbfm->openFile(tableName, rm_ScanIterator.rmFileHandle);
    //cout<<"filehandle in rm scan"<<endl;
    unsigned writeNum = 0;
    unsigned readNum = 0;
    unsigned appendNum = 0;
    rm_ScanIterator.rmFileHandle.collectCounterValues(readNum, writeNum, appendNum);
    cout<<writeNum<<", "<<readNum<<", "<<appendNum<<endl;
    //RC rc = rm_ScanIterator.rbfmIter.fileHandle->readPage(rm_ScanIterator.rbfmIter.nextRid.pageNum, rm_ScanIterator.rbfmIter.loadedPage);
    rbfm->scan(rm_ScanIterator.rmFileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfmIter);
    //rbfm->closeFile(fileHandle);
    return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
    //cout<<"getNextTuple"<<endl;
    return rbfmIter.getNextRecord(rid, data);
}

RC RM_ScanIterator::close(){
    //rbfmIter.rbfm->closeFile(fileHandle);
    return rbfmIter.close();
}

RM_ScanIterator:: ~RM_ScanIterator() {
    rbfmIter.rbfm->closeFile(rmFileHandle);
};

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
        //cout<<"generateTableRecord"<<endl;
        //cout<<tableName<<endl;
        //rbfm->printRecord(tableAttr, data);
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
        int colNameLength = columnName.size();
        memcpy((char*)data+length, &colNameLength, sizeof(int));
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