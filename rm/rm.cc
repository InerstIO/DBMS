
#include "rm.h"
#include "../rbf/rbfm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
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
    Attribute hasIndex;
    hasIndex.name = "hasIndex";
    hasIndex.type = TypeInt;
    hasIndex.length = 4;
    columnAttr.push_back(tableID);
    columnAttr.push_back(columnName);
    columnAttr.push_back(columnType);
    columnAttr.push_back(columnLength);
    columnAttr.push_back(columnPosition);
    columnAttr.push_back(hasIndex);
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    RC rc1 = rbfm->createFile(tableFileName);
    RC rc2 = rbfm->createFile(columnFileName);
    if(rc1==SUCCESS && rc2==SUCCESS){
        void* data = malloc(4000);
        memset(data, 0, 4000);
        int length = 0;
        RID rid;
        RC rct = generateTableRecord(1, "Tables", tableFileName, data, length);
        if(rct != SUCCESS) return -1;
        insertTupleHelper(tableFileName, tableAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rct = generateTableRecord(2, "Columns", columnFileName, data, length);
        if(rct != SUCCESS) return -1;
        insertTupleHelper(tableFileName, tableAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        RC rcc = generateColumnRecord(1, "table-id", TypeInt, 4, 1, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(1, "table-name", TypeVarChar, 50, 2, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(1, "file-name", TypeVarChar, 50, 3, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(2, "table-id", TypeInt, 4, 1, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(2, "column-name", TypeVarChar, 50, 2, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(2, "column-type", TypeInt, 4, 3, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(2, "column-length", TypeInt, 4, 4, data, length);
        if(rcc != SUCCESS) return -1;
        insertTupleHelper(columnFileName, columnAttr, data, rid);
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(2, "column-position", TypeInt, 4, 5, data, length);
        if(rcc != SUCCESS) return -1;
        length = 0;memset(data, 0, 4000);
        rcc = generateColumnRecord(2, "hasIndex", TypeInt, 4, 6, data, length);
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
    FileHandle fileHandle;
    rbfm->openFile(tableFileName, fileHandle);
    RBFM_ScanIterator rbfmIter;// = RBFM_ScanIterator();
    vector<string> targetAttr;
    for(int i=0;i<tableAttr.size();i++){
        targetAttr.push_back(tableAttr[i].name);
    }
    RC rc = rbfm->scan(fileHandle, tableAttr, "", NO_OP, NULL, targetAttr, rbfmIter);
    RID rid;
    void* data = malloc(4000);
    memset(data, 0, 4000);
    while(!rbfmIter.getNextRecord(rid, data)){
        //rbfm->printRecord(tableAttr, data);
        int id;
        memcpy(&id, (char*)data+1, sizeof(int));
        usedIds.insert(id);
        memset(data, 0, 4000);
    }
    int tableId;
    for(int i=1;i<INT_MAX;i++){
        if(usedIds.find(i) == usedIds.end()){
            tableId = i;
            break;
        }
    }
    rbfm->closeFile(fileHandle);
    int length = 0;
    rc = generateTableRecord(tableId, tableName, tableName, data, length);
    if(rc != SUCCESS) return -1;
    insertTupleHelper(tableFileName, tableAttr, data, rid);
    length = 0;
    for(int i=0;i<attrs.size();i++){
        Attribute attr = attrs[i];
        length = 0;
        memset(data,0,4000);
        rc = generateColumnRecord(tableId, attr.name, attr.type, (int)attr.length, i+1, data, length);
        if(rc != SUCCESS) return -1;
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
    while(rbfmIter.getNextRecord(rid, data) != RM_EOF){
        int length;
        int pos = 1;
        memcpy(&table_id, (char*)data+pos, sizeof(int));
        pos += 4;
        memcpy(&length, (char*)data+pos, sizeof(int));
        pos += 4;
        string targetTableName;
        targetTableName.resize(length);
        memcpy((char*)targetTableName.data(),(char*)data+pos,length);
        pos += length;
        bool isTableSame = (targetTableName==tableName);
        if(isTableSame){
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
    rbfm->closeFile(fileHandle);
    rbfm->openFile(columnFileName, fileHandle);
    rbfmIter.close();
    vector<string> attrs;
    for(int i=0;i<columnAttr.size();i++)
        attrs.push_back(columnAttr[i].name);
    rc = rbfm->scan(fileHandle, columnAttr, "", NO_OP, NULL, attrs, rbfmIter);
    if(rc != SUCCESS) return -1;
    memset(data,0,4000);
    while(!rbfmIter.getNextRecord(rid, data)){
        int id;
        memcpy(&id, (char*)data+1, sizeof(int));
        if(id == table_id){
            rbfm->deleteRecord(fileHandle, columnAttr, rid);
        }
        memset(data,0,4000);
    }
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
    if(rc != SUCCESS) {cout<<"fail"<<endl;return -1;}
    void* data = malloc(4000);
    memset(data,0,4000);
    RID rid;
    int tableId = -1;
    while(rbfmIter.getNextRecord(rid, data) != RM_EOF){
        int pos = 1;
        int id;
        memcpy(&id, (char*)data+pos, sizeof(int));
        pos += 4;
        int length;
        memcpy(&length, (char*)data+pos, sizeof(int));
        pos += 4;
        string name;
        name.resize(length);
        memcpy((char*)name.data(),(char*)data+pos,length);
        pos += length;
        if(name == tableName){
            tableId = id;
            break;
        }
        memset(data,0,4000);
    }
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
    rbfmIter.close();
    rc = rbfm->scan(fileHandle, columnAttr, "", NO_OP, NULL, columnAttrStr, rbfmIter);
    if(rc!=SUCCESS) {cout<<"fail"<<endl;return -1;}
    map<int, Attribute> attrMap;
    while(!rbfmIter.getNextRecord(rid, data)){
        int pos = 1;
        int id;
        memcpy(&id, (char*)data+pos, sizeof(int));
        pos+=4;
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
    if(tableName==tableFileName || tableName==columnFileName)
        return -1;
    vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;
    FileHandle fileHandle;
    rc = rbfm->openFile(tableName, fileHandle);
    if(rc != SUCCESS) return rc;
    rc = rbfm->insertRecord(fileHandle, attrs, data, rid);
    if(rc != SUCCESS) return rc;
    rc = rbfm->closeFile(fileHandle);
    if(rc != SUCCESS) return rc;

    //insert to index

    void* key = malloc(4000);
    memset((char*)key, 0, 4000);
    for(int i=0;i<attrs.size();i++){
        bool hasIx = false;
        rc = hasIndex(tableName, attrs[i].name, hasIx);
        if(rc != SUCCESS) return rc;
        //cout<<"inserttuple: "<<attrs[i].name<<", "<<(hasIx?"true":"false")<<endl;
        if(hasIx){
            rc = readAttribute(tableName, rid, attrs[i].name, key);
            if(rc != SUCCESS) return rc;
            string indexFileName = tableName+"."+attrs[i].name;
            IXFileHandle ixfileHandle;
            rc = indexManager->openFile(indexFileName, ixfileHandle);
            if(rc != SUCCESS) return rc;
            //cout<<"insert entry: "<<attrs[i].name<<", "<<*(int*)((char*)key+1)<<endl;
            rc = indexManager->insertEntry(ixfileHandle, attrs[i], ((char*)key+1), rid);
            if(rc != SUCCESS) return rc;
            rc = indexManager->closeFile(ixfileHandle);
            if(rc != SUCCESS) return rc;
            memset((char*)key, 0, 4000);
        }
    }
    free(key);
    return SUCCESS;
}

RC RelationManager::insertTupleHelper(const string &tableName, vector<Attribute>& attributes, const void* data, RID& rid){
    FileHandle fileHandle;
    RC rc = rbfm->openFile(tableName, fileHandle);
    if(rc != SUCCESS) return -1;
    rc = rbfm->insertRecord(fileHandle, attributes, data, rid);
    if(rc != SUCCESS) return -1;
    rc = rbfm->closeFile(fileHandle);
    if(rc != SUCCESS) return -1;
    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    //cout<<"delete tuple"<<endl;
    FileHandle fileHandle;
    vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;

    //delete index entry
    void* data = malloc(4000);
    memset(data,0,4000);
    for(int i=0;i<attrs.size();i++){
        bool hasIx = false;
        rc = hasIndex(tableName, attrs[i].name, hasIx);
        if(rc != SUCCESS) return rc;
        if(hasIx){
            rc = readAttribute(tableName, rid, attrs[i].name, data);
            if(rc != SUCCESS) return rc;
            string indexFileName = tableName+"."+attrs[i].name;
            IXFileHandle ixfileHandle;
            rc = indexManager->openFile(indexFileName, ixfileHandle);
            if(rc != SUCCESS) return rc;
            rc = indexManager->deleteEntry(ixfileHandle, attrs[i], ((char*)data+1), rid);
            if(rc != SUCCESS) return rc;
            rc = indexManager->closeFile(ixfileHandle);
            if(rc != SUCCESS) return rc;
            memset(data,0,4000);
        }
    }
    free(data);

    rc = rbfm->openFile(tableName, fileHandle);
    if(rc != SUCCESS) return rc;
    rc = rbfm->deleteRecord(fileHandle, attrs, rid);
    if(rc != SUCCESS) return rc;
    rc = rbfm->closeFile(fileHandle);
    if(rc != SUCCESS) return rc;
    return SUCCESS;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    FileHandle fileHandle;
    vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;

    //delete original index entries
    void* originData = malloc(4000);
    memset(originData,0,4000);
    for(int i=0;i<attrs.size();i++){
        bool hasIx = false;
        rc = hasIndex(tableName, attrs[i].name, hasIx);
        if(rc != SUCCESS) return rc;
        if(hasIx){
            //int temp;
            rc = readAttribute(tableName, rid, attrs[i].name, originData);
            if(rc != SUCCESS) return rc;
            string indexFileName = tableName+"."+attrs[i].name;
            IXFileHandle ixfileHandle;
            rc = indexManager->openFile(indexFileName, ixfileHandle);
            if(rc != SUCCESS) return rc;
            //cout<<indexFileName<<", "<<attrs[i].name<<", "<<*(int*)((char*)originData+1)<<endl;
            rc = indexManager->deleteEntry(ixfileHandle, attrs[i], ((char*)originData+1), rid);
            if(rc != SUCCESS) return rc;
            rc = indexManager->closeFile(ixfileHandle);
            if(rc != SUCCESS) return rc;
            memset(originData,0,4000);
        }
    }
    //cout<<"delete entry"<<endl;

//update record
    rc = rbfm->openFile(tableName, fileHandle);
    if(rc != SUCCESS) return rc;
    rc = rbfm->updateRecord(fileHandle, attrs, data, rid);
    if(rc != SUCCESS) return rc;
    rc = rbfm->closeFile(fileHandle);
    if(rc != SUCCESS) return rc;
    //cout<<"update record"<<endl;

//insert new data to index entries
    void* key = malloc(4000);
    memset((char*)key, 0, 4000);
    for(int i=0;i<attrs.size();i++){
        bool hasIx = false;
        rc = hasIndex(tableName, attrs[i].name, hasIx);
        if(rc != SUCCESS) return rc;
        if(hasIx){
            rc = readAttribute(tableName, rid, attrs[i].name, key);
            if(rc != SUCCESS) return rc;
            string indexFileName = tableName+"."+attrs[i].name;
            IXFileHandle ixfileHandle;
            rc = indexManager->openFile(indexFileName, ixfileHandle);
            if(rc != SUCCESS) return rc;
            rc = indexManager->insertEntry(ixfileHandle, attrs[i], key, rid);
            if(rc != SUCCESS) return rc;
            rc = indexManager->closeFile(ixfileHandle);
            if(rc != SUCCESS) return rc;
            memset((char*)key, 0, 4000);
        }
    }
    //cout<<"insert entry"<<endl;
    free(key);

    return SUCCESS;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    FileHandle fileHandle;
    vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;
    rc = rbfm->openFile(tableName, fileHandle);
    if(rc != SUCCESS) return rc;
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
    rm_ScanIterator.rm = RelationManager::instance();
    vector<Attribute> recordDescriptor;
    rm_ScanIterator.rm->getAttributes(tableName, recordDescriptor);
    rbfm->openFile(tableName, rm_ScanIterator.rmFileHandle);
    unsigned writeNum = 0;
    unsigned readNum = 0;
    unsigned appendNum = 0;
    rm_ScanIterator.rmFileHandle.collectCounterValues(readNum, writeNum, appendNum);
    rbfm->scan(rm_ScanIterator.rmFileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfmIter);
    return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
    return rbfmIter.getNextRecord(rid, data);
}

RC RM_ScanIterator::close(){
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

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
    //cout<<attributeName<<endl;
    bool hasIx;
    RC rc = hasIndex(tableName, attributeName, hasIx);
    if(hasIx) return -1;
    string indexFileName = tableName+"."+attributeName;
    rc = indexManager->createFile(indexFileName);
    if(rc != SUCCESS) return -1;
    //cout<<"set index true"<<endl;
    setIndex(tableName, attributeName, true);

    vector<Attribute> attrs;
    rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;
    Attribute targetAttr;
    for(int i=0;i<attrs.size();i++){
        if(attrs[i].name == attributeName){
            targetAttr = attrs[i];
            break;
        }
    }

    FileHandle fileHandle;
    RBFM_ScanIterator rbfmIter;
    vector<string> attrStr;
    attrStr.push_back(attributeName);
    rc = rbfm->openFile(tableName, fileHandle);
    if(rc != SUCCESS) {
        //cout<<"open file fail"<<endl;
        return -1;
    }
    rc = rbfm->scan(fileHandle, attrs, "", NO_OP, NULL, attrStr, rbfmIter);
    //cout<<"scan"<<endl;
    if(rc != SUCCESS) {return SUCCESS;}
    void* data = malloc(4000);
    void* key = malloc(4000);
    memset(key,0,4000);
    memset(data,0,4000);
    RID rid;
    IXFileHandle ixfileHandle;
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    //cout<<"open file"<<endl;
    if(rc != SUCCESS) return rc;
    int i=0;
    while(rbfmIter.getNextRecord(rid, data) != RM_EOF){
        memcpy((char*)key, (char*)data+1, 3999);
        rc = indexManager->insertEntry(ixfileHandle, targetAttr, key, rid);
        if(rc != SUCCESS) return rc;
        //cout<<"insert entry: "<<indexFileName<<": "<<i++<<endl;
        memset(key,0,4000);
        memset(data,0,4000);
    }
    rc = indexManager->closeFile(ixfileHandle);
    if(rc != SUCCESS) return rc;
    rc = rbfm->closeFile(fileHandle);
    if(rc != SUCCESS) return rc;
	return SUCCESS;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
    bool hasIx;
    RC rc = hasIndex(tableName, attributeName, hasIx);
    if(!hasIx) return -1;
    string indexFileName = tableName+"."+attributeName;
    rc = indexManager->destroyFile(indexFileName);
    if(rc != SUCCESS) return -1;
    setIndex(tableName, attributeName, false);
    return SUCCESS;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
    rm_IndexScanIterator.rm = RelationManager::instance();
    rm_IndexScanIterator.tableName = tableName;
    bool findAttr = false;
    vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != SUCCESS) return rc;
    Attribute targetAttr;
    for(int i=0;i<attrs.size();i++){
        if(attributeName == attrs[i].name){
            targetAttr = attrs[i];
            findAttr = true;
            break;
        }
    }
    if(!findAttr) return -1;

    string indexFileName = tableName+"."+attributeName;
    IXFileHandle ixfileHandle;
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    if(rc != SUCCESS) return rc;
    rc = indexManager->scan(ixfileHandle, targetAttr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_ScanIterator);
    if(rc != SUCCESS) return rc;
    rc = indexManager->closeFile(ixfileHandle);
    if(rc != SUCCESS) return rc;
    return SUCCESS;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
    return ix_ScanIterator.getNextEntry(rid, key);
}
RC RM_IndexScanIterator::close(){
    return ix_ScanIterator.close();
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
        //initially no index file
        char noIndex = 0;
        memcpy((char*)data+length, &noIndex, sizeof(char));
        length += 1;
        return SUCCESS;
    }
}

RC RelationManager::hasIndex(const string& tableName, const string& columnName, bool& result){
    int res = 2; //0 for false, 1 for true, 2 for not found
    int tableId;
    RC rc = getTableId(tableName, tableId);
    if(rc != SUCCESS) return rc;

    FileHandle fileHandle;
    RBFM_ScanIterator rbfmIter;
    vector<string> columnAttrStr;
    for(int i=0;i<columnAttr.size();i++){
        columnAttrStr.push_back(columnAttr[i].name);
    }
    rbfm->openFile(columnFileName, fileHandle);
    rc = rbfm->scan(fileHandle, columnAttr, "", NO_OP, NULL, columnAttrStr, rbfmIter);
    if(rc != SUCCESS) {cout<<"fail"<<endl;return -1;}
    void* data = malloc(4000);
    memset(data,0,4000);
    RID rid;
    while(rbfmIter.getNextRecord(rid, data) != RM_EOF){
        int pos = 1;
        int id;
        memcpy(&id, (char*)data+pos, sizeof(int));
        pos += 4;
        if(id == tableId){
            int length;
            memcpy(&length, (char*)data+pos, sizeof(int));
            pos += 4;
            string name;
            name.resize(length);
            memcpy((char*)name.data(),(char*)data+pos,length);
            pos += length;
            //cout<<"has index: "<<columnName<<", "<<name<<endl;
            if(columnName == name){
                pos += 12;
                memcpy(&res, (char*)data+pos, 4);
                //cout<<"get pos: "<<id<<", "<<name<<", "<<pos<<", "<<res<<", "<<rid.pageNum<<", "<<rid.slotNum<<endl;
                break;
            }
        }
        memset(data,0,4000);
    }
    free(data);
    if(res == 0){
        result = false;
    } else if(res == 1){
        result = true;
    } else{
        result = false;
        return -1;
    }
    return SUCCESS;
}

RC RelationManager::setIndex(const string& tableName, const string& columnName, bool hasIx){
    int res = 2; //0 for false, 1 for true, 2 for not found
    int tableId;
    RC rc = getTableId(tableName, tableId);
    if(rc != SUCCESS) return rc;

    FileHandle fileHandle;
    RBFM_ScanIterator rbfmIter;
    vector<string> columnAttrStr;
    for(int i=0;i<columnAttr.size();i++){
        columnAttrStr.push_back(columnAttr[i].name);
    }
    rbfm->openFile(columnFileName, fileHandle);
    rc = rbfm->scan(fileHandle, columnAttr, "", NO_OP, NULL, columnAttrStr, rbfmIter);
    if(rc != SUCCESS) {cout<<"fail"<<endl;return -1;}
    void* data = malloc(4000);
    memset(data,0,4000);
    RID rid;
    while(rbfmIter.getNextRecord(rid, data) != RM_EOF){
        int pos = 1;
        int id;
        memcpy(&id, (char*)data+pos, sizeof(int));
        pos += 4;
        if(id == tableId){
            int length;
            memcpy(&length, (char*)data+pos, sizeof(int));
            pos += 4;
            string name;
            name.resize(length);
            memcpy((char*)name.data(),(char*)data+pos,length);
            pos += length;
            if(columnName == name){
                pos += 12;
                memcpy(&res, (char*)data+pos, 4);
                int newHasIx;
                if(hasIx){
                    newHasIx = 1;
                } else{
                    newHasIx = 0;
                }
                //cout<<"set pos: "<<id<<", "<<name<<", "<<pos<<", "<<newHasIx<<", "<<rid.pageNum<<", "<<rid.slotNum<<endl;
                memcpy((char*)data+pos, &newHasIx, 4);
                rbfm->updateRecord(fileHandle, columnAttr, data, rid);
                break;
            }
        }
        memset(data,0,4000);
    }
    free(data);
    if(res == 0 || res == 1){
        return SUCCESS;
    } else{
        return -1;
    }
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}

RC RelationManager::getTableId(const string& tableName, int& tableId){
    FileHandle fileHandle;
    RBFM_ScanIterator rbfmIter;
    vector<string> tableAttrStr;
    for(int i=0;i<tableAttr.size();i++){
        tableAttrStr.push_back(tableAttr[i].name);
    }
    rbfm->openFile(tableFileName, fileHandle);
    RC rc = rbfm->scan(fileHandle, tableAttr, "", NO_OP, NULL, tableAttrStr, rbfmIter);
    if(rc != SUCCESS) {cout<<"get table id fail"<<endl;return -1;}
    void* data = malloc(4000);
    memset(data,0,4000);
    RID rid;
    tableId = -1;
    while(rbfmIter.getNextRecord(rid, data) != RM_EOF){
        int pos = 1;
        int id;
        memcpy(&id, (char*)data+pos, sizeof(int));
        pos += 4;
        int length;
        memcpy(&length, (char*)data+pos, sizeof(int));
        pos += 4;
        string name;
        name.resize(length);
        memcpy((char*)name.data(),(char*)data+pos,length);
        pos += length;
        if(name == tableName){
            tableId = id;
            break;
        }
        memset(data,0,4000);
    }
    free(data);
    if(tableId <= 0){
        return -1;
    }
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}