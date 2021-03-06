#include <string.h>

#include "rbfm.h"

vector<bitset<8>> nullIndicators(int size, const void *data) {
    vector<bitset<8>> nullBits;
    for(int i = 0; i < size; i++){
        bitset<8> onebyte(*((char*)data+i));
        nullBits.push_back(onebyte);
    }
    return nullBits;
}

void* data2record(const void* data, const vector<Attribute>& recordDescriptor, unsigned short& length){
    int dataPos = 0;
    int attrNum = recordDescriptor.size();
    int nullIndSize = ceil((double)attrNum/(double)8);
    vector<bitset<8>> nullBits = nullIndicators(nullIndSize, data);
    int recordSize = 4;
    int valSize = 0;
    for(unsigned i=0;i<recordDescriptor.size();i++){
        recordSize += 2;
        recordSize += recordDescriptor[i].length;
        valSize += recordDescriptor[i].length;
    }
    char* record = new char[recordSize];
    //put how many attributes
    *(record+3) = attrNum>>24;
    *(record+2) = attrNum>>16 & 0x00ff;
    *(record+1) = attrNum>>8 & 0x0000ff;
    *(record+0) = attrNum & 0x000000ff;
    length += 4;
    //put offset in record, put values in value
    char* values = new char[valSize];
    int valLength = 0;
    short offset = 0;
    dataPos += nullIndSize;
    for(int i=0;i<attrNum;i++){
        if(nullBits[i/8][7-i%8] == 0){
            if(recordDescriptor[i].type == 0 || recordDescriptor[i].type == 1){
                offset += 4;
                for(int j=0;j<4;j++){
                    *(values+valLength) = *((char*)data+dataPos);
                    dataPos++;
                    valLength++;
                }
            } else{
                int stringLength;
                memcpy(&stringLength, (char*)data+dataPos, sizeof(int));
                dataPos += 4;
                offset += stringLength;
                for(int j=0;j<stringLength;j++){
                    *((char*)values+valLength) = *((char*)data+dataPos);
                    valLength++;
                    dataPos++;
                }
            }
            *(record+length+1) = offset>>8;
            *(record+length) = offset & 0x00ff;
            length += 2;
        } else{
            *(record+length+1) = 0xff;
            *(record+length) = 0xff;
            length += 2;
        }
    }
    for(int i=0;i<valLength;i++){
        *((char*)record+length) = *(values+i);
        length++;
    }
    delete[] values;
    return (void*)record;
}

void record2data(const void* record, const vector<Attribute>& recordDescriptor, void* data){
    int length = 0;
    int pos = 0;
    int num;
    memcpy(&num, record, 4);
    pos += 4;
    vector<char> nullIndicator(ceil((double)num/8), 0);
    vector<short> pointers;
    for(int i=0;i<num;i++){
        short pointer;
        memcpy(&pointer, (char*)record+pos, sizeof(short));
        pos += 2;
        pointers.push_back(pointer);
        if(pointer == -1){
            nullIndicator[i/8] += 1<<(7-i%8);
        }
    }
    for(unsigned i=0;i<nullIndicator.size();i++){
        ((char*)data)[length] = nullIndicator[i];
        length++;
    }
    int lastPointer = 0;
    for(unsigned i=0;i<recordDescriptor.size();i++){
        if(pointers[i] != -1){
            if(recordDescriptor[i].type == 2){
                int l = pointers[i]-lastPointer;
                memcpy((char*)data+length, &l, sizeof(int));
                length += 4;
                memcpy((char*)data+length, (char*)record+pos, l);
                length += l;
                pos += l;
            } else{
                memcpy((char*)data+length, (char*)record+pos, 4);
                length += 4;
                pos += 4;
            }
            lastPointer = pointers[i];
        }
    }
}

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    pfm = PagedFileManager::instance();
    curPage = 0;
    curSlot = 0;
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    unsigned short length = 0;
    char *record = (char *)data2record(data, recordDescriptor, length);
    // leave at least sizeof(RID) space to make tomestone possible
    length = max(length, (unsigned short)sizeof(RID));
    RC rc = insertPos(fileHandle, length, rid);
    if (rc != SUCCESS) {
        delete[] record;
        return rc;
    }
    void *page = malloc(PAGE_SIZE);
    memset(page,0,PAGE_SIZE);
    if (rid.pageNum == fileHandle.getNumberOfPages()) {
        //init the page
        unsigned short zero = 0;
        setFreeBegin(zero, page);
        setNumSlots(zero, page);
        //update the page
        insert2data(page, record, length, rid.slotNum);
        delete[] record;
        //append new page
        RC rc = fileHandle.appendPage(page);
        if (rc != SUCCESS) {
            free(page);
            return rc;
        }
    }
    else
    {
        RC rc = fileHandle.readPage(rid.pageNum, page);
        if (rc != SUCCESS) {
            delete[] record;
            free(page);
            return rc;
        }
        //update the page
        insert2data(page, record, length, rid.slotNum);
        delete[] record;
        //write page
        rc = fileHandle.writePage(rid.pageNum, page);
        if (rc != SUCCESS) {
            free(page);
            return rc;
        }
    }
    free(page);
    return SUCCESS;
}

SlotDir RecordBasedFileManager::getSlotDir(const unsigned slotNum, const void* page) {
    SlotDir slotDir;
    memcpy(&slotDir, (char *)page + PAGE_SIZE - 2 * sizeof(short) - slotNum * sizeof(SlotDir), sizeof(SlotDir));
    return slotDir;
}

void RecordBasedFileManager::getRecord(void* record, SlotDir slotDir, void* page) {
    memcpy(record, (char *)page + slotDir.offset, slotDir.length);
    return;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    void *page = malloc(PAGE_SIZE);
    SlotDir* slotDir = new SlotDir;
    RC rc = getPageSlotDir(fileHandle, rid, page, slotDir);
    if (rc) {
        free(page);
        delete slotDir;
        return rc;
    }
    
    char *record = new char[slotDir->length];
    getRecord(record, *slotDir, page);
    record2data(record, recordDescriptor, data);
    free(page);
    delete slotDir;
    delete[] record;
    return 0;
}

RC RecordBasedFileManager::getPageSlotDir(FileHandle &fileHandle, const RID &rid, void* page, SlotDir* slotDirPtr) {
    RC rc = fileHandle.readPage(rid.pageNum, page);
    if (rc) {
        return rc;
    }
    SlotDir slotDir = getSlotDir(rid.slotNum, page);
    
    if (slotDir.offset == USHRT_MAX) {
        return -1; // deleted record.
    }

    if (slotDir.tombstone) {
        RID realRid;
        getRecord(&realRid, slotDir, page);
        RC rc = fileHandle.readPage(realRid.pageNum, page);
        if (rc) {
            return rc;
        }
        slotDir = getSlotDir(realRid.slotNum, page);
    }

    memcpy(slotDirPtr, &slotDir, sizeof(SlotDir));
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    int attrNum = recordDescriptor.size();
    int nullIndSize = ceil((double)attrNum/(double)8);
    vector<bitset<8>> nullBits = nullIndicators(nullIndSize, data);
    int offset = nullIndSize;
    int intVal, charLen;
    float floatVal;

    for(int i = 0; i < attrNum; i++)
    {
        cout<<recordDescriptor[i].name<<": ";
        if (nullBits[i/8][7-i%8]) {
            cout<<"NULL"<<endl;
        }
        else {
            switch (recordDescriptor[i].type)
            {
                case 0:
                    memcpy(&intVal, (char *)data + offset, sizeof(int));
                    cout<<intVal<<endl;
                    offset += sizeof(int);
                    break;
                case 1:
                    memcpy(&floatVal, (char *)data + offset, sizeof(float));
                    cout<<floatVal<<endl;
                    offset += sizeof(float);
                    break;
                case 2:
                    memcpy(&charLen, (char *)data + offset, sizeof(int));
                    offset += sizeof(int);
                    char *str = new char[charLen+1];
                    memcpy(str, (char *)data + offset, charLen);
                    str[charLen] = '\0';
                    offset += charLen;
                    cout<<str<<endl;
                    delete[] str;
                    break;
            }
        }
    }
    return 0;
}

void RecordBasedFileManager::setSlotDir(void* page, unsigned slotNum, SlotDir slotDir) {
    memcpy((char *)page + PAGE_SIZE - 2 * sizeof(short) - slotNum * sizeof(SlotDir), &slotDir, sizeof(SlotDir));
    return;
}

void RecordBasedFileManager::updateSlotDirOffsets(void* page, unsigned afterOffset, short numSlots, short delta) {
    for(int i = 1; i <= numSlots; i++)
    {
        SlotDir slotDir = getSlotDir(i, page);
        if (slotDir.offset == USHRT_MAX || slotDir.offset <= afterOffset) {
            continue;
        }
        slotDir.offset += delta;
        setSlotDir(page, i, slotDir);
    }
    return;
}

void RecordBasedFileManager::moveRecords(void* page, unsigned short destOffset, short freeBegin, short delta) {
    memmove((char *)page + destOffset, (char *)page + destOffset - delta, freeBegin - destOffset + delta);
    return;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    void *page = malloc(PAGE_SIZE);
    memset(page,0,PAGE_SIZE);
    RC rc = fileHandle.readPage(rid.pageNum, page);
    if (rc) {
        free(page);
        return rc;
    }
    SlotDir slotDir = getSlotDir(rid.slotNum, page);
    unsigned short recordLength = slotDir.length;
    short numSlots = getNumSlots(page);
    
    if (slotDir.tombstone) {
        RID realRid;
        getRecord(&realRid, slotDir, page);
        rc = deleteRecord(fileHandle, recordDescriptor, realRid);
        if (rc) {
            free(page);
            return rc;
        }
        // Handle a rare situation that new record is on the same page as the tombstone.
        if (realRid.pageNum == rid.pageNum) {
            rc = fileHandle.readPage(rid.pageNum, page);
            if (rc) {
                free(page);
                return rc;
            }
        }
    }

    short freeBegin = getFreeBegin(page);
    moveRecords(page, slotDir.offset, freeBegin, -recordLength);
    unsigned short deletedOffset = slotDir.offset;
    slotDir.offset = USHRT_MAX;
    setSlotDir(page, rid.slotNum, slotDir);
    updateSlotDirOffsets(page, deletedOffset, numSlots, -recordLength);
    
    setFreeBegin(freeBegin-recordLength, page);
    // do not update numSlots because we need that unchanged to find insertion position.
    //write page
    rc = fileHandle.writePage(rid.pageNum, page);
    if (rc) {
        free(page);
        return rc;
    }
    free(page);
    return 0;
}

void RecordBasedFileManager::setRecord(void* page, void* record, SlotDir slotDir) {
    memcpy((char *)page + slotDir.offset, record, slotDir.length);
    return;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
    unsigned short newLength = 0;
    void *page = malloc(PAGE_SIZE);
    memset(page,0,PAGE_SIZE);
    RC rc = fileHandle.readPage(rid.pageNum, page);
    if (rc) {
        free(page);
        return rc;
    }
    SlotDir slotDir = getSlotDir(rid.slotNum, page);
    char *record = (char *)data2record(data, recordDescriptor, newLength);
    
    bool tombstone = slotDir.tombstone;
    if (tombstone) { //TODO: test this block! Not covered in test cases.
        /*
        1. get realRid
        2. 
            if realRid.pageNum has enough freeSpace
                updateRecord(..., realRid)
            else
                updateRecord(..., realRid)
                get tombstone record(newRid) on realPage
                delete the tombstone record in realPage
                set the tombstone record in current page to newRid
        */
        RID realRid;
        getRecord(&realRid, slotDir, page);
        void* realPage = malloc(PAGE_SIZE);
        memset(realPage,0,PAGE_SIZE);
        RC rc = fileHandle.readPage(realRid.pageNum, realPage);
        if (rc) {
            free(page);
            delete[] record;
            free(realPage);
            return rc;
        }
        SlotDir realSlotDir = getSlotDir(realRid.slotNum, realPage);
        if (freeSpace(realPage) + realSlotDir.length >= newLength) {
            rc = updateRecord(fileHandle, recordDescriptor, data, realRid);
            free(page);
            delete[] record;
            free(realPage);
            return rc;
        }
        else {
            rc = updateRecord(fileHandle, recordDescriptor, data, realRid);
            if (rc) {
                free(page);
                delete[] record;
                free(realPage);
                return rc;
            }
            RID newRid;
            getRecord(&newRid, realSlotDir, realPage);
            deleteRecord(fileHandle, recordDescriptor, realRid);
            setRecord(page, &newRid, slotDir);
        }
        free(realPage);
    }
    else
    {
        if (slotDir.length == newLength) {
            setRecord(page, record, slotDir);
        }
        // have enough freeSpace
        else if(freeSpace(page) + slotDir.length >= newLength) {
            unsigned short length = slotDir.length;
            slotDir.length = newLength;
            setSlotDir(page, rid.slotNum, slotDir);
            short freeBegin = getFreeBegin(page);
            moveRecords(page, slotDir.offset + newLength, freeBegin, newLength - length);
            setFreeBegin(freeBegin - length + newLength, page);
            short numSlots = getNumSlots(page);
            updateSlotDirOffsets(page, slotDir.offset, numSlots, newLength-length);
            setRecord(page, record, slotDir);
        }
        else {
            /*
            1. find a new position (rid)
            2. update old record to rid
            3. update slotDir.length&tombstone & set slotDir
            4. move records forward if length different
            5. update slotDir offsets if length different
            6. update freeBegin if length different
            7. insert in new position
            */
            RID newRid;
            RC rc = insertPos(fileHandle, newLength, newRid);
            if (rc) {
                free(page);
                delete[] record;
                return rc;
            }
            unsigned short length = slotDir.length;
            slotDir.length = sizeof(RID);
            slotDir.tombstone = true;
            setSlotDir(page, rid.slotNum, slotDir);
            setRecord(page, &newRid, slotDir);
            short freeBegin = getFreeBegin(page);
            
            if (length != sizeof(RID)) {
                moveRecords(page, slotDir.offset + sizeof(RID), freeBegin, sizeof(RID) - length);
                short numSlots = getNumSlots(page);
                updateSlotDirOffsets(page, slotDir.offset, numSlots, sizeof(RID)-length);
                setFreeBegin(freeBegin -length + sizeof(RID), page);
            }

            insertRecord(fileHandle, recordDescriptor, data, newRid);
        }
    }
    // write page
    rc = fileHandle.writePage(rid.pageNum, page);
    if (rc) {
        free(page);
        delete[] record;
        return rc;
    }
    free(page);
    delete[] record;
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) {
    /*
    1. get record
    2. get attribute id
    3. read attribute
    */
    void *page = malloc(PAGE_SIZE);
    SlotDir* slotDir = new SlotDir;
    RC rc = getPageSlotDir(fileHandle, rid, page, slotDir);
    if (rc!=SUCCESS) {
        free(page);
        delete slotDir;
        return rc;
    }
    char *record = new char[slotDir->length];
    getRecord(record, *slotDir, page);
    readAttributeFromRecord(record, slotDir->length, recordDescriptor, attributeName, data);
    delete[] record;
    free(page);
    delete slotDir;
    return 0;
}

RC RecordBasedFileManager::readAttributeFromRecord(const void* record, unsigned short length, const vector<Attribute> &recordDescriptor, const string &attributeName, void *data) {
    unsigned i;
    short base = sizeof(short)*recordDescriptor.size()+4-1;
    short startAddr = sizeof(short)*recordDescriptor.size()+4;
    short endAddr = startAddr-1;
    bool isNull = false;
    for(i = 0; i < recordDescriptor.size(); i++)
    {
        short pointer;
        memcpy(&pointer, (char*)record+i*sizeof(short)+4, sizeof(short));
                //cout<<pointer<<", ";
        if (recordDescriptor[i].name == attributeName) {
            if(pointer == -1){
                isNull = true;
            } else{
                startAddr = endAddr+1;
                endAddr = pointer+base;
               // cout<<startAddr<<"--"<<base<<"+"<<pointer<<endl;
            }
            break;
        } else{
            if(pointer != -1){
                startAddr = endAddr+1;
                endAddr = pointer+base;
            }
        }
    }
    //cout<<endl;
    if (i == recordDescriptor.size()) {
        return -1;
    }
    
    char nullIndicator;
    if(isNull){
        nullIndicator = 255;
    } else{
        nullIndicator = 0;
    }
    ((char*)data)[0] = nullIndicator;
    if(recordDescriptor[i].type == 2){
        length = endAddr-startAddr+1;
        //cout<<length<<endl;
        memcpy((char*)data+1, &length, sizeof(int));
        memcpy((char*)data+5, (char*)record+startAddr, length);
    } else{
        length = 4;
        memcpy((char*)data+1, (char*)record+startAddr, 4);
    }
    return 0;
}

RBFM_ScanIterator::RBFM_ScanIterator() {
}

RBFM_ScanIterator::~RBFM_ScanIterator(){
}

RC RBFM_ScanIterator::close(){
    //free(value);
    free(loadedPage);
    return SUCCESS;
}

RC RBFM_ScanIterator::getNextRid(RID &rid) {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if (nextRid.pageNum >= numPages) {
        return RBFM_EOF;
    }
    if (nextRid.slotNum > numSlots) {
        nextRid.pageNum++;
        RC rc = fileHandle->readPage(nextRid.pageNum, loadedPage);
        if (rc != SUCCESS) {
            return -1;
        }
        numSlots = rbfm->getNumSlots(loadedPage);
        nextRid.slotNum = 1;
        return getNextRid(rid);
    }
    rid.pageNum = nextRid.pageNum;
    rid.slotNum = nextRid.slotNum;
    nextRid.slotNum++;
    return SUCCESS;
}

RC RecordBasedFileManager::concatData(const void* record, const vector<Attribute> &recordDescriptor, const vector<string> &attributeName, void* data){
    if(recordDescriptor.size() == attributeName.size()){
        record2data(record,recordDescriptor,data);
        return SUCCESS;
    }
    int nullIndSize = ceil(((double)attributeName.size())/8);
    char* nullIndicator = new char[nullIndSize];
    memset(nullIndicator,0,nullIndSize);
    void* dataBody = malloc(4000);
    int offset = 0;
    for(int i=0;i<attributeName.size();i++){
        Attribute targetAttr;
        for(int j=0;j<recordDescriptor.size();j++){
            if(recordDescriptor[j].name == attributeName[i]){
                targetAttr = recordDescriptor[j];
                break;
            }
        }
        string attrName = attributeName[i];
        int length = targetAttr.length;
        void* fieldData = malloc(4000);
        memset(fieldData,0,4000);
        RC rc = readAttributeFromRecord(record, length, recordDescriptor, attrName, fieldData);
        if(rc != SUCCESS){
            return rc;
        }
        char filedNullInd;
        memcpy(&filedNullInd,(char*)fieldData,1);
        memcpy((char*)dataBody+offset, fieldData+1, length);
        offset += length;
        if(filedNullInd > 0){
            nullIndicator[i/8] += 1<<(7-i%8);
        }
        free(fieldData);
    }
    memcpy((char*)data,nullIndicator,nullIndSize);
    memcpy((char*)data+nullIndSize,(char*)dataBody,offset);
    free(dataBody);
    delete[] nullIndicator;
    return SUCCESS;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    SlotDir slotDir;

    unsigned i;
    for(i = 0; i < recordDescriptor.size(); i++)
    {
        if (recordDescriptor[i].name == conditionAttribute) {
            break;
        }
    }
    int conditionDataLength;
    int nullIndSize = ceil(((double)attributeNames.size())/8);
    char nullInd;
    void* conditionData;

    
    while(true){
        do
        {
            if(getNextRid(rid) == RBFM_EOF){
                return RBFM_EOF;
            }
            slotDir = rbfm->getSlotDir(rid.slotNum, loadedPage);
        } while (slotDir.tombstone || slotDir.offset == USHRT_MAX);
        char *record = new char[slotDir.length];
        rbfm->getRecord(record, slotDir, loadedPage);
        

        if (compOp != 6) {
            if (i == recordDescriptor.size()) {
                delete[] record;
                return -1;
            }
            conditionDataLength = recordDescriptor[i].length;
            if(recordDescriptor[i].type == 2){
                conditionDataLength += 4;
            }
            conditionDataLength += recordDescriptor[i].length;
            conditionData = malloc(conditionDataLength);
            memset(conditionData, 0, conditionDataLength);
            rbfm->readAttributeFromRecord(record, slotDir.length, recordDescriptor, conditionAttribute, conditionData);
            //vector<Attribute> vec;
            //vec.push_back(recordDescriptor[i]);
            //void* td = malloc(200);
            //record2data(record,recordDescriptor,td);
            //rbfm->printRecord(vec, conditionData);
            memcpy(&nullInd, (char*)conditionData, 1);
        }

        if(compOp==CompOp::EQ_OP){
            if(recordDescriptor[i].type==0){
                int val;
                if(nullInd != 0){
                    continue;
                } else{
                    memcpy(&val, (char*)conditionData+1, 4);
                    if(val == *(int*)(value)){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else if(recordDescriptor[i].type==1){
                float val;
                if(nullInd != 0){
                    continue;
                } else{
                    memcpy(&val, (char*)conditionData+1, 4);
                    if(val == *(float*)(value)){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else if(recordDescriptor[i].type==2){
                string val;
                if(nullInd != 0){
                    continue;
                } else{
                    int length;
                    memcpy(&length, (char*)conditionData+1, 4);
                    val.resize(length);
                    memcpy((char*)val.data(), (char*)conditionData+5, length);
                    string valueStr;
                    int valueLength;
                    memcpy(&valueLength, (char*)value, sizeof(int));
                    valueStr.resize(valueLength);
                    memcpy((char*)valueStr.data(), (char*)value+4, valueLength);
                    if(val == valueStr){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else{
                return -1;
            }
        } else if(compOp==CompOp::LT_OP){
            if(recordDescriptor[i].type==0){
                int val;
                if(nullInd != 0){
                    continue;
                } else{
                    memcpy(&val, (char*)conditionData+1, 4);
                    if(val < *(int*)(value)){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else if(recordDescriptor[i].type==1){
                float val;
                if(nullInd != 0){
                    continue;
                } else{
                    memcpy(&val, (char*)conditionData+1, 4);
                    if(val < *(float*)(value)){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else if(recordDescriptor[i].type==2){
                string val;
                if(nullInd != 0){
                    continue;
                } else{
                    int length;
                    memcpy(&length, (char*)conditionData+1, 4);
                    val.resize(length);
                    memcpy((char*)val.data(), (char*)conditionData+5, length);
                    string valueStr;
                    int valueLength;
                    memcpy(&valueLength, (char*)value, sizeof(int));
                    valueStr.resize(valueLength);
                    memcpy((char*)valueStr.data(), (char*)value+4, valueLength);
                    if(val < valueStr){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else{
                delete[] record;
                free(conditionData);
                return -1;
            }
        } else if(compOp==CompOp::LE_OP){
            if(recordDescriptor[i].type==0){
                int val;
                if(nullInd != 0){
                    continue;
                } else{
                    memcpy(&val, (char*)conditionData+1, 4);
                    if(val <= *(int*)(value)){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else if(recordDescriptor[i].type==1){
                float val;
                if(nullInd != 0){
                    continue;
                } else{
                    memcpy(&val, (char*)conditionData+1, 4);
                    if(val <= *(float*)(value)){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else if(recordDescriptor[i].type==2){
                string val;
                if(nullInd != 0){
                    continue;
                } else{
                    int length;
                    memcpy(&length, (char*)conditionData+1, 4);
                    val.resize(length);
                    memcpy((char*)val.data(), (char*)conditionData+5, length);
                    string valueStr;
                    int valueLength;
                    memcpy(&valueLength, (char*)value, sizeof(int));
                    valueStr.resize(valueLength);
                    memcpy((char*)valueStr.data(), (char*)value+4, valueLength);
                    if(val <= valueStr){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else{
                delete[] record;
                free(conditionData);
                return -1;
            }
        } else if(compOp==CompOp::GT_OP){
            if(recordDescriptor[i].type==0){
                int val;
                if(nullInd != 0){
                    continue;
                } else{
                    memcpy(&val, (char*)conditionData+1, 4);
                    if(val > *(int*)(value)){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else if(recordDescriptor[i].type==1){
                float val;
                if(nullInd != 0){
                    continue;
                } else{
                    memcpy(&val, (char*)conditionData+1, 4);
                    if(val > *(float*)(value)){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else if(recordDescriptor[i].type==2){
                string val;
                if(nullInd != 0){
                    continue;
                } else{
                    int length;
                    memcpy(&length, (char*)conditionData+1, 4);
                    val.resize(length);
                    memcpy((char*)val.data(), (char*)conditionData+5, length);
                    string valueStr;
                    int valueLength;
                    memcpy(&valueLength, (char*)value, sizeof(int));
                    valueStr.resize(valueLength);
                    memcpy((char*)valueStr.data(), (char*)value+4, valueLength);
                    if(val > valueStr){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else{
                delete[] record;
                free(conditionData);
                return -1;
            }
        } else if(compOp==CompOp::GE_OP){
            if(recordDescriptor[i].type==0){
                int val;
                if(nullInd != 0){
                    continue;
                } else{
                    memcpy(&val, (char*)conditionData+1, 4);
                    if(val >= *(int*)(value)){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else if(recordDescriptor[i].type==1){
                float val;
                if(nullInd != 0){
                    continue;
                } else{
                    memcpy(&val, (char*)conditionData+1, 4);
                    if(val >= *(float*)(value)){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else if(recordDescriptor[i].type==2){
                string val;
                if(nullInd != 0){
                    continue;
                } else{
                    int length;
                    memcpy(&length, (char*)conditionData+1, 4);
                    val.resize(length);
                    memcpy((char*)val.data(), (char*)conditionData+5, length);
                    string valueStr;
                    int valueLength;
                    memcpy(&valueLength, (char*)value, sizeof(int));
                    valueStr.resize(valueLength);
                    memcpy((char*)valueStr.data(), (char*)value+4, valueLength);
                    if(val >= valueStr){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else{
                delete[] record;
                free(conditionData);
                return -1;
            }
        } else if(compOp==CompOp::NE_OP){
            if(recordDescriptor[i].type==0){
                int val;
                if(nullInd != 0){
                    continue;
                } else{
                    memcpy(&val, (char*)conditionData+1, 4);
                    if(val != *(int*)(value)){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else if(recordDescriptor[i].type==1){
                float val;
                if(nullInd != 0){
                    continue;
                } else{
                    memcpy(&val, (char*)conditionData+1, 4);
                    if(val != *(float*)(value)){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            } else if(recordDescriptor[i].type==2){
                string val;
                if(nullInd != 0){
                    continue;
                } else{
                    int length;
                    memcpy(&length, (char*)conditionData+1, 4);
                    val.resize(length);
                    memcpy((char*)val.data(), (char*)conditionData+5, length);
                    string valueStr;
                    int valueLength;
                    memcpy(&valueLength, (char*)value, sizeof(int));
                    valueStr.resize(valueLength);
                    memcpy((char*)valueStr.data(), (char*)value+4, valueLength);
                    if(val != valueStr){
                        RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
                        if(rc != SUCCESS){
                            delete[] record;
                            free(conditionData);
                            return rc;
                        }
                        delete[] record;
                        free(conditionData);
                        return 0;
                    } else{
                        continue;
                    }
                }
            }else{
                delete[] record;
                free(conditionData);
                return -1;
            }
        } else if(compOp == CompOp::NO_OP){
            RC rc = rbfm->concatData(record, recordDescriptor, attributeNames, data);
            if(rc != SUCCESS){
                delete[] record;
                free(conditionData);
                return rc;
            }
            //record2data((void*)record, recordDescriptor, data);
            //rbfm->printRecord(recordDescriptor, data);
            delete[] record;
            return SUCCESS;
        } else{
            delete[] record;
            free(conditionData);
            return -1;
        }
        delete[] record;
    }
    

    free(conditionData);
    return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    rbfm_ScanIterator.fileHandle = &fileHandle;
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.conditionAttribute = conditionAttribute;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = (void *)value;
    rbfm_ScanIterator.attributeNames = attributeNames;
    rbfm_ScanIterator.rbfm = RecordBasedFileManager::instance();
    rbfm_ScanIterator.numPages = rbfm_ScanIterator.fileHandle->getNumberOfPages();
    rbfm_ScanIterator.nextRid.pageNum = 0;
    rbfm_ScanIterator.loadedPage = malloc(PAGE_SIZE); //TODO: free when close()
    RC rc = rbfm_ScanIterator.fileHandle->readPage(rbfm_ScanIterator.nextRid.pageNum, rbfm_ScanIterator.loadedPage);
    if (rc != SUCCESS) {
        return rc;
    }
    rbfm_ScanIterator.numSlots = rbfm_ScanIterator.rbfm->getNumSlots(rbfm_ScanIterator.loadedPage);
    rbfm_ScanIterator.nextRid.slotNum = 1;
    return 0;
}

RC RecordBasedFileManager::insertPos(FileHandle &fileHandle, unsigned short length, RID &rid) {
    int curPage = fileHandle.getNumberOfPages() - 1;
    void *data = malloc(PAGE_SIZE);
    int pageNum;
    short numSlots;
    
    for (pageNum = curPage; pageNum >= 0; pageNum--)
    {
        RC rc = fileHandle.readPage(pageNum, data);
        if (rc != SUCCESS) {
            free(data);
            return rc;
        }
        if (freeSpace(data) >= length + sizeof(SlotDir)) {
            numSlots = getNumSlots(data);
            break;
        }
    }

    if (pageNum < 0) {
        pageNum = curPage + 1;
        numSlots = 0;
        if (PAGE_SIZE < length + sizeof(SlotDir)) {
            free(data);
            return -1; // the record is too long to fit in an empty page.
        }
    }
    
    SlotDir slotDir;
    
    for(int i = 1; i <= numSlots; i++)
    {
        slotDir = getSlotDir(i, data);
        
        if (slotDir.offset == USHRT_MAX) {
            rid.slotNum = i;
            rid.pageNum = pageNum;
            free(data);
            return 0;
        }
    }

    rid.slotNum = ++numSlots;
    rid.pageNum = pageNum;

    free(data);
    return 0;
}

short RecordBasedFileManager::getFreeBegin(const void* page) {
    short freeBegin;
    memcpy(&freeBegin, (char *)page + PAGE_SIZE - sizeof(short), sizeof(short));
    return freeBegin;
}

short RecordBasedFileManager::getNumSlots(const void* page) {
    short numSlots;
    memcpy(&numSlots, (char *)page + PAGE_SIZE - 2 * sizeof(short), sizeof(short));
    return numSlots;
}

unsigned RecordBasedFileManager::freeSpace(const void *data) {
    short numSlots = getNumSlots(data);
    short freeBegin = getFreeBegin(data);
    int freeEnd = PAGE_SIZE - 2 * sizeof(short) - numSlots * sizeof(SlotDir);
    return freeEnd - freeBegin;
}

void RecordBasedFileManager::setFreeBegin(unsigned short freeBegin, void* page) {
    memcpy((char *)page + PAGE_SIZE - sizeof(short), &freeBegin, sizeof(short));
    return;
}

void RecordBasedFileManager::setNumSlots(unsigned short numSlots, void* page) {
    memcpy((char *)page + PAGE_SIZE - 2 * sizeof(short), &numSlots, sizeof(short));
    return;
}

void RecordBasedFileManager::insert2data(void *data, char *record, unsigned short length, unsigned slotNum) {
    unsigned short freeBegin;
    memcpy(&freeBegin, (char *)data + PAGE_SIZE - sizeof(short), sizeof(short));
    // Insert SoltDir.
    SlotDir slotDir = {false, freeBegin, length};
    setSlotDir(data, slotNum, slotDir);
    // Insert record.
    setRecord(data, record, slotDir);
    // Update free space.
    freeBegin += length;
    setFreeBegin(freeBegin, data);
    // Update num of slots.    
    short numSlots = getNumSlots(data);
    if ((unsigned short)numSlots == slotNum - 1) {
        setNumSlots(slotNum, data);
    }

    return;
}
