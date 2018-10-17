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
    if (rc) {
        return rc;
    }
    void *page = malloc(PAGE_SIZE);
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
        if (rc) {
            return rc;
        }
    }
    else
    {
        RC rc = fileHandle.readPage(rid.pageNum, page);
        if (rc) {
            return rc;
        }
        //update the page
        insert2data(page, record, length, rid.slotNum);
        delete[] record;
        //write page
        rc = fileHandle.writePage(rid.pageNum, page);
        if (rc) {
            return rc;
        }
    }
    free(page);
    return 0;
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
    RC rc = fileHandle.readPage(rid.pageNum, page);
    if (rc) {
        free(page);
        return rc;
    }
    SlotDir slotDir = getSlotDir(rid.slotNum, page);
    
    if (slotDir.offset == USHRT_MAX) {
        free(page);
        return -1; // deleted record.
    }
    
    char *record = new char[slotDir.length];
    getRecord(record, slotDir, page);
    record2data(record, recordDescriptor, data);
    free(page);
    delete[] record;
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

void RecordBasedFileManager::updateSlotDirOffsets(void* page, unsigned start, short numSlots, short delta) {
    for(int i = start; i <= numSlots; i++)
    {
        SlotDir slotDir = getSlotDir(i, page);
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
        memcpy(&realRid, (char *)page + slotDir.offset, recordLength);
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
    slotDir.offset = USHRT_MAX;
    setSlotDir(page, rid.slotNum, slotDir);
    
    updateSlotDirOffsets(page, rid.slotNum+1, numSlots, -recordLength);
    
    setFreeBegin(freeBegin+recordLength, page);
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
RC RecordBasedFileManager::insertPos(FileHandle &fileHandle, unsigned short length, RID &rid) {
    int curPage = fileHandle.getNumberOfPages() - 1;
    void *data = malloc(PAGE_SIZE);
    int pageNum;
    short numSlots;
    
    for (pageNum = curPage; pageNum >= 0; pageNum--)
    {
        RC rc = fileHandle.readPage(pageNum, data);
        if (rc) {
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
