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
                int stringLength = (int)*((char*)data+dataPos)+((int)*((char*)data+dataPos+1)<<8)+((int)*((char*)data+dataPos+2)<<16)+((int)*((char*)data+dataPos+3)<<24);
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
    for(unsigned i=0;i<recordDescriptor.size();i++){
        if(pointers[i] != -1){
        if(recordDescriptor[i].type == 2){
            int l = i>0?pointers[i]-pointers[i-1]:pointers[i];
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
    RC rc = insertPos(fileHandle, length, rid);
    if (rc) {
        return rc;
    }
    void *page = malloc(PAGE_SIZE);
    if (rid.pageNum == fileHandle.getNumberOfPages()) {
        //init the page
        short zero = 0;
        memcpy((char *)page + PAGE_SIZE - sizeof(short), &zero, sizeof(short));
        memcpy((char *)page + PAGE_SIZE - 2 * sizeof(short), &zero, sizeof(short));
        //update the page
        insert2data(page, record, length, rid.slotNum);
        delete[] record;
        //append new page
        int rc = fileHandle.appendPage(page);
        if (rc) {
            return rc;
        }
    }
    else
    {
        int rc = fileHandle.readPage(rid.pageNum, page);
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

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    void *page = malloc(PAGE_SIZE);
    int rc = fileHandle.readPage(rid.pageNum, page);
    if (rc) {
        return rc;
    }
    SlotDir slotDir = getSlotDir(rid.slotNum, page);
    char *record = new char[slotDir.length];
    memcpy((char *)record, (char *)page + slotDir.offset, slotDir.length);
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

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    void *page = malloc(PAGE_SIZE);
    int rc = fileHandle.readPage(rid.pageNum, page);
    if (rc) {
        return rc;
    }
    SlotDir slotDir = getSlotDir(rid.slotNum, page);
    unsigned short recordLength = slotDir.length;
    short freeBegin = getFreeBegin(page);
    short numSlots = getNumSlots(page);
    
    if (slotDir.tombstone) {
        RID realRid;
        memcpy(&realRid, (char *)page + slotDir.offset, recordLength);
        rc = deleteRecord(fileHandle, recordDescriptor, realRid);
        if (rc) {
            return rc;
        }
    }

    memcpy((char *)page + slotDir.offset, (char *)page + slotDir.offset + recordLength, freeBegin - slotDir.offset - recordLength);
    slotDir.offset = -1;
    memcpy((char *)page + PAGE_SIZE - 2 * sizeof(short) - rid.slotNum * sizeof(SlotDir), &slotDir, sizeof(SlotDir));
    
    for(int i = rid.slotNum+1; i <= numSlots; i++)
    {
        slotDir = getSlotDir(i, page);
        slotDir.offset -= recordLength;
        memcpy((char *)page + PAGE_SIZE - 2 * sizeof(short) - i * sizeof(SlotDir), &slotDir, sizeof(SlotDir));
    }
    
    setFreeBegin(freeBegin+recordLength, page);
    // do not update numSlots because we need that unchanged to find insertion position.
    return 0;
}

RC RecordBasedFileManager::insertPos(FileHandle &fileHandle, unsigned short length, RID &rid) {
    int curPage = fileHandle.getNumberOfPages() - 1;
    void *data = malloc(PAGE_SIZE);
    int pageNum;
    
    for (pageNum = curPage; pageNum >= 0; pageNum--)
    {
        int rc = fileHandle.readPage(pageNum, data);
        if (rc) {
            return rc;
        }
        if (freeSpace(data) >= length + sizeof(SlotDir)) {
            break;
        }
    }

    if (pageNum < 0) {
        pageNum = curPage + 1;
        rid.slotNum = 1;
    }
    else {
        memcpy(&rid.slotNum, (char *)data + PAGE_SIZE - 2 * sizeof(short), sizeof(short));
        rid.slotNum++;//TODO: fix slotNum after deletion
    }
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

void RecordBasedFileManager::insert2data(void *data, char *record, unsigned short length, int slotNum) {
    // Insert record.
    unsigned short freeBegin;
    memcpy(&freeBegin, (char *)data + PAGE_SIZE - sizeof(short), sizeof(short));
    memcpy((char *)data + freeBegin, record, length);
    // Insert SoltDir.
    SlotDir slotDir = {false, freeBegin, length};
    memcpy((char *)data + PAGE_SIZE - 2 * sizeof(short) - slotNum * sizeof(SlotDir), &slotDir, sizeof(SlotDir));
    // Update free space.
    freeBegin += length;
    setFreeBegin(freeBegin, data);
    // Update num of slots.
    setNumSlots(slotNum, data); //TODO: fix slotNum
    return;
}
