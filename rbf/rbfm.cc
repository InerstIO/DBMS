#include <string.h>

#include "rbfm.h"

void* data2record(const void* data, const vector<Attribute>& recordDescriptor, int& length);

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return -1;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return -1;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return -1;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return -1;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    int length = 0;
    char *record = (char *)data2record(data, recordDescriptor, length);
    RC rc = insertPos(fileHandle, length, rid);
    if (rc) {
        return rc;
    }
    void *page = malloc(PAGE_SIZE);
    if (rid.pageNum == fileHandle.getNumberOfPages()) {
        //init the page
        int zero = 0;
        memcpy((char *)page + PAGE_SIZE - sizeof(int), &zero, sizeof(int));
        memcpy((char *)page + PAGE_SIZE - 2 * sizeof(int), &zero, sizeof(int));
        //update the page
        insert2data(page, record, length, rid.slotNum);
        //append new page
        int rc = fileHandle.appendPage(page);
        if (rc) {
            return rc;
        }
    }
    else
    {
        fileHandle.readPage(rid.pageNum, page);
        //update the page
        insert2data(page, record, length, rid.slotNum);
        //write page
        int rc = fileHandle.writePage(rid.pageNum, page);
        if (rc) {
            return rc;
        }
    }
    free(page);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}

RC RecordBasedFileManager::insertPos(FileHandle &fileHandle, int length, RID &rid) {
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
        memcpy(&rid.slotNum, (char *)data + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
        rid.slotNum++;
    }
    rid.pageNum = pageNum;

    free(data);
    return 0;
}

unsigned RecordBasedFileManager::freeSpace(const void *data) {
    int numSlots;
    memcpy(&numSlots, (char *)data + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
    int freeBegin;
    memcpy(&freeBegin, (char *)data + PAGE_SIZE - sizeof(int), sizeof(int));
    int freeEnd = PAGE_SIZE - 2 * sizeof(int) - numSlots * sizeof(SlotDir);
    return freeEnd - freeBegin;
}

void RecordBasedFileManager::insert2data(void *data, char *record, int length, int slotNum) {
    // Insert record.
    int freeBegin;
    memcpy(&freeBegin, (char *)data + PAGE_SIZE - sizeof(int), sizeof(int));
    memcpy((char *)data + freeBegin, record, length);
    // Insert SoltDir.
    SlotDir slotDir = {freeBegin, slotNum};
    memcpy((char *)data + PAGE_SIZE - 2 * sizeof(int) - slotNum * sizeof(SlotDir), &slotDir, sizeof(SlotDir));
    // Update free space.
    freeBegin += length;
    memcpy((char *)data + PAGE_SIZE - sizeof(int), &freeBegin, sizeof(int));
    // Update num of slots.
    memcpy((char *)data + PAGE_SIZE - 2 * sizeof(int), &slotNum, sizeof(int));
    return;
}
