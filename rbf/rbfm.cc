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
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}

RID RecordBasedFileManager::insertPos(FileHandle &fileHandle, int length) {
    int curPage = fileHandle.getNumberOfPages() - 1;
    RID rid = {};
    void *data = malloc(PAGE_SIZE);
    int pageNum;
    
    for (pageNum = curPage; pageNum >= 0; pageNum--)
    {
        fileHandle.readPage(pageNum, data);
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
    return rid;
}

unsigned RecordBasedFileManager::freeSpace(const void *data) {
    int numSlots;
    memcpy(&numSlots, (char *)data + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
    int freeBegin;
    memcpy(&freeBegin, (char *)data + PAGE_SIZE - sizeof(int), sizeof(int));
    int freeEnd = PAGE_SIZE - 2 * sizeof(int) - numSlots * sizeof(SlotDir);
    return freeEnd - freeBegin;
}
