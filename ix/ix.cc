
#include "ix.h"
#include <stack>
#include <vector>
#include <string.h>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    ifstream ifile(fileName);
    if (ifile) {
        ifile.close();
       return FILE_EXISTED;
    } else{
       ofstream outfile (fileName);
       char* firstPage = new char[PAGE_SIZE];
       memset((void*)firstPage,0,PAGE_SIZE);
       outfile.write(firstPage, PAGE_SIZE);
       outfile.flush();
       outfile.close();
       delete[] firstPage;
       return SUCCESS;
    }
    return -1;
}

RC IndexManager::destroyFile(const string &fileName)
{
    RC rc = -1;
    if( std::remove(fileName.c_str()) == 0){
        rc = SUCCESS;
    } else{
       rc= FILE_DELETE_FAIL;
    }
    return rc;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    RC rc = -1;
    if(ixfileHandle.fileHandle.filefs.is_open()){
        rc = -1;
    } else{
       ixfileHandle.fileHandle.filefs.open(fileName);
       if(ixfileHandle.fileHandle.filefs.is_open()){
            char* firstPage = new char[PAGE_SIZE];
            memset(firstPage,0,PAGE_SIZE);
            ixfileHandle.fileHandle.filefs.read(firstPage, PAGE_SIZE);
            memcpy(&(ixfileHandle.fileHandle.readPageCounter), firstPage, sizeof(int));
            memcpy(&(ixfileHandle.fileHandle.writePageCounter), firstPage+4, sizeof(int));
            memcpy(&(ixfileHandle.fileHandle.appendPageCounter), firstPage+8, sizeof(int));
            memcpy(&(ixfileHandle.rootNodePointer), firstPage+12, sizeof(int));
            delete[] firstPage;
            rc = SUCCESS;
       } else{
            rc = OPEN_HANDLE_FAIL;
       }
   }
       //cout<<fileHandle.appendPageCounter<<endl;
    return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    char* firstPage = new char[PAGE_SIZE];
    memset(firstPage,0,PAGE_SIZE);
    memcpy(firstPage, &(ixfileHandle.fileHandle.readPageCounter), sizeof(int));
    memcpy(firstPage+4, &(ixfileHandle.fileHandle.writePageCounter), sizeof(int));
    memcpy(firstPage+8, &(ixfileHandle.fileHandle.appendPageCounter), sizeof(int));
    memcpy(firstPage+12, &(ixfileHandle.rootNodePointer), sizeof(int));
    ixfileHandle.fileHandle.filefs.seekg(0, ixfileHandle.fileHandle.filefs.beg);
    ixfileHandle.fileHandle.filefs.write(firstPage, PAGE_SIZE);
    ixfileHandle.fileHandle.filefs.flush();
    ixfileHandle.fileHandle.filefs.close();
    delete[] firstPage;
    return SUCCESS;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void* retKey;
    int retPageId1=0;
    int retPageId2=0;
    //cout<<"rootNodePointer: "<<ixfileHandle.rootNodePointer<<endl;
    RID retRid;
    //cout<<*(int*)key<<": "<<ixfileHandle.rootNodePointer<<endl;
    return insertEntryHelper(attribute, ixfileHandle, key, rid, ixfileHandle.rootNodePointer, retPageId1, retKey, retRid, retPageId2);
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    int pageId = ixfileHandle.rootNodePointer;
    int offset = 5;
    findVictimLeafKey(ixfileHandle, pageId, offset, attribute, key, rid);
    void* page = malloc(PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(pageId-1, page);
    if(attribute.type == TypeInt){
        int victimKey;
        RID victimRid;
        memcpy(&victimKey, (char*)page+offset, sizeof(int));
        memcpy(&victimRid.pageNum, (char*)page+offset+sizeof(int), sizeof(unsigned));
        memcpy(&victimRid.slotNum, (char*)page+offset+sizeof(int)+sizeof(unsigned), sizeof(unsigned));
        int itemSize = sizeof(int)+2*sizeof(unsigned);
        cout<<victimKey<<", "<<victimRid.pageNum<<", "<<victimRid.slotNum<<endl;
        if(victimKey==*(int*)key && rid.pageNum==victimRid.pageNum && rid.slotNum==victimRid.slotNum){
            void* newPage = malloc(PAGE_SIZE);
            memset(newPage,0,PAGE_SIZE);
            memcpy((char*)newPage, (char*)page, offset);
            memcpy((char*)newPage+offset, (char*)page+offset+itemSize, PAGE_SIZE-offset-itemSize);
            ixfileHandle.fileHandle.writePage(pageId-1, newPage);
            free(newPage);
        } else{
            free(page);
            return -1;
        }
    } else if(attribute.type == TypeReal){
        float victimKey;
        RID victimRid;
        memcpy(&victimKey, (char*)page+offset, sizeof(int));
        memcpy(&victimRid.pageNum, (char*)page+offset+sizeof(float), sizeof(unsigned));
        memcpy(&victimRid.slotNum, (char*)page+offset+sizeof(float)+sizeof(unsigned), sizeof(unsigned));
        int itemSize = sizeof(float)+2*sizeof(unsigned);
        if(victimKey==*(float*)key && rid.pageNum==victimRid.pageNum && rid.slotNum==victimRid.slotNum){
            void* newPage = malloc(PAGE_SIZE);
            memset(newPage,0,PAGE_SIZE);
            memcpy((char*)newPage, (char*)page, offset);
            memcpy((char*)newPage+offset, (char*)page+offset+itemSize, PAGE_SIZE-offset-itemSize);
            ixfileHandle.fileHandle.writePage(pageId-1, newPage);
            free(newPage);
        } else{
            free(page);
            return -1;
        }
    } else if(attribute.type == TypeVarChar){
        int keySize, victimKeySize;
        RID victimRid;
        memcpy((char*)(&keySize), (char*)key, sizeof(int));
        memcpy((char*)(&victimKeySize), (char*)page+offset, sizeof(int));
        char* keyptr = new char[keySize+1];
        char* vicKeyptr = new char[victimKeySize+1];
        memcpy(keyptr, (char*)key+sizeof(int), keySize);
        memcpy(vicKeyptr, (char*)page+offset+sizeof(int), victimKeySize);
        memcpy(&victimRid.pageNum, (char*)page+offset+sizeof(int)+victimKeySize, sizeof(unsigned));
        memcpy(&victimRid.slotNum, (char*)page+offset+sizeof(int)+victimKeySize+sizeof(unsigned), sizeof(unsigned));
        keyptr[keySize]='\0';
        vicKeyptr[victimKeySize]='\0';
        string keystr(keyptr);
        string vickeystr(vicKeyptr);
        delete[](keyptr);
        delete[](vicKeyptr);
        if(keystr==vickeystr && rid.pageNum==victimRid.pageNum && rid.slotNum==victimRid.slotNum){
            int itemSize = sizeof(int)+2*sizeof(unsigned)+victimKeySize;
            void* newPage = malloc(PAGE_SIZE);
            memset(newPage,0,PAGE_SIZE);
            memcpy((char*)newPage, (char*)page, offset);
            memcpy((char*)newPage+offset, (char*)page+offset+itemSize, PAGE_SIZE-offset-itemSize);
            ixfileHandle.fileHandle.writePage(pageId-1, newPage);
            free(newPage);
        } else{
            free(page);
            return -1;
        }
    } else{
        free(page);
        return -1;
    }
    free(page);
    return SUCCESS;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    if(!ixfileHandle.fileHandle.filefs.is_open()) return -1;
    ix_ScanIterator.ixfileHandle = &ixfileHandle;
    ix_ScanIterator.type = attribute.type;
    ix_ScanIterator.lowKey = lowKey;
    ix_ScanIterator.highKey = highKey;
    ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
    ix_ScanIterator.highKeyInclusive = highKeyInclusive;
    ix_ScanIterator.loadedPage = malloc(PAGE_SIZE);
    RC rc = ixfileHandle.fileHandle.readPage(ixfileHandle.rootNodePointer - 1, ix_ScanIterator.loadedPage);
    if (rc) {
        return -1;
    }
    memcpy(&ix_ScanIterator.space, (char *)ix_ScanIterator.loadedPage+sizeof(bool), sizeof(int));
    ix_ScanIterator.offset = sizeof(bool) + sizeof(int);
    // find starting page, space and offset
    rc = findLeafKey(ixfileHandle, ix_ScanIterator);
    if (rc) {
        return -1;
    }
    return 0;
}

RC IndexManager::findVictimLeafKey(IXFileHandle &ixfileHandle, int& pageId, int& offset, const Attribute& attribute, const void* victimKey, 
        const RID& victimRid) {
    void* page = malloc(PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(pageId-1, page);
    bool isLeaf = isLeafPage(page);
    free(page);
    while (!isLeaf) {
        RC rc = findVictimKey(ixfileHandle, pageId, offset, attribute, true, victimKey, victimRid);
        if (rc) {
            free(page);
            return -1;
        }
        void* page = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.readPage(pageId-1, page);
        isLeaf = isLeafPage(page);
        free(page);
    }

    RC rc = findVictimKey(ixfileHandle, pageId, offset, attribute, true, victimKey, victimRid);
    if (rc) {
        free(page);
        return -1;
    }
    return 0;
}

RC IndexManager::findVictimKey(IXFileHandle &ixfileHandle, int& pageId, int& offset, const Attribute& attribute, bool isLeaf, 
        const void* victimKey, const RID& victimRid) {
    int type = attribute.type;
    bool isSmaller = false;
    int prevPagePointer;
    void* key;
    int length;
    RID rid;
    RC rc;
    void* page = malloc(PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(pageId-1, page);
    int space;
    memcpy(&space, (char*)page+1, sizeof(int));

    if (isLeaf) {
        offset += sizeof(int);
    }
    while (offset < space - (int)sizeof(int)) {
        if (!isLeaf) {
            memcpy(&prevPagePointer, (char *)page+offset, sizeof(int));
            offset += sizeof(int);
        }
        switch (type)
        {
            case TypeInt:
                key = malloc(sizeof(int));
                memcpy(key, (char *)page+offset, sizeof(int));
                memcpy(&rid.pageNum, (char*)page+offset+sizeof(int), sizeof(unsigned));
                memcpy(&rid.slotNum, (char*)page+offset+sizeof(int)+sizeof(unsigned), sizeof(unsigned));
                rc = keyCompare(isSmaller, attribute, victimKey, key, victimRid, rid);
                if (rc) {
                    free(page);
                    free(key);
                    return -1;
                }
                free(key);
                break;
            case TypeReal:
                key = malloc(sizeof(float));
                memcpy(key, (char *)page+offset, sizeof(float));
                memcpy(&rid.pageNum, (char*)page+offset+sizeof(float), sizeof(unsigned));
                memcpy(&rid.slotNum, (char*)page+offset+sizeof(float)+sizeof(unsigned), sizeof(unsigned));
                rc = keyCompare(isSmaller, attribute, victimKey, key, victimRid, rid);
                if (rc) {
                    free(key);
                    free(page);
                    return -1;
                }
                free(key);
                break;
            case TypeVarChar:
                memcpy(&length, (char *)page+offset, sizeof(int));
                key = malloc(sizeof(length));
                memcpy(&rid.pageNum, (char*)page+offset+length, sizeof(unsigned));
                memcpy(&rid.slotNum, (char*)page+offset+length+sizeof(unsigned), sizeof(unsigned));
                rc = keyCompare(isSmaller, attribute, victimKey, key, victimRid, rid);
                if (rc) {
                    free(key);
                    free(page);
                    return -1;
                }
                free(key);
                break;
            default:
                break;
        }

        if (isSmaller) {
            break;
        }
        
        switch (type)
        {
            case TypeInt:
                offset += sizeof(int) + sizeof(RID);                
                break;
            case TypeReal:
                offset += sizeof(float) + sizeof(RID);
                break;
            case TypeVarChar:
                offset += sizeof(int) + length + sizeof(RID);
                break;
            default:
                break;
        }
    }
    if (!isLeaf) {
        if (!isSmaller) {
            memcpy(&prevPagePointer, (char *)page+offset, sizeof(int));
            offset += sizeof(int);
        }
        ixfileHandle.fileHandle.readPage(prevPagePointer-1, page);
        offset = sizeof(bool) + sizeof(int);
        pageId = prevPagePointer;
        memcpy(&space, (char *)page+sizeof(bool), sizeof(int));
    }
    free(page);
    return 0;
}

RC IndexManager::findLeafKey(IXFileHandle &ixfileHandle, IX_ScanIterator &ix_ScanIterator) {
    bool isLeaf = isLeafPage(ix_ScanIterator.loadedPage);
    while (!isLeaf) {
        RC rc = findKey(ixfileHandle, ix_ScanIterator, isLeaf);
        if (rc) {
            return -1;
        }
        isLeaf = isLeafPage(ix_ScanIterator.loadedPage);
    }
    RC rc = findKey(ixfileHandle, ix_ScanIterator, true);
    if (rc) {
        return -1;
    }
    return 0;
}

RC IndexManager::findKey(IXFileHandle &ixfileHandle, IX_ScanIterator &ix_ScanIterator, bool isLeaf) {
    int type = ix_ScanIterator.type;
    bool isSmaller = false;
    int prevPagePointer;
    void* key;
    int length;
    RC rc;

    if (isLeaf) {
        ix_ScanIterator.offset += sizeof(int);
    }
    while (ix_ScanIterator.offset < ix_ScanIterator.space - (int)sizeof(int)) {
        if (!isLeaf) {
            memcpy(&prevPagePointer, (char *)ix_ScanIterator.loadedPage+ix_ScanIterator.offset, sizeof(int));
            ix_ScanIterator.offset += sizeof(int);
        }
        switch (type)
        {
            case TypeInt:
                key = malloc(sizeof(int));
                memcpy(key, (char *)ix_ScanIterator.loadedPage+ix_ScanIterator.offset, sizeof(int));
                rc = ix_ScanIterator.compare(isSmaller, type, ix_ScanIterator.lowKey, key, ix_ScanIterator.lowKeyInclusive);
                if (rc) {
                    free(key);
                    return -1;
                }
                free(key);
                break;
            case TypeReal:
                key = malloc(sizeof(float));
                memcpy(key, (char *)ix_ScanIterator.loadedPage+ix_ScanIterator.offset, sizeof(float));
                rc = ix_ScanIterator.compare(isSmaller, type, ix_ScanIterator.lowKey, key, ix_ScanIterator.lowKeyInclusive);
                if (rc) {
                    free(key);
                    return -1;
                }
                free(key);
                break;
            case TypeVarChar:
                memcpy(&length, (char *)ix_ScanIterator.loadedPage+ix_ScanIterator.offset, sizeof(int));
                ix_ScanIterator.offset += sizeof(int);
                key = malloc(sizeof(length));
                memcpy(key, (char *)ix_ScanIterator.loadedPage+ix_ScanIterator.offset+sizeof(int), length);
                rc = ix_ScanIterator.compare(isSmaller, type, ix_ScanIterator.lowKey, key, ix_ScanIterator.lowKeyInclusive);
                if (rc) {
                    free(key);
                    return -1;
                }
                free(key);
                break;
            default:
                break;
        }

        if (isSmaller) {
            break;
        }
        
        switch (type)
        {
            case TypeInt:
                ix_ScanIterator.offset += sizeof(int) + sizeof(RID);                
                break;
            case TypeReal:
                ix_ScanIterator.offset += sizeof(float) + sizeof(RID);
                break;
            case TypeVarChar:
                ix_ScanIterator.offset += sizeof(int) + length + sizeof(RID);
                break;
            default:
                break;
        }
    }
    if (!isLeaf) {
        if (!isSmaller) {
            memcpy(&prevPagePointer, (char *)ix_ScanIterator.loadedPage+ix_ScanIterator.offset, sizeof(int));
            ix_ScanIterator.offset += sizeof(int);
        }
        ixfileHandle.fileHandle.readPage(prevPagePointer-1, ix_ScanIterator.loadedPage);
        ix_ScanIterator.offset = sizeof(bool) + sizeof(int);
        memcpy(&ix_ScanIterator.space, (char *)ix_ScanIterator.loadedPage+sizeof(bool), sizeof(int));
    }
    return 0;
}

bool IndexManager::isLeafPage(void* page) {
    bool isLeaf;
    memcpy(&isLeaf, page, sizeof(bool));
    return isLeaf;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    unsigned int rootNodePointer;
    rootNodePointer = ixfileHandle.rootNodePointer;
    //cout<<"rootNodePointer: "<<rootNodePointer<<endl;
    if (rootNodePointer <= 0) {
        cout<<"Failed to get root node."<<endl;
    }
    //cout << "{" << endl;
    dfsPrint(ixfileHandle, attribute, rootNodePointer, 0, true);
    //cout << "}" << endl;
}

void IndexManager::dfsPrint(IXFileHandle &ixfileHandle, const Attribute &attribute, unsigned pageId, int depth, bool last) const {
    void* page = malloc(PAGE_SIZE);
    int type = attribute.type;
    ixfileHandle.fileHandle.readPage(pageId - 1, page); //-1????????????????
    bool isLeaf;
    memcpy(&isLeaf, page, sizeof(bool));
    int space;
    memcpy(&space, (char *)page+sizeof(bool), sizeof(int));
    int offset = sizeof(bool) + sizeof(int);
    vector<int> pageVector;
    unsigned childPageId;

    printTabs(depth);
    cout << "{\"keys\":[";

    if (!isLeaf) {
        
        switch (type)
        {
            case TypeInt:
            {
                vector<int> keyVector;
                while (offset < space - (int)sizeof(int)) { //need to - sizeof(int) to handle the last pointer separately?????????
                    memcpy(&childPageId, (char *)page+offset, sizeof(int));
                    offset += sizeof(int);
                    pageVector.push_back(childPageId);
                    int k;
                    memcpy(&k, (char *)page+offset, sizeof(int));
                    offset += sizeof(int);
                    keyVector.push_back(k);
                    offset += sizeof(RID);
                }
                memcpy(&childPageId, (char *)page+offset, sizeof(int));
                pageVector.push_back(childPageId);
                if (!keyVector.empty()) {
                    for (unsigned i=0; i<keyVector.size() - 1; i++) {
                        cout << "\"" << keyVector.at(i) << "\",";
                    }
                    cout << "\"" << keyVector.at(keyVector.size() - 1) << "\"]," << endl;
                }
                break;
            }
            case TypeReal:
            {
                vector<float> keyVector;
                while (offset < space - (int)sizeof(int)) {
                    memcpy(&childPageId, (char *)page+offset, sizeof(int));
                    offset += sizeof(int);
                    pageVector.push_back(childPageId);
                    float k;
                    memcpy(&k, (char *)page+offset, sizeof(float));
                    offset += sizeof(float);
                    keyVector.push_back(k);
                    offset += sizeof(RID);
                }
                memcpy(&childPageId, (char *)page+offset, sizeof(int));
                pageVector.push_back(childPageId);
                if (!keyVector.empty()) {
                    for (unsigned i=0; i<keyVector.size() - 1; i++) {
                        cout << "\"" << keyVector.at(i) << "\",";
                    }
                    cout << "\"" << keyVector.at(keyVector.size() - 1) << "\"]," << endl;
                }
                break;
            }
            case TypeVarChar:
            {
                vector<char*> keyVector;
                while (offset < space - (int)sizeof(int)) {
                    memcpy(&childPageId, (char *)page+offset, sizeof(int));
                    offset += sizeof(int);
                    pageVector.push_back(childPageId);
                    int length;
                    memcpy(&length, (char *)page+offset, sizeof(int));
                    offset += sizeof(int);
                    void* k = malloc(length);
                    memcpy(k, (char *)page+offset, length);
                    offset += length;
                    keyVector.push_back((char *)k);
                    offset += sizeof(RID);
                    free(k);
                }
                memcpy(&childPageId, (char *)page+offset, sizeof(int));
                pageVector.push_back(childPageId);
                if (!keyVector.empty()) {
                    for (unsigned i=0; i<keyVector.size() - 1; i++) {
                        cout << "\"" << keyVector.at(i) << "\",";
                    }
                    cout << "\"" << keyVector.at(keyVector.size() - 1) << "\"]," << endl;
                }
                break;
            }

            default:
                break;
        }

        printTabs(depth);
        cout << "\"children\":[" << endl;
        
        for(unsigned i = 0; i < pageVector.size() - 1; i++)
        {
            dfsPrint(ixfileHandle, attribute, pageVector.at(i), depth + 1, false);
        }
        dfsPrint(ixfileHandle, attribute, pageVector.at(pageVector.size() - 1), depth + 1, true);
        printTabs(depth);
        cout << "]}";
        if (!last) {
            cout << ",";
        }
        cout << endl;
    }
    else {
        vector<RID> RIDVector;
        RID rid;
        offset += sizeof(int);
        switch (type)
        {
            case TypeInt:
            {
                vector<int> keyVector;
                while (offset < space) {
                    int k;
                    memcpy(&k, (char *)page+offset, sizeof(int));
                    offset += sizeof(int);
                    keyVector.push_back(k);
                    memcpy(&rid, (char *)page+offset, sizeof(RID));
                    offset += sizeof(RID);
                    RIDVector.push_back(rid);
                }
                
                if (!keyVector.empty()) {
                    for (unsigned i=0; i<keyVector.size() - 1; i++) {
                        cout << "\"" << keyVector.at(i) << ":";
                        cout << "(" << RIDVector.at(i).pageNum << ", " << RIDVector.at(i).slotNum << ")\",";
                    }
                    cout << "\"" << keyVector.at(keyVector.size() - 1) << ":";
                    cout << "(" << RIDVector.at(keyVector.size() - 1).pageNum << ", " << RIDVector.at(keyVector.size() - 1).slotNum << ")\"";
                }
                break;
            }
            case TypeReal:
            {
                vector<float> keyVector;
                while (offset < space) {
                    float k;
                    memcpy(&k, (char *)page+offset, sizeof(float));
                    offset += sizeof(float);
                    keyVector.push_back(k);
                    memcpy(&rid, (char *)page+offset, sizeof(RID));
                    offset += sizeof(RID);
                    RIDVector.push_back(rid);
                }
                
                if (!keyVector.empty()) {
                    for (unsigned i=0; i<keyVector.size() - 1; i++) {
                        cout << "\"" << keyVector.at(i) << ":";
                        cout << "(" << RIDVector.at(i).pageNum << ", " << RIDVector.at(i).slotNum << ")\",";
                    }
                    cout << "\"" << keyVector.at(keyVector.size() - 1) << ":";
                    cout << "(" << RIDVector.at(keyVector.size() - 1).pageNum << ", " << RIDVector.at(keyVector.size() - 1).slotNum << ")\"";
                }
                break;
            }
            case TypeVarChar:
            {
                vector<char*> keyVector;
                while (offset < space) {
                    int length;
                    memcpy(&length, (char *)page+offset, sizeof(int));
                    offset += sizeof(int);
                    void* k = malloc(length);
                    memcpy(k, (char *)page+offset, length);
                    offset += length;
                    keyVector.push_back((char *)k);
                    memcpy(&rid, (char *)page+offset, sizeof(RID));
                    offset += sizeof(RID);
                    RIDVector.push_back(rid);
                }
                
                if (!keyVector.empty()) {
                    for (unsigned i=0; i<keyVector.size() - 1; i++) {
                        cout << "\"" << keyVector.at(i) << ":";
                        cout << "(" << RIDVector.at(i).pageNum << ", " << RIDVector.at(i).slotNum << ")\",";
                    }
                    cout << "\"" << keyVector.at(keyVector.size() - 1) << ":";
                    cout << "(" << RIDVector.at(keyVector.size() - 1).pageNum << ", " << RIDVector.at(keyVector.size() - 1).slotNum << ")\"";
                }
                break;
            }
            default:
                break;
        }
        cout << "]}";
        if (!last) {
            cout << ",";
        }
        cout << endl;
    }
    free(page);
}

void IndexManager::printTabs(int num) const {
    for(int i = 0; i < num; i++)
    {
        cout << '\t';
    }
}

RC IndexManager::insertEntryHelper(const Attribute &attribute, IXFileHandle &ixfileHandle, const void* key, const RID &rid, int curPageId, 
            int &retPageId1, void* retKey, RID &retRid, int &retPageId2){
    //cout<<"insert entry: "<<curPageId<<endl;
    int curRetPageId1 = 0;
    int  curRetPageId2 = 0;
    RID curRetRid;
    void* curRetKeyPtr;
    if(attribute.type == 0 || attribute.type == 1){
        curRetKeyPtr = malloc(4);
    } else if(attribute.type == 2){
        curRetKeyPtr = malloc(4+attribute.length);
    } else{
        retPageId1 = 0;
        retPageId2 = 0;
        free(curRetKeyPtr);
        return -1;
    }
    if(ixfileHandle.rootNodePointer == 0){
        int newPageId;
        RC rc = createNewPage(true, ixfileHandle, newPageId);
        ixfileHandle.rootNodePointer = newPageId;
        if(rc != SUCCESS) return rc;
        //rc = insertLeaf(ixfileHandle, newPageId, attribute, key, rid);
        //if(rc != SUCCESS) return rc;
        retPageId1 = 0;
        retPageId2 = 0;
        insertEntryHelper(attribute, ixfileHandle, key, rid, newPageId, curRetPageId1, curRetKeyPtr, curRetRid, curRetPageId2);
    } else{
        void* page = malloc(PAGE_SIZE);
        memset((char*)page,0,PAGE_SIZE);
        ixfileHandle.fileHandle.readPage(curPageId-1, page);
        bool isLeaf;
        int sp;
        memcpy((char*)(&sp), (char*)page+1, sizeof(int));
        memcpy((char*)(&isLeaf), (char*)page, sizeof(bool));
        if(isLeaf){
            int insertSize, pushupKeySize, keySize;
            if(attribute.type == 0){
                insertSize = sizeof(int)+2*sizeof(unsigned);
                pushupKeySize = sizeof(int);
                keySize = sizeof(int);
            } else if(attribute.type == 1){
                insertSize = sizeof(float)+2*sizeof(unsigned);
                pushupKeySize = sizeof(float);
                keySize = sizeof(float);
            } else if(attribute.type == 2){
                memcpy((char*)(&keySize), (char*)key, sizeof(int));
                keySize += 4;
                //cout<<"keysize: "<<keySize<<endl;
                insertSize = keySize+2*sizeof(unsigned);
                pushupKeySize = attribute.length+sizeof(int);
            } else{
                retPageId1 = 0;
                retPageId2 = 0;
                free(curRetKeyPtr);
                free(page);
                return -1;
            }
                int space;
                memcpy((char*)(&space), (char*)page+1, sizeof(int));
                if(space+insertSize <= PAGE_SIZE){
                    //cout<<"start insert"<<endl;
                    insertLeaf(ixfileHandle, curPageId, attribute, key, rid);
                    //cout<<"end insert"<<endl;
                    retPageId1 = 0;
                    retPageId2 = 0;
                } else{
                    int newPageId;
                    void* pushupKey = malloc(pushupKeySize);
                    memset((char*)pushupKey, 0, pushupKeySize);
                    RID pushupRid;
                    splitLeafPage(ixfileHandle, curPageId, newPageId, pushupKey, pushupRid, attribute);
                    bool isSmall;
                    RC rc = keyCompare(isSmall, attribute, key, pushupKey, rid, pushupRid);
                    if(ixfileHandle.rootNodePointer == curPageId){
                        if(isSmall){
                            insertLeaf(ixfileHandle, curPageId, attribute, key, rid);
                        } else{
                            insertLeaf(ixfileHandle, newPageId, attribute, key, rid);
                        }
                        int newRootPageId;
                        createNewPage(false, ixfileHandle, newRootPageId);
                        ixfileHandle.rootNodePointer = newRootPageId;
                        void* rootPage = malloc(PAGE_SIZE);
                        int realPushupKeySize = 0;
                        if(attribute.type == 2){
                            memcpy((char*)(&realPushupKeySize), (char*)pushupKey, sizeof(int));
                        }
                        realPushupKeySize += 4;
                        //cout<<"real pushup key size: "<<realPushupKeySize<<", "<<pushupRid.pageNum<<", "<<pushupRid.slotNum<<endl;
                        int size = 21+realPushupKeySize;
                        memcpy((char*)rootPage+1, &size, sizeof(int));
                        memcpy((char*)rootPage+5, &curPageId, sizeof(int));
                        memcpy((char*)rootPage+9, pushupKey, realPushupKeySize);
                        memcpy((char*)rootPage+9+realPushupKeySize, &(pushupRid.pageNum), sizeof(unsigned));
                        memcpy((char*)rootPage+13+realPushupKeySize, &(pushupRid.slotNum), sizeof(unsigned));
                        memcpy((char*)rootPage+17+realPushupKeySize, &(newPageId), sizeof(int));
                        ixfileHandle.fileHandle.writePage(newRootPageId-1, rootPage);
                        free(rootPage);
                        return SUCCESS;
                    }
                    if(isSmall){
                        insertLeaf(ixfileHandle, curPageId, attribute, key, rid);
                    } else{
                        insertLeaf(ixfileHandle, newPageId, attribute, key, rid);
                    }
                    memcpy((char*)retKey, (char*)pushupKey, pushupKeySize);
                    free(pushupKey);
                    retRid = pushupRid;
                    retPageId1 = curPageId;
                    retPageId2 = newPageId;
                }
        } else{
                //find pageid for next layer
                int offset = 5;
                int space;
                memcpy((char*)(&space), (char*)page+1, sizeof(int));
                bool hasInserted = false;
                //offset may points to the last pageid
                int rightPage = 0;
                while(offset < space-4){
                    int page1, page2;
                    RID r;
                    memcpy(&page1, (char*)page+offset, sizeof(int));
                    int ksize;
                    if(attribute.type == 0 || attribute.type == 1){
                        ksize = 4;
                    } else if(attribute.type == 2){
                        memcpy((char*)(&ksize), (char*)page+offset+4, sizeof(int));
                        ksize += 4;
                    } else{
                        retPageId1 = 0;
                        retPageId2 = 0;
                        free(curRetKeyPtr);
                        free(page);
                        return -1;
                    }
                    void* kptr = malloc(ksize);
                    memset((char*)kptr, 0, ksize);
                    memcpy((char*)kptr, (char*)page+offset+4, ksize);
                    memcpy(&(r.pageNum), (char*)page+offset+4+ksize, sizeof(unsigned));
                    memcpy(&(r.slotNum), (char*)page+offset+8+ksize, sizeof(unsigned));
                    memcpy(&page2, (char*)page+offset+12+ksize, sizeof(int));
                    rightPage = page2;
                    offset += ksize+sizeof(int)+2*sizeof(unsigned);
                    bool isSmall;
                    RC rc = keyCompare(isSmall, attribute, kptr, key, r, rid);
                    free(kptr);
                    if(!isSmall){
                        insertEntryHelper(attribute, ixfileHandle, key, rid, page1, curRetPageId1, curRetKeyPtr, curRetRid, curRetPageId2);
                        hasInserted = true;
                        break;
                    } 
                }
                if(!hasInserted && rightPage>0){
                    insertEntryHelper(attribute, ixfileHandle, key, rid, rightPage, curRetPageId1, curRetKeyPtr, curRetRid, curRetPageId2);
                }
        }
        free(page);
    }

    if(curRetPageId1>0 && curRetPageId2>0){
        bool isRootLeaf = false;
        void* rootPage = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.readPage(ixfileHandle.rootNodePointer-1, rootPage);
        memcpy(&isRootLeaf, (char*)rootPage, sizeof(bool));
        free(rootPage);
        if(isRootLeaf){
            int newRootPageId = 0;
            createNewPage(false, ixfileHandle, newRootPageId);
            ixfileHandle.rootNodePointer = newRootPageId;
            insertInternalNode(ixfileHandle, newRootPageId, attribute, curRetKeyPtr, curRetRid, curRetPageId1, curRetPageId2);
        } else{
            void* page = malloc(PAGE_SIZE);
            ixfileHandle.fileHandle.readPage(curPageId-1, page);
            int space;
            memcpy(&space, (char*)page+1, sizeof(int));
            int itemSize = sizeof(int)+2*sizeof(unsigned);
            if(attribute.type==0 || attribute.type==1){
                itemSize += 4;
            } else{
                itemSize += attribute.length;
            }
            if(space+itemSize > PAGE_SIZE){
                int newPageId = 0;
                splitInternalPage(ixfileHandle, curPageId, newPageId, curRetKeyPtr, curRetRid, attribute);
                if(curPageId == ixfileHandle.rootNodePointer){
                    int newRootPageId;
                    createNewPage(false, ixfileHandle, newRootPageId);
                    insertInternalNode(ixfileHandle, newRootPageId, attribute, curRetKeyPtr, curRetRid, curPageId, newPageId);
                    ixfileHandle.rootNodePointer = newRootPageId;
                } else{
                    retPageId1 = curPageId;
                    retPageId2 = newPageId;
                    retKey = curRetKeyPtr;
                    retRid = curRetRid;
                }
            } else{
                insertInternalNode(ixfileHandle, curPageId, attribute, curRetKeyPtr, curRetRid, curRetPageId1, curRetPageId2);
                retKey = NULL;
                retPageId1=0;
                retPageId2=0;
            }
            free(page);
        }
    }
    free(curRetKeyPtr);
    return SUCCESS;
}

RC IndexManager::createNewPage(bool isLeaf, IXFileHandle &ixfileHandle, int &newPageId){
    char* newPage = (char*)malloc(PAGE_SIZE);
    memset(newPage, 0, PAGE_SIZE);
    memcpy(newPage, (char*)&isLeaf, sizeof(bool));
    int size;
    if(isLeaf){
        size = 2*sizeof(int)+sizeof(bool);
    } else{
        size = sizeof(int)+sizeof(bool);
    }
    memcpy(newPage+1, &size, sizeof(int));
    int nextPtr = 0;
    memcpy(newPage+5, &nextPtr, sizeof(int));
    ixfileHandle.fileHandle.appendPage(newPage);
    newPageId = ixfileHandle.fileHandle.appendPageCounter;
    free(newPage);
    return SUCCESS;
}
        
RC IndexManager::keyCompare(bool &res, const Attribute &attribute, const void* key1, const void* key2, const RID &rid1, const RID &rid2){
    if(attribute.type == 0){
        int k1;
        int k2;
        memcpy((char*)(&k1), (char*)key1, sizeof(int));
        memcpy((char*)(&k2), (char*)key2, sizeof(int));
        if(k1 != k2){
            res = k1<k2;
        } else{
            if(rid1.pageNum != rid2.pageNum){
                res = rid1.pageNum < rid2.pageNum;
            } else{
                res = rid1.slotNum <= rid2.slotNum;
            }
        }
    } else if(attribute.type==1){
        float k1, k2;
        memcpy((char*)(&k1), (char*)key1, sizeof(float));
        memcpy((char*)(&k2), (char*)key2, sizeof(float));
        if(k1 != k2){
            res = k1<k2;
        } else{
            if(rid1.pageNum != rid2.pageNum){
                res = rid1.pageNum < rid2.pageNum;
            } else{
                res = rid1.slotNum <= rid2.slotNum;
            }
        }
    } else if(attribute.type == 2){
        int size1, size2;
        //cout<<"before compare"<<endl;
        memcpy((char*)(&size1), (char*)key1, sizeof(int));
        memcpy((char*)(&size2), (char*)key2, sizeof(int));
        char* k1ptr = new char[size1+1];
        char* k2ptr = new char[size2+1];
        //cout<<size1<<", "<<size2<<endl;
        memcpy(k1ptr, (char*)key1+sizeof(int), size1);
        memcpy(k2ptr, (char*)key2+sizeof(int), size2);
        k1ptr[size1]='\0';
        k2ptr[size2]='\0';
        string k1(k1ptr);
        string k2(k2ptr);
        //cout<<k1<<", "<<k2<<endl;
        delete[](k1ptr);
        delete[](k2ptr);
        if(k1 != k2){
            res = k1<k2;
        } else{
            if(rid1.pageNum != rid2.pageNum){
                res = rid1.pageNum < rid2.pageNum;
            } else{
                res = rid1.slotNum <= rid2.slotNum;
            }
        }
    } else{
        res = true;
        return -1;
    }
    return SUCCESS;
}

RC IndexManager::insertInternalNode(IXFileHandle& ixfileHandle, int pageId, const Attribute &attribute, const void* key, 
    const RID &rid, int page1, int page2){
    //cout<<"insertInternalNode"<<endl;
    if(attribute.type == 0){
        void* page = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.readPage(pageId-1, page);
        void* newPage = malloc(PAGE_SIZE);
        int space;
        memcpy((char*)newPage, (char*)page, sizeof(bool));
        memcpy((char*)(&space), (char*)page+1, sizeof(int));
        //first item to insert in this page, insert one key and two page ids
        if(space == 5){
            int offset = 5;
            memcpy((char*)newPage+offset, (char*)(&page1), sizeof(int));
            offset += sizeof(int);
            memcpy((char*)newPage+offset, (char*)(key), sizeof(int));
            offset += sizeof(int);
            memcpy((char*)newPage+offset, (char*)(&(rid.pageNum)), sizeof(unsigned));
            offset += sizeof(unsigned);
            memcpy((char*)newPage+offset, (char*)(&(rid.slotNum)), sizeof(unsigned));
            offset += sizeof(unsigned);
            memcpy((char*)newPage+offset, (char*)(&page2), sizeof(int));
            offset += sizeof(int);
            memcpy((char*)newPage+1, (char*)(&offset), sizeof(int));
        } else{
            //if it is not first item to insert, insert one key and one page id
            int offset = 5;
            int newOffset = offset;
            bool hasInserted = false;
            bool isFirst = true;
            while(offset<space-sizeof(int)){
                //cout<<"while: "<<offset<<", "<<space<<endl;
                int beginPageId;
                int pageId;
                if(!hasInserted){
                    int keyReaderPtr = offset+sizeof(int);
                    int pageNum, slotNum;
                    void* k = malloc(sizeof(int));
                    //cout<<"key offset: "<<keyReaderPtr<<endl;
                    memcpy((char*)(k), (char*)page+keyReaderPtr, sizeof(int));
                    //offset += sizeof(int);
                    keyReaderPtr += sizeof(int);
                    memcpy((char*)(&pageNum), (char*)page+keyReaderPtr, sizeof(int));
                    //offset += sizeof(int);
                    keyReaderPtr += sizeof(int);
                    memcpy((char*)(&slotNum), (char*)page+keyReaderPtr, sizeof(int));
                    //offset += sizeof(int);
                    keyReaderPtr += sizeof(int);
                    bool isSmall;
                    RID r;
                    r.pageNum = pageNum;
                    r.slotNum = slotNum;
                    RC rc = keyCompare(isSmall, attribute, key, k, rid, r);
                    //cout<<"isSmall: "<<isSmall<<", "<<*(int*)key<<", "<<*(int*)k<<endl;
                    free(k);
                    if(rc!=SUCCESS) return -1;
                    if(isSmall){
                        if(isFirst){
                            //put page1, new key, then everything from old page
                            memcpy((char*)newPage+newOffset, (char*)(&page1), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)(key), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
                            newOffset += sizeof(unsigned);
                            memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
                            newOffset += sizeof(unsigned);
                            memcpy((char*)newPage+newOffset, (char*)(&page2), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)page+9, space-9);
                            //set space for new page
                            int newSpace = space+2*sizeof(int)+2*sizeof(unsigned);
                            memcpy((char*)newPage+1, &newSpace, sizeof(int));
                            offset = space;
                            hasInserted = true;
                        } else{
                            //put page1, new key, then page2, then everything from old page
                            memcpy((char*)newPage+newOffset, (char*)(&page1), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)(key), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
                            newOffset += sizeof(unsigned);
                            memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
                            newOffset += sizeof(unsigned);
                            memcpy((char*)newPage+newOffset, (char*)(&page2), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)page+offset+sizeof(int), space-offset-sizeof(int));
                            //set space for new page
                            int newSpace = space+2*sizeof(int)+2*sizeof(unsigned);
                            memcpy((char*)newPage+1, &newSpace, sizeof(int));
                            offset = space;
                            hasInserted = true;
                        }
                    } else{
                        int size = 2*sizeof(int)+2*sizeof(unsigned);
                        //cout<<"append: "<<size<<", "<<newOffset<<", "<<offset<<endl;
                        memcpy((char*)newPage+newOffset, (char*)page+offset, size);
                        offset += size;
                        newOffset += size;
                    }
                } else{
                    ;
                }
                isFirst = false;
            }
            if(!hasInserted){
                //cout<<"insertlast"<<endl;
                memcpy((char*)newPage+newOffset, (char*)page+offset, sizeof(int));
                newOffset += sizeof(int);
                memcpy((char*)newPage+newOffset, (char*)key, sizeof(int));
                newOffset += sizeof(int);
                memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
                newOffset += sizeof(unsigned);
                memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
                newOffset += sizeof(unsigned);
                memcpy((char*)newPage+newOffset, (char*)(&page2), sizeof(int));
                newOffset += sizeof(int);
                memcpy((char*)newPage+1, (char*)(&newOffset), sizeof(int));
            }
        }
        ixfileHandle.fileHandle.writePage(pageId-1, newPage);
        free(page);
        free(newPage);
    //handle float
    } else if(attribute.type == 1){
        void* page = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.readPage(pageId-1, page);
        void* newPage = malloc(PAGE_SIZE);
        int space;
        memcpy((char*)newPage, (char*)page, sizeof(bool));
        memcpy((char*)(&space), (char*)page+1, sizeof(int));
        //first item to insert in this page, insert one key and two page ids
        if(space == 5){
            int offset = 5;
            memcpy((char*)newPage+offset, (char*)(&page1), sizeof(int));
            offset += sizeof(int);
            memcpy((char*)newPage+offset, (char*)(key), sizeof(float));
            offset += sizeof(int);
            memcpy((char*)newPage+offset, (char*)(&(rid.pageNum)), sizeof(unsigned));
            offset += sizeof(unsigned);
            memcpy((char*)newPage+offset, (char*)(&(rid.slotNum)), sizeof(unsigned));
            offset += sizeof(unsigned);
            memcpy((char*)newPage+offset, (char*)(&page2), sizeof(int));
            offset += sizeof(int);
            memcpy((char*)newPage+1, (char*)(&offset), sizeof(int));
        } else{
            //if it is not first item to insert, insert one key and one page id
            int offset = 5;
            int newOffset = offset;
            bool hasInserted = false;
            bool isFirst = true;
            while(offset<space-sizeof(int)){
                //cout<<"while: "<<offset<<", "<<space<<endl;
                int beginPageId;
                int pageId;
                if(!hasInserted){
                    int keyReaderPtr = offset+sizeof(int);
                    int pageNum, slotNum;
                    void* k = malloc(sizeof(float));
                    memcpy((char*)(k), (char*)page+keyReaderPtr, sizeof(float));
                    keyReaderPtr += sizeof(float);
                    memcpy((char*)(&pageNum), (char*)page+keyReaderPtr, sizeof(int));
                    keyReaderPtr += sizeof(int);
                    memcpy((char*)(&slotNum), (char*)page+keyReaderPtr, sizeof(int));
                    keyReaderPtr += sizeof(int);
                    bool isSmall;
                    RID r;
                    r.pageNum = pageNum;
                    r.slotNum = slotNum;
                    RC rc = keyCompare(isSmall, attribute, key, k, rid, r);
                    free(k);
                    if(rc!=SUCCESS) return -1;
                    if(isSmall){
                        if(isFirst){
                            memcpy((char*)newPage+newOffset, (char*)(&page1), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)(key), sizeof(float));
                            newOffset += sizeof(float);
                            memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
                            newOffset += sizeof(unsigned);
                            memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
                            newOffset += sizeof(unsigned);
                            memcpy((char*)newPage+newOffset, (char*)(&page2), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)page+9, space-9);
                            //set space for new page
                            int newSpace = space+2*sizeof(int)+2*sizeof(unsigned);
                            memcpy((char*)newPage+1, &newSpace, sizeof(int));
                            offset = space;
                            hasInserted = true;
                        } else{
                            //put page1, new key, then page2, then everything from old page
                            memcpy((char*)newPage+newOffset, (char*)(&page1), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)(key), sizeof(float));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
                            newOffset += sizeof(unsigned);
                            memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
                            newOffset += sizeof(unsigned);
                            memcpy((char*)newPage+newOffset, (char*)(&page2), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)page+offset+sizeof(int), space-offset-sizeof(int));
                            //set space for new page, keysize + pageid size + rid size;
                            int newSpace = space+2*sizeof(int)+2*sizeof(unsigned);
                            memcpy((char*)newPage+1, &newSpace, sizeof(int));
                            offset = space;
                            hasInserted = true;
                        }
                    } else{
                        //move the currrent item
                        int size = 2*sizeof(int)+2*sizeof(unsigned);
                        memcpy((char*)newPage+newOffset, (char*)page+offset, size);
                        offset += size;
                        newOffset += size;
                    }
                } else{
                    ;
                }
                isFirst = false;
            }
            if(!hasInserted){
                //cout<<"insertlast"<<endl;
                memcpy((char*)newPage+newOffset, (char*)page+offset, sizeof(int));
                newOffset += sizeof(int);
                memcpy((char*)newPage+newOffset, (char*)key, sizeof(float));
                newOffset += sizeof(float);
                memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
                newOffset += sizeof(unsigned);
                memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
                newOffset += sizeof(unsigned);
                memcpy((char*)newPage+newOffset, (char*)(&page2), sizeof(int));
                newOffset += sizeof(int);
                memcpy((char*)newPage+1, (char*)(&newOffset), sizeof(int));
            }
        }
        ixfileHandle.fileHandle.writePage(pageId-1, newPage);
        free(page);
        free(newPage);
    } else if(attribute.type == 2){
        void* page = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.readPage(pageId-1, page);
        void* newPage = malloc(PAGE_SIZE);
        int space;
        memcpy((char*)newPage, (char*)page, sizeof(bool));
        memcpy((char*)(&space), (char*)page+1, sizeof(int));
        int keySize;
        memcpy((char*)(&keySize), (char*)key, sizeof(int));
        keySize += sizeof(int);
        //first item to insert in this page, insert one key and two page ids
        if(space == 5){
            int offset = 5;
            memcpy((char*)newPage+offset, (char*)(&page1), sizeof(int));
            offset += sizeof(int);
            memcpy((char*)newPage+offset, (char*)(key), sizeof(keySize));
            offset += keySize;
            memcpy((char*)newPage+offset, (char*)(&(rid.pageNum)), sizeof(unsigned));
            offset += sizeof(unsigned);
            memcpy((char*)newPage+offset, (char*)(&(rid.slotNum)), sizeof(unsigned));
            offset += sizeof(unsigned);
            memcpy((char*)newPage+offset, (char*)(&page2), sizeof(int));
            offset += sizeof(int);
            memcpy((char*)newPage+1, (char*)(&offset), sizeof(int));
        } else{
            //if it is not first item to insert, insert one key and one page id
            int offset = 5;
            int newOffset = offset;
            bool hasInserted = false;
            bool isFirst = true;
            while(offset<space-sizeof(int)){
                //cout<<"offset, page: "<<offset<<", "<<space<<endl;
                int beginPageId;
                int pageId;
                if(!hasInserted){
                    int keyReaderPtr = offset+sizeof(int);
                    int ksize;
                    int pageNum, slotNum;
                    memcpy((char*)(&ksize), (char*)page+keyReaderPtr, sizeof(int));
                    ksize += sizeof(int);
                    //cout<<"ksize: "<<ksize<<endl;
                    void* k = malloc(ksize);
                    memcpy((char*)(k), (char*)page+keyReaderPtr, ksize);
                    keyReaderPtr += ksize;
                    memcpy((char*)(&pageNum), (char*)page+keyReaderPtr, sizeof(int));
                    keyReaderPtr += sizeof(int);
                    memcpy((char*)(&slotNum), (char*)page+keyReaderPtr, sizeof(int));
                    keyReaderPtr += sizeof(int);
                    bool isSmall;
                    RID r;
                    r.pageNum = pageNum;
                    r.slotNum = slotNum;
                    //cout<<"before key compare"<<endl;
        /*int size1, size2;
        memcpy((char*)(&size1), (char*)key, sizeof(int));
        memcpy((char*)(&size2), (char*)k, sizeof(int));
        char* k1ptr = new char[size1+1];
        char* k2ptr = new char[size2+1];
        cout<<size1<<", "<<size2<<endl;
        memcpy(k1ptr, (char*)key+sizeof(int), size1);
        memcpy(k2ptr, (char*)k+sizeof(int), size2);
        k1ptr[size1]='\0';
        k2ptr[size2]='\0';
        string k1(k1ptr);
        string k2(k2ptr);
        cout<<k1<<", "<<k2<<endl;
        free(k1ptr);
        free(k2ptr);*/
                    RC rc = keyCompare(isSmall, attribute, key, k, rid, r);
                    //cout<<"issmall: "<<isSmall<<endl;
                    free(k);
                    if(rc!=SUCCESS) return -1;
                    if(isSmall){
                        //cout<<"isSmall"<<endl;
                        if(isFirst){
                           // cout<<"first"<<endl;
                            memcpy((char*)newPage+newOffset, (char*)(&page1), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)(key), keySize);
                            newOffset += keySize;
                            memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
                            newOffset += sizeof(unsigned);
                            memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
                            newOffset += sizeof(unsigned);
                            memcpy((char*)newPage+newOffset, (char*)(&page2), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)page+9, space-9);
                            //set space for new page
                            int newSpace = space+sizeof(int)+2*sizeof(unsigned)+keySize;
                            memcpy((char*)newPage+1, &newSpace, sizeof(int));
                            offset = space;
                            hasInserted = true;
                        } else{
                            //cout<<"not first"<<endl;
                            //put page1, new key, then page2, then everything from old page
                            memcpy((char*)newPage+newOffset, (char*)(&page1), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)(key), keySize);
                            newOffset += keySize;
                            memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
                            newOffset += sizeof(unsigned);
                            memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
                            newOffset += sizeof(unsigned);
                            memcpy((char*)newPage+newOffset, (char*)(&page2), sizeof(int));
                            newOffset += sizeof(int);
                            memcpy((char*)newPage+newOffset, (char*)page+offset+sizeof(int), space-offset-sizeof(int));
                            //set space for new page
                            int newSpace = space+sizeof(int)+2*sizeof(unsigned)+keySize;
                            memcpy((char*)newPage+1, &newSpace, sizeof(int));
                            offset = space;
                            hasInserted = true;
                            break;
                        }
                    } else{
                        int size = sizeof(int)+2*sizeof(unsigned)+ksize;
                        //cout<<"offset change: "<<size<<", "<<newOffset<<", "<<offset<<endl;
                        memcpy((char*)newPage+newOffset, (char*)page+offset, size);
                        offset += size;
                        newOffset += size;
                    }
                } else{
                    break;
                }
                isFirst = false;
            }
            if(!hasInserted){
                //cout<<"insertlast"<<endl;
                //cout<<offset<<", "<<newOffset<<endl;
                //newOffset -= sizeof(int);
                //offset -= sizeof(int);
                memcpy((char*)newPage+newOffset, (char*)page+offset, sizeof(int));
                newOffset += sizeof(int);
                memcpy((char*)newPage+newOffset, (char*)key, keySize);
                newOffset += keySize;
                memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
                newOffset += sizeof(unsigned);
                memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
                newOffset += sizeof(unsigned);
                memcpy((char*)newPage+newOffset, (char*)(&page2), sizeof(int));
                newOffset += sizeof(int);
                memcpy((char*)newPage+1, (char*)(&newOffset), sizeof(int));
            }
        }
        ixfileHandle.fileHandle.writePage(pageId-1, newPage);
        free(page);
        free(newPage);
    }else{
        return -1;
    }
    //cout<<"finish insert internal"<<endl;
    return SUCCESS;
}

RC IndexManager::insertLeaf(IXFileHandle &ixfileHandle, int pageId, const Attribute &attribute, const void* key, const RID &rid){
    //cout<<"insertLeaf: "<<rid.slotNum<<endl;
    if(attribute.type == 0){
        void* page = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.readPage(pageId-1, page);
        void* newPage = malloc(PAGE_SIZE);
        memset((char*)newPage, 0, PAGE_SIZE);
        int space;
        memcpy(&space, (char*)page+1, sizeof(int));
        int offset = 9;
        int newOffset = offset;
        memcpy((char*)(newPage), (char*)page, 2*sizeof(int)+sizeof(bool));
        bool hasInserted = false;
        while(offset < space){
            void* k = malloc(sizeof(int));
            memcpy((char*)k, (char*)page+offset, sizeof(int));
            RID r;
            memcpy(&(r.pageNum), (char*)page+offset+sizeof(int), sizeof(unsigned));
            memcpy(&(r.slotNum), (char*)page+offset+sizeof(int)+sizeof(unsigned), sizeof(unsigned));
            bool isSmall;
            RC rc = keyCompare(isSmall, attribute, key, k, rid, r);
            free(k);
            if(!rc==SUCCESS)   return -1;
            if(isSmall && !hasInserted){
                memcpy((char*)newPage+newOffset, (char*)key, sizeof(int));
                newOffset += sizeof(int);
                memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
                newOffset += sizeof(unsigned);
                memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
                newOffset += sizeof(unsigned);
                hasInserted = true;
            } else{
                int itemSize = sizeof(int)+2*sizeof(unsigned);
                memcpy((char*)newPage+newOffset, (char*)page+offset, itemSize);
                offset += itemSize;
                newOffset += itemSize;
            }
        }
        if(!hasInserted){
            memcpy((char*)newPage+newOffset, (char*)key, sizeof(int));
            newOffset += sizeof(int);
            memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
            newOffset += sizeof(unsigned);
            memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
            newOffset += sizeof(unsigned);
            hasInserted = true;
        }
        int itemSize = sizeof(int)+2*sizeof(unsigned);
        space += itemSize;
        memcpy((char*)newPage+1, (char*)(&space), sizeof(int));
        int nextpage;
        memcpy((char*)(&nextpage), (char*)newPage+5, sizeof(int));
        ixfileHandle.fileHandle.writePage(pageId-1, newPage);
        free(page);
        free(newPage);
    } else if(attribute.type == 1){
        void* page = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.readPage(pageId-1, page);
        void* newPage = malloc(PAGE_SIZE);
        int space;
        memcpy(&space, (char*)page+1, sizeof(int));
        int offset = 9;
        int newOffset = offset;
        memcpy((char*)(newPage), (char*)page, 2*sizeof(int)+sizeof(bool));
        bool hasInserted = false;
        while(offset < space){
            void* k = malloc(sizeof(float));
            memcpy((char*)k, (char*)page+offset, sizeof(float));
            RID r;
            memcpy(&(r.pageNum), (char*)page+offset+sizeof(float), sizeof(unsigned));
            memcpy(&(r.slotNum), (char*)page+offset+sizeof(float)+sizeof(unsigned), sizeof(unsigned));
            bool isSmall;
            RC rc = keyCompare(isSmall, attribute, key, k, rid, r);
            free(k);
            if(!rc==SUCCESS)   return -1;
            if(isSmall && !hasInserted){
                memcpy((char*)newPage+newOffset, (char*)key, sizeof(float));
                newOffset += sizeof(float);
                memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
                newOffset += sizeof(unsigned);
                memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
                newOffset += sizeof(unsigned);
                hasInserted = true;
            } else{
                int itemSize = sizeof(float)+2*sizeof(unsigned);
                memcpy((char*)newPage+newOffset, (char*)page+offset, itemSize);
                offset += itemSize;
                newOffset += itemSize;
            }
        }
        if(!hasInserted){
            memcpy((char*)newPage+newOffset, (char*)key, sizeof(float));
            newOffset += sizeof(float);
            memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
            newOffset += sizeof(unsigned);
            memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
            newOffset += sizeof(unsigned);
            hasInserted = true;
        }
        int itemSize = sizeof(float)+2*sizeof(unsigned);
        space += itemSize;
        memcpy((char*)newPage+1, (char*)(&space), sizeof(int));
        int nextpage;
        memcpy((char*)(&nextpage), (char*)newPage+5, sizeof(int));
        ixfileHandle.fileHandle.writePage(pageId-1, newPage);
        free(page);
        free(newPage);
    } else if(attribute.type == 2){
        void* page = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.readPage(pageId-1, page);
        void* newPage = malloc(PAGE_SIZE);
        int space;
        memcpy(&space, (char*)page+1, sizeof(int));
        int offset = 9;
        int newOffset = offset;
        memcpy((char*)(newPage), (char*)page, 2*sizeof(int)+sizeof(bool));
        bool hasInserted = false;
        int keySize;
        memcpy((char*)&keySize, (char*)key, sizeof(int));
        keySize += sizeof(int);
        //cout<<"before while"<<endl;
        while(offset < space){
            int ksize;
            memcpy((char*)(&ksize), (char*)page+offset, sizeof(int));
            ksize += 4;
            void* k = malloc(ksize);
            memcpy((char*)k, (char*)page+offset, ksize);
            RID r;
            memcpy(&(r.pageNum), (char*)page+offset+ksize, sizeof(unsigned));
            memcpy(&(r.slotNum), (char*)page+offset+ksize+sizeof(unsigned), sizeof(unsigned));
            bool isSmall;
            //cout<<"before compare"<<endl;
            RC rc = keyCompare(isSmall, attribute, key, k, rid, r);
            //cout<<"end compare: "<<isSmall<<endl;
            free(k);
            if(!rc==SUCCESS)   return -1;
            if(isSmall && !hasInserted){
                memcpy((char*)newPage+newOffset, (char*)key, keySize);
                newOffset += keySize;
                memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
                newOffset += sizeof(unsigned);
                memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
                newOffset += sizeof(unsigned);
                hasInserted = true;
                memcpy((char*)newPage+newOffset, (char*)page+offset, PAGE_SIZE-newOffset);
                break;
            } else if(!isSmall){
                int itemSize = ksize+2*sizeof(unsigned);
                memcpy((char*)newPage+newOffset, (char*)page+offset, itemSize);
                offset += itemSize;
                newOffset += itemSize;
            }
        }
        if(!hasInserted){
            //cout<<"no has inserted keysize: "<<keySize<<", "<<newOffset<<endl;
            char *str = new char[keySize-4+1];
            memcpy(str, (char *)key + 4, keySize-4);
            str[keySize-4] = '\0';
            //cout<<"key: "<<str<<endl;
            memcpy((char*)newPage+newOffset, (char*)key, keySize);
            newOffset += keySize;
            memcpy((char*)newPage+newOffset, (char*)(&rid.pageNum), sizeof(unsigned));
            newOffset += sizeof(unsigned);
            memcpy((char*)newPage+newOffset, (char*)(&rid.slotNum), sizeof(unsigned));
            newOffset += sizeof(unsigned);
            hasInserted = true;
        }
        //cout<<"after nothasinserted"<<endl;
        int itemSize = keySize+2*sizeof(unsigned);
        space += itemSize;
        memcpy((char*)newPage+1, (char*)(&space), sizeof(int));
        int nextpage;// = *(int*)((char*)newPage+5);
        memcpy((char*)(&nextpage), (char*)newPage+5, sizeof(int));
        ixfileHandle.fileHandle.writePage(pageId-1, newPage);
        free(page);
        free(newPage);
    } else{
        return -1;
    }
    return SUCCESS;
}

//input page to split, return newpageid, push-up key, push-up rid
RC IndexManager::splitLeafPage(IXFileHandle& ixfileHandle, int pageId, int &newPageId, void* key, RID& rid, const Attribute &attribute){
    void* page = malloc(PAGE_SIZE);
    void* splitedPage = malloc(PAGE_SIZE);
    void* newPage = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    memset(newPage, 0, PAGE_SIZE);
    memset(splitedPage, 0, PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(pageId-1, page);
    if(attribute.type == 0){
        int offset = 9;
        int splitOffset = 9;
        int newOffset = 9;
        memcpy((char*)newPage, (char*)page, sizeof(bool)+2*sizeof(int));
        memcpy((char*)splitedPage, (char*)page, sizeof(bool)+2*sizeof(int));
        bool isLeaf = true;
        memcpy((char*)newPage, (char*)(&isLeaf), sizeof(bool));
        memcpy((char*)splitedPage, (char*)(&isLeaf), sizeof(bool));
        int itemSize = sizeof(int)+2*sizeof(unsigned);
        while(offset <= PAGE_SIZE/2){
            /*memcpy((char*)key, (char*)page+offset, sizeof(int));
        memcpy((char*)(&rid.pageNum), (char*)page+offset+sizeof(int), sizeof(unsigned));
        memcpy((char*)(&rid.slotNum), (char*)page+offset+sizeof(int)+sizeof(unsigned), sizeof(unsigned));
        cout<<"scan: "<<*(int*)key<<", "<<rid.pageNum<<", "<<rid.slotNum<<", "<<offset<<endl;*/
            memcpy((char*)splitedPage+splitOffset, (char*)page+offset, itemSize);
            offset += itemSize;
            splitOffset += itemSize;
        }
        memcpy((char*)key, (char*)page+offset, sizeof(int));
        memcpy((char*)(&rid.pageNum), (char*)page+offset+sizeof(int), sizeof(unsigned));
        memcpy((char*)(&rid.slotNum), (char*)page+offset+sizeof(int)+sizeof(unsigned), sizeof(unsigned));
        //cout<<"split: "<<*(int*)key<<", "<<rid.pageNum<<", "<<rid.slotNum<<", "<<offset<<endl;
        while(offset+itemSize <= PAGE_SIZE){
            memcpy((char*)newPage+newOffset, (char*)page+offset, itemSize);
            offset += itemSize;
            newOffset += itemSize;
        }
        memcpy((char*)newPage+1, (char*)(&newOffset), sizeof(int));
        memcpy((char*)splitedPage+1, (char*)(&splitOffset), sizeof(int));
        int originNextPtr = 0;
        memcpy((char*)(&originNextPtr), (char*)page+5, sizeof(int));
        memcpy((char*)newPage+5, &originNextPtr, sizeof(int));
        int nextPagePtr = ixfileHandle.fileHandle.appendPageCounter+1;
        memcpy((char*)splitedPage+5, &nextPagePtr, sizeof(int));
        ixfileHandle.fileHandle.writePage(pageId-1, splitedPage);
        ixfileHandle.fileHandle.appendPage(newPage);
        newPageId = ixfileHandle.fileHandle.appendPageCounter;
    } else if(attribute.type == 1){
        int offset = 9;
        int splitOffset = 9;
        int newOffset = 9;
        memcpy((char*)newPage, (char*)page, sizeof(bool)+2*sizeof(int));
        memcpy((char*)splitedPage, (char*)page, sizeof(bool)+2*sizeof(int));
        bool isLeaf = true;
        memcpy((char*)newPage, (char*)(&isLeaf), sizeof(bool));
        memcpy((char*)splitedPage, (char*)(&isLeaf), sizeof(bool));
        int itemSize = sizeof(float)+2*sizeof(unsigned);
        while(offset <= PAGE_SIZE/2){
            memcpy((char*)splitedPage+splitOffset, (char*)page+offset, itemSize);
            offset += itemSize;
            splitOffset += itemSize;
        }
        memcpy((char*)key, (char*)page+offset, sizeof(float));
        memcpy((char*)(&rid.pageNum), (char*)page+offset+sizeof(int), sizeof(unsigned));
        memcpy((char*)(&rid.slotNum), (char*)page+offset+sizeof(int)+sizeof(unsigned), sizeof(unsigned));

        while(offset+itemSize <= PAGE_SIZE){
            memcpy((char*)newPage+newOffset, (char*)page+offset, itemSize);
            offset += itemSize;
            newOffset += itemSize;
        }
        memcpy((char*)newPage+1, (char*)(&newOffset), sizeof(int));
        memcpy((char*)splitedPage+1, (char*)(&splitOffset), sizeof(int));
        int originNextPtr = 0;
        memcpy((char*)(&originNextPtr), (char*)page+5, sizeof(int));
        memcpy((char*)newPage+5, &originNextPtr, sizeof(int));
        int nextPagePtr = ixfileHandle.fileHandle.appendPageCounter+1;
        memcpy((char*)splitedPage+5, &nextPagePtr, sizeof(int));
        ixfileHandle.fileHandle.writePage(pageId-1, splitedPage);
        ixfileHandle.fileHandle.appendPage(newPage);
        newPageId = ixfileHandle.fileHandle.appendPageCounter;
    } else if(attribute.type == 2){
        int offset = 9;
        int splitOffset = 9;
        int newOffset = 9;
        int space;
        memcpy((char*)(&space), (char*)page+1, sizeof(int));
        memcpy((char*)newPage, (char*)page, sizeof(bool)+2*sizeof(int));
        memcpy((char*)splitedPage, (char*)page, sizeof(bool)+2*sizeof(int));
        bool isLeaf = true;
        memcpy((char*)newPage, (char*)(&isLeaf), sizeof(bool));
        memcpy((char*)splitedPage, (char*)(&isLeaf), sizeof(bool));
        while(offset <= space/2){
            int ksize;
            memcpy((char*)(&ksize), (char*)page+offset, sizeof(int));
            ksize += sizeof(int);
            int itemSize = ksize+2*sizeof(unsigned);
            memcpy((char*)splitedPage+splitOffset, (char*)page+offset, itemSize);
            offset += itemSize;
            splitOffset += itemSize;
        }
        int ksize;
        memcpy((char*)(&ksize), (char*)page+offset, sizeof(int));
        ksize += sizeof(int);
        memcpy((char*)key, (char*)page+offset, ksize);
        memcpy((char*)(&rid.pageNum), (char*)page+offset+ksize, sizeof(unsigned));
        memcpy((char*)(&rid.slotNum), (char*)page+offset+ksize+sizeof(unsigned), sizeof(unsigned));

        while(offset < space){
            int ksize;
            memcpy((char*)(&ksize), (char*)page+offset, sizeof(int));
            ksize += sizeof(int);
            int itemSize = ksize+2*sizeof(unsigned);
            memcpy((char*)newPage+newOffset, (char*)page+offset, itemSize);
            offset += itemSize;
            newOffset += itemSize;
        }
        memcpy((char*)newPage+1, (char*)(&newOffset), sizeof(int));
        memcpy((char*)splitedPage+1, (char*)(&splitOffset), sizeof(int));
        int originNextPtr = 0;
        memcpy((char*)(&originNextPtr), (char*)page+5, sizeof(int));
        memcpy((char*)newPage+5, &originNextPtr, sizeof(int));
        int nextPagePtr = ixfileHandle.fileHandle.appendPageCounter+1;
        memcpy((char*)splitedPage+5, &nextPagePtr, sizeof(int));
        ixfileHandle.fileHandle.writePage(pageId-1, splitedPage);
        ixfileHandle.fileHandle.appendPage(newPage);
        newPageId = ixfileHandle.fileHandle.appendPageCounter;
    } else{
        free(page);
        free(splitedPage);
        free(newPage);
        return -1;
    }
    free(page);
    free(splitedPage);
    free(newPage);
    return SUCCESS;
}

//input page to split, return newpageid, push-up key, push-up rid, push-up left page, push-up right page
RC IndexManager::splitInternalPage(IXFileHandle& ixfileHandle, int pageId, int &newPageId, void* key, RID& rid, const Attribute &attribute){
    //cout<<"splitInternalPage"<<endl;
    void* page = malloc(PAGE_SIZE);
    void* splitedPage = malloc(PAGE_SIZE);
    void* newPage = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    memset(newPage, 0, PAGE_SIZE);
    memset(splitedPage, 0, PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(pageId-1, page);
    if(attribute.type == 0){
        int offset = 5;
        int splitOffset = 5;
        int newOffset = 5;
        memcpy((char*)newPage, (char*)page, sizeof(bool)+sizeof(int));
        memcpy((char*)splitedPage, (char*)page, sizeof(bool)+sizeof(int));
        bool isLeaf = false;
        memcpy((char*)newPage, (char*)(&isLeaf), sizeof(bool));
        memcpy((char*)splitedPage, (char*)(&isLeaf), sizeof(bool));
        //key, rid.pagenum, rid.slotnum, pageid
        int itemSize = 2*sizeof(int)+2*sizeof(unsigned);
        //put first pageid
        memcpy((char*)splitedPage+splitOffset, (char*)page+offset, sizeof(int));
        offset += sizeof(int);
        splitOffset += sizeof(int);
        while(offset <= PAGE_SIZE/2){
            memcpy((char*)splitedPage+splitOffset, (char*)page+offset, itemSize);
            offset += itemSize;
            splitOffset += itemSize;
        }
        memcpy((char*)key, (char*)page+offset, sizeof(int));
        memcpy((char*)(&rid.pageNum), (char*)page+offset+sizeof(int), sizeof(unsigned));
        memcpy((char*)(&rid.slotNum), (char*)page+offset+sizeof(int)+sizeof(unsigned), sizeof(unsigned));

        //put first pageid to the new page
        memcpy((char*)newPage+newOffset, (char*)page+offset-sizeof(int), sizeof(int));
        newOffset += sizeof(int);
        while(offset+itemSize <= PAGE_SIZE){
            memcpy((char*)newPage+newOffset, (char*)page+offset, itemSize);
            offset += itemSize;
            newOffset += itemSize;
        }
        memcpy((char*)newPage+1, (char*)(&newOffset), sizeof(int));
        memcpy((char*)splitedPage+1, (char*)(&splitOffset), sizeof(int));
        ixfileHandle.fileHandle.writePage(pageId-1, splitedPage);
        ixfileHandle.fileHandle.appendPage(newPage);
        newPageId = ixfileHandle.fileHandle.appendPageCounter;
    } else if(attribute.type == 1){
        int offset = 5;
        int splitOffset = 5;
        int newOffset = 5;
        memcpy((char*)newPage, (char*)page, sizeof(bool)+sizeof(int));
        memcpy((char*)splitedPage, (char*)page, sizeof(bool)+sizeof(int));
        bool isLeaf = false;
        memcpy((char*)newPage, (char*)(&isLeaf), sizeof(bool));
        memcpy((char*)splitedPage, (char*)(&isLeaf), sizeof(bool));
        //key, rid.pagenum, rid.slotnum, pageid
        int itemSize = sizeof(float)+sizeof(int)+2*sizeof(unsigned);
        //put first pageid
        memcpy((char*)splitedPage+splitOffset, (char*)page+offset, sizeof(int));
        offset += sizeof(int);
        splitOffset += sizeof(int);
        while(offset <= PAGE_SIZE/2){
            memcpy((char*)splitedPage+splitOffset, (char*)page+offset, itemSize);
            offset += itemSize;
            splitOffset += itemSize;
        }
        memcpy((char*)key, (char*)page+offset, sizeof(float));
        memcpy((char*)(&rid.pageNum), (char*)page+offset+sizeof(float), sizeof(unsigned));
        memcpy((char*)(&rid.slotNum), (char*)page+offset+sizeof(float)+sizeof(unsigned), sizeof(unsigned));

        //put first pageid to the new page
        memcpy((char*)newPage+newOffset, (char*)page+offset-sizeof(int), sizeof(int));
        newOffset += sizeof(int);
        while(offset+itemSize <= PAGE_SIZE){
            memcpy((char*)newPage+newOffset, (char*)page+offset, itemSize);
            offset += itemSize;
            newOffset += itemSize;
        }
        memcpy((char*)newPage+1, (char*)(&newOffset), sizeof(int));
        memcpy((char*)splitedPage+1, (char*)(&splitOffset), sizeof(int));
        ixfileHandle.fileHandle.writePage(pageId-1, splitedPage);
        ixfileHandle.fileHandle.appendPage(newPage);
        newPageId = ixfileHandle.fileHandle.appendPageCounter;
    } else if(attribute.type == 2){
        int offset = 5;
        int splitOffset = 5;
        int newOffset = 5;
        int space;
        memcpy((char*)(&space), (char*)page+1, sizeof(int));
        memcpy((char*)newPage, (char*)page, sizeof(bool)+sizeof(int));
        memcpy((char*)splitedPage, (char*)page, sizeof(bool)+sizeof(int));
        bool isLeaf = false;
        memcpy((char*)newPage, (char*)(&isLeaf), sizeof(bool));
        memcpy((char*)splitedPage, (char*)(&isLeaf), sizeof(bool));
        //put first pageid
        memcpy((char*)splitedPage+splitOffset, (char*)page+offset, sizeof(int));
        offset += sizeof(int);
        splitOffset += sizeof(int);
        while(offset <= space/2){
            int ksize;
            memcpy((char*)(&ksize), (char*)page+offset, sizeof(int));
            ksize += sizeof(int);
            int itemSize = ksize+sizeof(int)+2*sizeof(unsigned);
            memcpy((char*)splitedPage+splitOffset, (char*)page+offset, itemSize);
            offset += itemSize;
            splitOffset += itemSize;
        }
        int ksize;
        memcpy((char*)(&ksize), (char*)page+offset, sizeof(int));
        ksize += sizeof(int);
        int itemSize = ksize+sizeof(int)+2*sizeof(unsigned);
        memcpy((char*)key, (char*)page+offset, itemSize);
        memcpy((char*)(&rid.pageNum), (char*)page+offset+sizeof(float), sizeof(unsigned));
        memcpy((char*)(&rid.slotNum), (char*)page+offset+sizeof(float)+sizeof(unsigned), sizeof(unsigned));

        //put first pageid to the new page
        memcpy((char*)newPage+newOffset, (char*)page+offset-sizeof(int), sizeof(int));
        newOffset += sizeof(int);
        while(offset+itemSize < space){
            memcpy((char*)(&ksize), (char*)page+offset, sizeof(int));
            ksize += sizeof(int);
            itemSize = ksize+sizeof(int)+2*sizeof(unsigned);
            memcpy((char*)newPage+newOffset, (char*)page+offset, itemSize);
            offset += itemSize;
            newOffset += itemSize;
        }
        memcpy((char*)newPage+1, (char*)(&newOffset), sizeof(int));
        memcpy((char*)splitedPage+1, (char*)(&splitOffset), sizeof(int));
        ixfileHandle.fileHandle.writePage(pageId-1, splitedPage);
        ixfileHandle.fileHandle.appendPage(newPage);
        newPageId = ixfileHandle.fileHandle.appendPageCounter;
    } else{
        free(page);
        free(splitedPage);
        free(newPage);
        return -1;
    }
    free(page);
    free(splitedPage);
    free(newPage);
    //cout<<"end split internal"<<endl;
    return SUCCESS;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    if (offset > space) {
        return -1;
    }
    else if (offset == space)
    {
        int pageNum;
        memcpy(&pageNum, (char *)loadedPage+sizeof(bool)+sizeof(int), sizeof(int));
        if (pageNum == 0) {
            return IX_EOF;
        }
        ixfileHandle->fileHandle.readPage(pageNum-1, loadedPage);
        offset = sizeof(bool);
        memcpy(&space, (char *)loadedPage+offset, sizeof(int));
        offset += 2 * sizeof(int);
    }
    
    switch (type)
    {
        case TypeInt:
            memcpy(key, (char *)loadedPage+offset, sizeof(int));
            offset += sizeof(int);
            break;
        case TypeReal:
            memcpy(key, (char *)loadedPage+offset, sizeof(float));
            offset += sizeof(float);
            break;
        case TypeVarChar:
            int length;
            memcpy(&length, (char *)loadedPage+offset, sizeof(int));
            offset += sizeof(int);
            memcpy(key, (char *)loadedPage+offset, length);
            offset += length;
            break;
        default:
            break;
    }
    bool isSmaller;
    RC rc = compare(isSmaller, type, key, highKey, highKeyInclusive);
    if (rc) {
        return -1;
    }
    if (!isSmaller) {
        return IX_EOF;
    }
    memcpy(&rid, (char *)loadedPage+offset, sizeof(RID));
    offset += sizeof(RID);
    
    return 0;
}

RC IX_ScanIterator::compare(bool &isSmaller, const int type, const void* key1, const void* key2, const bool inclusive) {
    if (key1 == NULL || key2 == NULL) {
        isSmaller = true;
        return 0;
    }
    switch (type)
    {
        case TypeInt:
        {
            int k1;
            int k2;
            memcpy((char *)(&k1), (char *)key1, sizeof(int));
            memcpy((char *)(&k2), (char *)key2, sizeof(int));
            if (inclusive) {
                isSmaller = k1 <= k2;
            }
            else {
                isSmaller = k1 < k2;
            }
            return 0;
        }
        case TypeReal:
        {
            float k1;
            float k2;
            memcpy((char *)(&k1), (char *)key1, sizeof(float));
            memcpy((char *)(&k2), (char *)key2, sizeof(float));
            if (inclusive) {
                isSmaller = k1 <= k2;
            }
            else {
                isSmaller = k1 < k2;
            }
            return 0;
        }
        case TypeVarChar:
        {
            if (inclusive) {
                isSmaller = strcmp((char *)key1, (char *)key2) <= 0;
            }
            else {
                isSmaller = strcmp((char *)key1, (char *)key2) < 0;
            }
            return 0;
        }
        default:
            break;
    }
    return -1;
}

RC IX_ScanIterator::close()
{
    free(loadedPage);
    return 0;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return this->fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
}

RC IXFileHandle::getRootNodePointer(unsigned &rootNodePointer){
    rootNodePointer = this->rootNodePointer;
    return SUCCESS;
}
