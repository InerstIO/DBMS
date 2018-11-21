#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"
using namespace std;

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        RC insertEntryHelper(const Attribute &attribute, IXFileHandle &ixfileHandle, const void* key, const RID &rid, int curPageId, 
            int &retPageId1, void* retKey, RID &retRid, int &retPageId2);
        RC createNewPage(bool isLeaf, IXFileHandle &ixfileHandle, int &newPageId);
        RC keyCompare(bool &res, const Attribute &attribute, const void* key1, const void* key2, const RID &rid1, const RID &rid2);
        RC insertLeaf(IXFileHandle &ixfileHandle, int pageId, const Attribute &attribute, const void* key, const RID &rid);
        RC insertInternalNode(IXFileHandle& ixfileHandle, int pageId, const Attribute &attribute, const void* key, const RID &rid, int page1, int page2);
        //input page to split, return newpageid, push-up key, push-up rid
        RC splitLeafPage(IXFileHandle& ixfileHandle, int pageId1, int &newPageId, void* key, RID& rid, const Attribute &attribute);
        RC splitInternalPage(IXFileHandle& ixfileHandle, int pageId, int &newPageId, void* key, RID& rid, const Attribute &attribute);
        // dfs helper function for tree printing
        void dfsPrint(IXFileHandle &ixfileHandle, const Attribute &attribute, unsigned pageId, int depth, bool last) const;
        // print num of tabs
        void printTabs(int num) const;
        // find the left most offset
        RC findLeafKey(IXFileHandle &ixfileHandle, IX_ScanIterator &ix_ScanIterator);
        // check whether the page is a leaf page
        bool isLeafPage(void* page);
        // find the offset of lowKey and load the next page if current not leaf
        RC findKey(IXFileHandle &ixfileHandle, IX_ScanIterator &ix_ScanIterator, bool isLeaf);
        RC findVictimLeafKey(IXFileHandle &ixfileHandle, int& pageId, int& offset, const Attribute& attribute, const void* victimKey, 
            const RID& victimRid);
        RC findVictimKey(IXFileHandle &ixfileHandle, int& pageId, int& offset, const Attribute& attribute, bool isLeaf, 
            const void* victimKey, const RID& victimRid);
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
        
        IXFileHandle* ixfileHandle;
        int type;
        void* lowKey;
        void* highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        void* loadedPage;
        int offset;
        int space;
        // return true if key1 smaller (or equal, depends on inclusive) than key2
        RC compare(bool &isSmaller, const int type, const void* key1, const void* key2, const bool inclusive);
        // append '\0' to the end of the VarChar
        char* appendNULL(char* str);
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    FileHandle fileHandle;
    unsigned rootNodePointer;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    RC getRootNodePointer(unsigned &rootNodePointer);

};

#endif
