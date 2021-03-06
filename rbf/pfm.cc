#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}

// The file exists, and is open for input
RC PagedFileManager::createFile(const string &fileName)
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


RC PagedFileManager::destroyFile(const string &fileName)
{
    RC rc = -1;
    if( std::remove(fileName.c_str()) == 0){
        rc = SUCCESS;
    } else{
	   rc= FILE_DELETE_FAIL;
    }
    return rc;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    RC rc = -1;
    if(fileHandle.filefs.is_open()){
        fileHandle.filefs.close();
    }
	   fileHandle.filefs.open(fileName);
	   if(fileHandle.filefs.is_open()){
            char* firstPage = new char[PAGE_SIZE];
            memset(firstPage,0,PAGE_SIZE);
            fileHandle.filefs.read(firstPage, PAGE_SIZE);
            memcpy(&(fileHandle.readPageCounter), firstPage, sizeof(int));
            memcpy(&(fileHandle.writePageCounter), firstPage+4, sizeof(int));
            memcpy(&(fileHandle.appendPageCounter), firstPage+8, sizeof(int));
            delete[] firstPage;
	        rc = SUCCESS;
	   } else{
	        rc = OPEN_HANDLE_FAIL;
	   }
       //cout<<fileHandle.appendPageCounter<<endl;
    return rc;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    char* firstPage = new char[PAGE_SIZE];
    memset(firstPage,0,PAGE_SIZE);
    memcpy(firstPage, &(fileHandle.readPageCounter), sizeof(int));
    memcpy(firstPage+4, &(fileHandle.writePageCounter), sizeof(int));
    memcpy(firstPage+8, &(fileHandle.appendPageCounter), sizeof(int));
    fileHandle.filefs.seekg(0, fileHandle.filefs.beg);
    fileHandle.filefs.write(firstPage, PAGE_SIZE);
    fileHandle.filefs.flush();
    fileHandle.filefs.close();
    delete[] firstPage;
    return SUCCESS;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
    filefs.flush();
    filefs.close();
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    //cout<<"readpage: "<<filefs.is_open()<<endl;
    pageNum = pageNum+1;
    RC rc = -1;
    //cout<<"r1"<<endl;
    //cout<<"readPage: "<<appendPageCounter<<", "<<writePageCounter<<", "<<readPageCounter<<", "<<pageNum<<endl;
    if(pageNum <= getNumberOfPages()){
        //cout<<"r2"<<endl;
        //cout<<filefs.peek()<<endl;
        //cout<<"seekg: "<<pageNum*PAGE_SIZE<<endl;
        filefs.seekg(pageNum*PAGE_SIZE, filefs.beg);
        //cout<<"r"<<endl;
        filefs.read((char*)data, PAGE_SIZE);
        //cout<<"r3"<<endl;
        if(filefs){
            rc = SUCCESS;
            readPageCounter++;
            char* firstPage = new char[PAGE_SIZE];
            memset(firstPage,0,PAGE_SIZE);
            memcpy(firstPage, &(readPageCounter), sizeof(int));
            memcpy(firstPage+4, &(writePageCounter), sizeof(int));
            memcpy(firstPage+8, &(appendPageCounter), sizeof(int));
            filefs.seekg(0, filefs.beg);
            filefs.write(firstPage, PAGE_SIZE);
            filefs.flush();
            delete[] firstPage;
        } else{
            //cout<<"readpage: "<<getNumberOfPages()<<", "<<pageNum<<endl;
            rc = READ_PAGE_FAIL;
        }
    } else{
        rc = PAGENUM_EXCEED;
    }
    return rc;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    RC rc = -1;
    pageNum += 1;
    //cout<<"write page: "<<appendPageCounter<<", "<<writePageCounter<<", "<<readPageCounter<<", "<<pageNum<<endl;
    if(pageNum <= getNumberOfPages()){
        filefs.seekg(pageNum*PAGE_SIZE, filefs.beg);
        filefs.write((char*)data, PAGE_SIZE);
        if(filefs){
            rc = SUCCESS;
            writePageCounter++;
            char* firstPage = new char[PAGE_SIZE];
            memset(firstPage,0,PAGE_SIZE);
            memcpy(firstPage, &(readPageCounter), sizeof(int));
            memcpy(firstPage+4, &(writePageCounter), sizeof(int));
            memcpy(firstPage+8, &(appendPageCounter), sizeof(int));
            filefs.seekg(0, filefs.beg);
            filefs.write(firstPage, PAGE_SIZE);
            filefs.flush();
            delete[] firstPage;
        } else{
            rc = WRITE_PAGE_FAIL;
        }
    } else{
        rc = PAGENUM_EXCEED;
    }
    return rc;
}


RC FileHandle::appendPage(const void *data)
{
    RC rc = -1;
   // cout<<"append page: "<<appendPageCounter<<", "<<writePageCounter<<", "<<readPageCounter<<endl;
    filefs.seekg((appendPageCounter+1)*PAGE_SIZE, filefs.beg);
    filefs.write((char*)data, PAGE_SIZE);
    rc = SUCCESS;
    appendPageCounter++;
    char* firstPage = new char[PAGE_SIZE];
    memset(firstPage,0,PAGE_SIZE);
    memcpy(firstPage, &(readPageCounter), sizeof(int));
    memcpy(firstPage+4, &(writePageCounter), sizeof(int));
    memcpy(firstPage+8, &(appendPageCounter), sizeof(int));
    filefs.seekg(0, filefs.beg);
    filefs.write(firstPage, PAGE_SIZE);
    filefs.flush();
    delete[] firstPage;
    return rc;
}


unsigned FileHandle::getNumberOfPages()
{
    return appendPageCounter;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}
