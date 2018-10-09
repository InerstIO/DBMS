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
	   return FILE_EXISTED;
    } else{
	   ofstream outfile (fileName);
       char* firstPage = new char[PAGE_SIZE];
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
	   fileHandle.filefs.open(fileName);
	   if(fileHandle.filefs.is_open()){
            char* firstPage = new char[PAGE_SIZE];
            fileHandle.filefs.read(firstPage, PAGE_SIZE);
            memcpy(&(fileHandle.readPageCounter), firstPage, sizeof(int));
            memcpy(&(fileHandle.writePageCounter), firstPage+4, sizeof(int));
            memcpy(&(fileHandle.appendPageCounter), firstPage+8, sizeof(int));
            delete[] firstPage;
	        rc = SUCCESS;
	   } else{
	        rc = OPEN_HANDLE_FAIL;
	   }
    return rc;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    char* firstPage = new char[PAGE_SIZE];
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
    pageNum = pageNum+1;
    RC rc = -1;
    //if(sizeof(*(char*)data) == PAGE_SIZE){
        if(pageNum <= getNumberOfPages()){
            filefs.seekg(pageNum*PAGE_SIZE, filefs.beg);
            filefs.read((char*)data, PAGE_SIZE);
            if(filefs){
                rc = SUCCESS;
                readPageCounter++;
                char* firstPage = new char[PAGE_SIZE];
                memcpy(firstPage, &(readPageCounter), sizeof(int));
                memcpy(firstPage+4, &(writePageCounter), sizeof(int));
                memcpy(firstPage+8, &(appendPageCounter), sizeof(int));
                filefs.seekg(0, filefs.beg);
                filefs.write(firstPage, PAGE_SIZE);
                filefs.flush();
            } else{
                rc = READ_PAGE_FAIL;
            }
        } else{
            rc = PAGENUM_EXCEED;
        }
    //} else{
    //    rc = WRONG_DATA_SIZE;
    //}
    return rc;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    RC rc = -1;
    pageNum += 1;
    //if(sizeof(*(char*)data) == PAGE_SIZE){
        if(pageNum <= getNumberOfPages()){
            filefs.seekg(pageNum*PAGE_SIZE, filefs.beg);
            filefs.write((char*)data, PAGE_SIZE);
            if(filefs){
                rc = SUCCESS;
                writePageCounter++;
                char* firstPage = new char[PAGE_SIZE];
                memcpy(firstPage, &(readPageCounter), sizeof(int));
                memcpy(firstPage+4, &(writePageCounter), sizeof(int));
                memcpy(firstPage+8, &(appendPageCounter), sizeof(int));
                filefs.seekg(0, filefs.beg);
                filefs.write(firstPage, PAGE_SIZE);
                filefs.flush();
            } else{
                rc = WRITE_PAGE_FAIL;
            }
        } else{
            rc = PAGENUM_EXCEED;
        }
    //} else{
      //  rc = WRONG_DATA_SIZE;
    //}
    return rc;
}


RC FileHandle::appendPage(const void *data)
{
    RC rc = -1;
    //cout<<sizeof((char*)data)<<endl;
    //if(sizeof(*(char*)data) == PAGE_SIZE){
        //cout<<"hyx1"<<endl;
        filefs.seekg((appendPageCounter+1)*PAGE_SIZE, filefs.beg);
        filefs.write((char*)data, PAGE_SIZE);
        //cout<<"hyx2"<<endl;
        rc = SUCCESS;
    //} else{
      //  rc = WRONG_DATA_SIZE;
    //}
    appendPageCounter++;
    char* firstPage = new char[PAGE_SIZE];
    memcpy(firstPage, &(readPageCounter), sizeof(int));
    memcpy(firstPage+4, &(writePageCounter), sizeof(int));
    memcpy(firstPage+8, &(appendPageCounter), sizeof(int));
    filefs.seekg(0, filefs.beg);
    filefs.write(firstPage, PAGE_SIZE);
    filefs.flush();
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
