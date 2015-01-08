
#include "ix.h"

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

RC IndexManager::createFile(const string &fileName, const unsigned &numberOfPages)
{
    return IXFileHandle::createFile(fileName, numberOfPages);
}

RC IndexManager::destroyFile(const string &fileName)
{
    return IXFileHandle::destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
    RC rc = ixFileHandle.openFile(fileName);
    if(rc != SUCCESS)
        return rc;
    ixFileHandle.loadMetadata(g_N_OriginalNumberOfBuckets,g_currentLevel,g_nextSplitPtr,g_bottomLinePtr);
	return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	return ixfileHandle.closeFile();
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    unsigned index = hash(attribute, key);

    //TODO: optimize length
    Entry entry;
    int keyLength = 0;
    if(attribute.type == TypeVarChar){
        keyLength = sizeof(int) + *(int*)key;
    }else
        keyLength = sizeof(int);

    entry.key= malloc(keyLength);
    memcpy( entry.key, key, keyLength);
    entry.rid = rid;
    ixfileHandle.insertManagement( attribute, index , entry );

//    char *tmp = Varchart2CString(entry.key);
//    cout<<"<<"<<tmp<<">>"<<endl;

    free(entry.key);

    // Update Metadata
    this->g_currentLevel = ixfileHandle.g_currentLevel;
    this->g_nextSplitPtr = ixfileHandle.g_nextSplitPtr;
    this->g_bottomLinePtr = ixfileHandle.g_bottomLinePtr;

	return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    unsigned index = hash(attribute, key);

    Entry entry;
    int keyLength;
    if(attribute.type == TypeVarChar){
        keyLength = sizeof(int) + *(int*)key;
    }else{
        keyLength = sizeof(int);
    }

    entry.key = malloc(keyLength);
    memcpy(entry.key , key, keyLength);

    entry.rid = rid;
    RC rc = ixfileHandle.deleteEntry( attribute, index , entry );
    return rc;
}

unsigned IndexManager::hash(const Attribute &attribute, const void *key)
{
    return hashFunction(attribute, key, g_N_OriginalNumberOfBuckets, g_currentLevel, g_nextSplitPtr);
}
unsigned IndexManager::hashFunction(const Attribute &attribute, const void *key, unsigned g_N_OriginalNumberOfBuckets, unsigned g_currentLevel, unsigned g_nextSplitPtr)
{
    unsigned hash;

    switch(attribute.type)
    {
    case TypeReal:
        //1
//        unsigned int ui;
//        memcpy( &ui, key, sizeof( float ) );
//        int key_float;
//        key_float = ui & 0xfffffff8;
        //2
//        int key_float;
//        memcpy( &key_float, key, sizeof( int ) );
        //3
       int key_float;
       key_float = (*(float*)key)*10;
        //4
        // int key_float;
        // key_float = (int)(*(float*)key);

        hash = hashFunction_int(key_float, g_N_OriginalNumberOfBuckets, g_currentLevel, g_nextSplitPtr);
        break;
    case TypeInt:
        int key_int;
        memcpy( &key_int, key, sizeof(int));
        hash = hashFunction_int(key_int, g_N_OriginalNumberOfBuckets, g_currentLevel, g_nextSplitPtr);

        break;
    case TypeVarChar:
        char* keyStr = Varchart2CString(key);
        int key_varchar = 5381;
        int c;

        while (c = *keyStr++)
            key_varchar = ((key_varchar << 5) + key_varchar) + c; /* hash * 33 + c */

        hash = hashFunction_int(key_varchar, g_N_OriginalNumberOfBuckets, g_currentLevel, g_nextSplitPtr);
        break;
    }

    return hash;
}

unsigned IndexManager::hashFunction_int(int key_int, unsigned g_N_OriginalNumberOfBuckets, unsigned g_currentLevel, unsigned g_nextSplitPtr)
{
    unsigned hash;

    //h(key)
    hash = key_int % ( pow( 2 , g_currentLevel) * g_N_OriginalNumberOfBuckets );

    //h'(key)
    if(hash < g_nextSplitPtr)
        hash = key_int % ( pow( 2 , g_currentLevel+1) * g_N_OriginalNumberOfBuckets );

    return hash;
}

int IndexManager::pow(int base, int power)
{
    int value = 1;
    for( int i = 0 ; i < power ; i++)
    {
        value *= base;
    }
    return value;
}

char* IndexManager::Varchart2CString(const void *recordBasedStr)
{
    int strLength = 0;
    memcpy(&strLength,recordBasedStr,sizeof(int));
//    if(strLength>100){
//        cout<<"["<<strLength<<"]";
//        strLength=20;
//    }
    char* cstring = (char*)malloc(strLength+1);
    memcpy(cstring, (char*)recordBasedStr + sizeof(int) , strLength);
    cstring[strLength] = '\0';
    return cstring;
}

/*
Number of total entries in the page (+ overflow pages) : ??

primary Page No.??
 a. # of entries : ??
 b. entries: [xx] [xx] [xx] [xx] [xx] [xx]

overflow Page No.?? linked to [primary | overflow] page ??
 a. # of entries : ??
 b. entries: [xx] [xx] [xx] [xx] [xx]

where [xx] refers each entry: [key/rid.pagenum,rid.slotnum] (e.g., [20/0,0]).
 */
RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber) 
{
	return ixfileHandle.printIndexEntriesInAPage(attribute, primaryPageNumber);
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages) 
{
	return ixfileHandle.getNumberOfPrimaryPages(numberOfPrimaryPages);
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages) 
{
	return ixfileHandle.getNumberOfAllPages(numberOfAllPages);
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
    ix_ScanIterator.ixfileHandlePtr = &ixfileHandle;
    ix_ScanIterator.attribute = attribute;
    ix_ScanIterator.lowKey = lowKey;
    ix_ScanIterator.highKey = highKey;
    ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
    ix_ScanIterator.highKeyInclusive = highKeyInclusive;

    //maxIndex = totalBucketsCount -1;
    ix_ScanIterator.maxIndex = ixfileHandle.g_bottomLinePtr + ixfileHandle.g_nextSplitPtr;
    ix_ScanIterator.currentEntryPtrOfList = 0;
    ix_ScanIterator.currentIndex = 0;
    ix_ScanIterator.ifMoveToNextIndex = true;
    ix_ScanIterator.ifFirstIteration = true;

    //for hashFunction
    ix_ScanIterator.ixs_N_OriginalNumberOfBuckets = g_N_OriginalNumberOfBuckets;
    ix_ScanIterator.ixs_currentLevel = g_currentLevel;
    ix_ScanIterator.ixs_nextSplitPtr = g_nextSplitPtr;
//    ix_ScanIterator.g_bottomLinePtr = g_bottomLinePtr;


	return 0;
}

IX_ScanIterator::IX_ScanIterator()
{
    indexPageList.clear();
    entryList.clear();
    ifFirstIteration = true;
    currentEntryPtrOfList = 0;
    currentIndex = 0;
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    RC rc = 0;

    /*
    if(lowKey == NULL && highKey == NULL) // scan all
    {
        while(true)
        {
            if(ifMoveToNextIndex){
                indexPageList.clear();
                entryList.clear();
                rc = ixfileHandlePtr->loadAllBucketsByIndex(currentIndex, indexPageList);
                ixfileHandlePtr->getWholeEntryListForNumerical(indexPageList, entryList);

                currentIndex++;
                currentEntryPtrOfList = 0;

                if(entryList.size() > 0)
                    ifMoveToNextIndex = false;
            }

            int size = entryList.size(); //for test
            if(currentEntryPtrOfList < entryList.size()){
                memcpy(key , entryList[currentEntryPtrOfList].key , sizeof(entryList[currentEntryPtrOfList].key));
                memcpy(&rid , &entryList[currentEntryPtrOfList].rid , sizeof(RID));
                currentEntryPtrOfList++;
                return 0;
            }else{
                ifMoveToNextIndex = true;
                if(currentIndex > maxIndex)
                    return IX_EOF;
            }
        }
    }else
    */

    if(lowKey == highKey && lowKey != NULL) //for exact match
    {

        if(ifFirstIteration){
            currentIndex = IndexManager::hashFunction(attribute, key, ixs_N_OriginalNumberOfBuckets, ixs_currentLevel, ixs_nextSplitPtr);

            rc = ixfileHandlePtr->loadAllBucketsByIndex(currentIndex, indexPageList);
            ixfileHandlePtr->getWholeEntryList(attribute, indexPageList, entryList);
            ifFirstIteration = false;
        }

        while(true)
        {
            if(currentEntryPtrOfList < entryList.size()){
                bool ifPass = false;
                int key_int = 0;
                float key_float = 0.0f;
                string key_string;
                char* key_cstring;

                // convert and compare key
                switch(attribute.type)
                {
                case TypeReal:
                    key_float = *(float *)entryList[currentEntryPtrOfList].key;
                    ifPass = compare(key_float);
                    break;
                case TypeInt:
                    key_int = *(int *)entryList[currentEntryPtrOfList].key;
                    ifPass = compare(key_int);
                    break;
                case TypeVarChar:
                    //take care of length issue when insert
                    key_cstring = IndexManager::Varchart2CString(entryList[currentEntryPtrOfList].key);
                    key_string = string(key_cstring);
                    ifPass = compare(key_string);
                    break;
                }

                // return SUCCESS or goto next iteration
                if(ifPass){
                    int keyLength = 0;
                    int entryLength = 0;
                    if(attribute.type == TypeVarChar){
                        keyLength = sizeof(int)+ *(unsigned*)entryList[currentEntryPtrOfList].key; // the first 4 bytes is the length of varchar key
//                        entryLength = keyLength + sizeof(RID);
                    }else{
                        keyLength = sizeof(int);
//                        entryLength = sizeof(int) + sizeof(RID);
                    }
                    memcpy(key , entryList[currentEntryPtrOfList].key , keyLength);
//                    memcpy(key , entryList[currentEntryPtrOfList].key , entryLength);
                    memcpy(&rid , &entryList[currentEntryPtrOfList].rid , sizeof(RID));
                    currentEntryPtrOfList++;
                    return 0;
                }else{
                    currentEntryPtrOfList++;
                    continue;
                }
            }else{
                return IX_EOF;
            }
        }
    }else{

        while(true)
        {
            if(ifMoveToNextIndex){
                indexPageList.clear();
                entryList.clear();
                rc = ixfileHandlePtr->loadAllBucketsByIndex(currentIndex, indexPageList);
                ixfileHandlePtr->getWholeEntryList(attribute, indexPageList, entryList);

                currentIndex++;
                currentEntryPtrOfList = 0;

                if(entryList.size() > 0)
                    ifMoveToNextIndex = false;
            }

            if(currentEntryPtrOfList < entryList.size()){
                bool ifPass = false;
                int key_int = 0;
                float key_float = 0.0f;
                string key_string;
                char* key_cstring;

                // convert and compare key
                switch(attribute.type)
                {
                case TypeReal:
                    key_float = *(float *)entryList[currentEntryPtrOfList].key;
                    ifPass = compare(key_float);
                    break;
                case TypeInt:
                    key_int = *(int *)entryList[currentEntryPtrOfList].key;
                    ifPass = compare(key_int);
                    break;
                case TypeVarChar:
                    //take care of length issue when insert
                    key_cstring = IndexManager::Varchart2CString(entryList[currentEntryPtrOfList].key);
                    key_string = string(key_cstring);
                    ifPass = compare(key_string);
                    break;
                }

                // return SUCCESS or goto next iteration
                if(ifPass){
                    int keyLength = 0;
                    int entryLength = 0;
                    if(attribute.type == TypeVarChar){
                        keyLength = sizeof(int)+ *(unsigned*)entryList[currentEntryPtrOfList].key; // the first 4 bytes is the length of varchar key
//                        entryLength = keyLength + sizeof(RID);
                    }else{
                        keyLength = sizeof(int);
//                        entryLength = sizeof(int) + sizeof(RID);
                    }
                    memcpy(key , entryList[currentEntryPtrOfList].key , keyLength);
//                    memcpy(key , entryList[currentEntryPtrOfList].key , entryLength);
                    memcpy(&rid , &entryList[currentEntryPtrOfList].rid , sizeof(RID));
                    currentEntryPtrOfList++;
                    return 0;
                }else{
                    currentEntryPtrOfList++;
                    continue;
                }
            }else{
                ifMoveToNextIndex = true;
                if(currentIndex > maxIndex)
                    return IX_EOF;
            }
        }

    }
	return rc;
}
bool IX_ScanIterator::compare(string key)
{
    string lowKey_string;
    string highKey_string;
    bool isNot_LowKeyNull = false;
    bool isNot_HighKeyNull = false;

    // # prepare variable
    if(lowKey != NULL){
        isNot_LowKeyNull = true;
        lowKey_string = string(IndexManager::Varchart2CString(lowKey));
    }
    if(highKey != NULL){
        isNot_HighKeyNull = true;
        highKey_string = string(IndexManager::Varchart2CString(highKey));
    }


    // # comparison
    // >= lowKey
    if(isNot_LowKeyNull){
        if( key > lowKey_string)
            return true;
        else if(lowKeyInclusive && key == lowKey_string)
            return true;
    }
    // <= highKey
    if(isNot_HighKeyNull){
        if(key < highKey_string)
            return true;
        else if(highKeyInclusive && key == highKey_string)
            return true;
    }
    // lowKey == highKey == NULL -> scan all
    if(!isNot_LowKeyNull && !isNot_HighKeyNull)
        return true;
    // key == lowKey == highKey != NULL && lowKeyInclusive == highKeyInclusive == true -> exact match
    if(isNot_LowKeyNull && lowKey_string == highKey_string && key == lowKey_string && lowKeyInclusive)
        return true;
    // lowKey == highKey != key && lowKeyInclusive == highKeyInclusive == false -> exclusive
    if(isNot_LowKeyNull && lowKey_string == highKey_string && !lowKeyInclusive && lowKey_string != key)
        return true;

    return false; // return true means pass, false means fail
}
bool IX_ScanIterator::compare(int key)
{
    int lowKey_int = 0;
    int highKey_int = 0;
    bool isNot_LowKeyNull = false;
    bool isNot_HighKeyNull = false;

    // # prepare variable
    if(lowKey != NULL){
        isNot_LowKeyNull = true;
        lowKey_int = *(int *)lowKey;
    }
    if(highKey != NULL){
        isNot_HighKeyNull = true;
        highKey_int =*(int *)highKey;
    }


    // # comparison
    // >= lowKey
    if(isNot_LowKeyNull){
        if( key > lowKey_int)
            return true;
        else if(lowKeyInclusive && key == lowKey_int)
            return true;
    }
    // <= highKey
    if(isNot_HighKeyNull){
        if(key < highKey_int)
            return true;
        else if(highKeyInclusive && key == highKey_int)
            return true;
    }
    // lowKey == highKey == NULL -> scan all
    if(!isNot_LowKeyNull && !isNot_HighKeyNull)
        return true;
    // key == lowKey == highKey != NULL && lowKeyInclusive == highKeyInclusive == true -> exact match
    if(isNot_LowKeyNull && lowKey_int == highKey_int && key == lowKey_int && lowKeyInclusive)
        return true;
    // lowKey == highKey != key && lowKeyInclusive == highKeyInclusive == false -> exclusive
    if(isNot_LowKeyNull && lowKey_int == highKey_int && !lowKeyInclusive && lowKey_int != key)
        return true;

    return false; // return true means pass, false means fail
}
bool IX_ScanIterator::compare(float key)
{
    float lowKey_float = 0;
    float highKey_float = 0;
    bool isNot_LowKeyNull = false;
    bool isNot_HighKeyNull = false;

    // # prepare variable
    if(lowKey != NULL){
        isNot_LowKeyNull = true;
        lowKey_float = *(float *)lowKey;
    }
    if(highKey != NULL){
        isNot_HighKeyNull = true;
        highKey_float =*(float *)highKey;
    }


    // # comparison
    // >= lowKey
    if(isNot_LowKeyNull){
        if( key > lowKey_float)
            return true;
        else if(lowKeyInclusive && key == lowKey_float)
            return true;
    }
    // <= highKey
    if(isNot_HighKeyNull){
        if(key < highKey_float)
            return true;
        else if(highKeyInclusive && key == highKey_float)
            return true;
    }
    // lowKey == highKey == NULL -> scan all
    if(!isNot_LowKeyNull && !isNot_HighKeyNull)
        return true;
    // key == lowKey == highKey != NULL && lowKeyInclusive == highKeyInclusive == true -> exact match
    if(isNot_LowKeyNull && lowKey_float == highKey_float && key == lowKey_float && lowKeyInclusive)
        return true;
    // lowKey == highKey != key && lowKeyInclusive == highKeyInclusive == false -> exclusive
    if(isNot_LowKeyNull && lowKey_float == highKey_float && !lowKeyInclusive && lowKey_float != key)
        return true;

    return false; // return true means pass, false means fail
}
RC IX_ScanIterator::close()
{
    //TODO: IX_ScanIterator::close()
    indexPageList.clear();
    entryList.clear();
    // ixfileHandlePtr->closeFile();
	return 0;
}


IXFileHandle::IXFileHandle()
{
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    unsigned int readPageCount1, writePageCount1, appendPageCount1;
    fileHandle_primary.collectCounterValues(readPageCount1, writePageCount1, appendPageCount1);
    unsigned int readPageCount2, writePageCount2, appendPageCount2;
    fileHandle_extra.collectCounterValues(readPageCount2, writePageCount2, appendPageCount2);

    readPageCount   = readPageCount1    + readPageCount2;
    writePageCount  = writePageCount1   + writePageCount2;
    appendPageCount = appendPageCount1  + appendPageCount2;
    return 0;
}

RC IXFileHandle::createFile (const string &fileName, const unsigned &numberOfPages)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    FileHandle fileHandle_primary, fileHandle_extra;

    RC rc = 0;

    // ***Create two files

    // **Generate the Primary Index File
    string fileName_primary = fileName + "_primary";
    rc += pfm->createFile(fileName_primary.c_str());
    if(rc != SUCCESS)
        return -1;
    rc += pfm->openFile(fileName_primary.c_str(), fileHandle_primary);

    // Primary Index Format: EntryCount(int), NextPagePtr(int)
    void *primaryPage = malloc(PAGE_SIZE);
    prepareIndexPage(primaryPage);

    for(unsigned i = 0 ; i < numberOfPages ; i++)
    {
        rc += fileHandle_primary.appendPage(primaryPage);
    }
    rc += pfm->closeFile(fileHandle_primary);


    // **Generate the Extra Index File with Metadata**
    // Metadata Format:
    // N(unsigned),CurrentLevel(unsigned),NextSplitPtr(unsigned),BottomLinePtr(unsigned)
    string fileName_extra = fileName + "_extra";
    rc += pfm->createFile(fileName_extra.c_str());
    if(rc != SUCCESS)
        return -1;
    rc += pfm->openFile(fileName_extra.c_str(), fileHandle_extra);

    void *extraPage = malloc(PAGE_SIZE);
    prepareInitMetadataPage( extraPage , numberOfPages);

    rc += fileHandle_extra.appendPage(extraPage);
    pfm->closeFile(fileHandle_extra);

    free(primaryPage);
    free(extraPage);
    pfm = NULL;
    return rc;
}
RC IXFileHandle::prepareIndexPage(void *bufferPage)
{
    // Index Page Format: EntryCount(int), FreeSloPtr(int), NextPagePtr(int)
    int indexPageData[] = {0,3*sizeof(int),-1};
    memcpy(bufferPage, indexPageData, sizeof(indexPageData));

    return 0;
}

RC IXFileHandle::prepareInitMetadataPage(void *pageBuffer, unsigned initNumberOfPages)
{
    // Metadata Format:
    // N(unsigned),CurrentLevel(unsigned),NextSplitPtr(unsigned),BottomLinePtr(unsigned)
    unsigned metadata[] = {initNumberOfPages, 0, 0, initNumberOfPages-1};
    memcpy(pageBuffer, metadata, sizeof(metadata));

    return 0;
}

RC IXFileHandle::destroyFile(const string &fileName)
{
    RC rc = 0;
    PagedFileManager *pfm = PagedFileManager::instance();

    //destroy two files
    string fileName_primary = fileName + "_primary";
    rc += pfm->destroyFile(fileName_primary.c_str());
    string fileName_extra = fileName + "_extra";
    rc += pfm->destroyFile(fileName_extra.c_str());

    pfm = NULL;
    return rc;
}
RC IXFileHandle::openFile(const string &fileName)
{
    RC rc = 0;
    PagedFileManager *pfm = PagedFileManager::instance();
    //open two files
    string fileName_primary = fileName + "_primary";
    rc += pfm->openFile(fileName_primary.c_str(), fileHandle_primary);
    string fileName_extra = fileName + "_extra";
    rc += pfm->openFile(fileName_extra.c_str(), fileHandle_extra);

    pfm = NULL;
    return rc;
}

RC IXFileHandle::loadMetadata(  unsigned &o_N_OriginalNumberOfBuckets,
                                unsigned &o_currentLevel,
                                unsigned &o_nextSplitPtr,
                                unsigned &o_bottomLinePtr   )
{
    unsigned metadata[4];
    void *pageBuffer = malloc(PAGE_SIZE); 
    fileHandle_extra.readPage( 0, pageBuffer);
    memcpy( metadata , pageBuffer , sizeof(metadata));

    //Info for outside class
    o_N_OriginalNumberOfBuckets = metadata[0];
    o_currentLevel              = metadata[1];
    o_nextSplitPtr              = metadata[2];
    o_bottomLinePtr             = metadata[3];

    //Info for self usage
    g_N_OriginalNumberOfBuckets = metadata[0];
    g_currentLevel              = metadata[1];
    g_nextSplitPtr              = metadata[2];
    g_bottomLinePtr             = metadata[3];

    free(pageBuffer);
    return 0;
}
RC IXFileHandle::updateMetadata()
{
    RC rc = 0;
    void *pageBuffer = malloc(PAGE_SIZE);
    rc = fileHandle_extra.readPage( 0, pageBuffer);

    unsigned metadata[] = {g_N_OriginalNumberOfBuckets, g_currentLevel, g_nextSplitPtr, g_bottomLinePtr};
    memcpy(pageBuffer, metadata, sizeof(metadata));

    rc += fileHandle_extra.writePage( 0, pageBuffer);

    return rc;
}
RC IXFileHandle::closeFile()
{
    RC rc = 0;
    PagedFileManager *pfm = PagedFileManager::instance();
    //close two files
    rc += pfm->closeFile(fileHandle_primary);
    rc += pfm->closeFile(fileHandle_extra);

    pfm = NULL;
    return rc;

}
RC  IXFileHandle::getNumberOfAllPages(unsigned &numberOfAllPages)
{
    numberOfAllPages = fileHandle_primary.getNumberOfPages();
    numberOfAllPages += fileHandle_extra.getNumberOfPages();
    return 0;
}
RC IXFileHandle::getNumberOfPrimaryPages(unsigned &numberOfPrimaryPages)
{
    numberOfPrimaryPages = fileHandle_primary.getNumberOfPages();
    return 0;
}

//RC IXFileHandle::bulkUpdateEntries(IndexPage &indexPage, const Attribute &attribute, vector<Entry> &entryList)
//{
////        memcpy(pnIntArray,&vIntVector[0], sizeof(int)*vIntVector.size());

//    // update EntryCount
//    memcpy(indexPage.pageBuffer, &indexPage.EntryCount , sizeof(int));
//
//    // update FreeSlotPointer
//    indexPage.FreeSlotPointer += SIZE_OF_NUMERICAL_ENTRY;
//    memcpy((int*)indexPage.pageBuffer+1, &indexPage.FreeSlotPointer , sizeof(int));
//
//    // a chance to update NextPagePtr
//    memcpy((int*)indexPage.pageBuffer+2, &indexPage.NextPagePtr , sizeof(int));
//
//    // write page
//    if(indexPage.isPrimaryPage)
//        return fileHandle_primary.writePage( indexPage.pageNumber , indexPage.pageBuffer);
//    else
//        return fileHandle_extra.writePage( indexPage.pageNumber , indexPage.pageBuffer);
//
//    return 0;
//}



RC IXFileHandle::split(const Attribute &attribute)
{
    unsigned splitIndex = this->g_nextSplitPtr;

    // # Manage Split Round-robin Attributes
    if(g_nextSplitPtr < g_bottomLinePtr){ //this round not finished
        this->g_nextSplitPtr++;
    }else{ // this round finished, then move on to next level
        this->g_nextSplitPtr = 0;
        this->g_currentLevel++;
        this->g_bottomLinePtr = IndexManager::pow( 2 , this->g_currentLevel) * this->g_N_OriginalNumberOfBuckets - 1;
    }
    // update metadata in extra page
    updateMetadata();



//    unsigned imageIndex = splitIndex + this->g_bottomLinePtr + 1;
//    if(this->g_nextSplitPtr == 0) imageIndex = this->g_bottomLinePtr;
//
//    unsigned primaryPageCount;
//    getNumberOfPrimaryPages(primaryPageCount);


    // # Append Image Page
//    if( (primaryPageCount-1) < imageIndex )
    {
        void *imagePage = malloc(PAGE_SIZE);
        prepareIndexPage(imagePage);
        fileHandle_primary.appendPage(imagePage);
    }

//    cout << "++Before>+++++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;
//    printIndexEntriesCountInAPage(attribute, splitIndex);
//    printIndexEntriesCountInAPage(attribute, imageIndex);
//    printIndexEntriesInAPage(attribute, splitIndex);
//    printIndexEntriesInAPage(attribute, imageIndex);

    // ### Redistribute Entries
    redistributeEntries(splitIndex, attribute);

//    cout << "--After>--------------------------------------------------" << endl;
//    printIndexEntriesCountInAPage(attribute, splitIndex);
//    printIndexEntriesCountInAPage(attribute, imageIndex);
//    printIndexEntriesInAPage(attribute, splitIndex);
//    printIndexEntriesInAPage(attribute, imageIndex);

    return 0;
}
RC IXFileHandle::redistributeEntries(const unsigned splitIndex, const Attribute &attribute)
{
    IndexPage tempPage;
    vector<Entry> entryList_original;


    // # redistribute to original page or new image page
    vector<Entry> entryList_primary; // stay at original page
    vector<Entry> entryList_image; // move to new image page
    entryList_primary.clear();
    entryList_image.clear();
    Entry tempEntry;
    bool isFirstLoop = true;
    bool isPrimaryPageFilled = false;
    bool isImagePageFilled = false;
    vector<unsigned> overflowPageNumberToFill_primaryV;
    unsigned overflowPageNumberToFill_image;
    unsigned h2Index = 0;
    int nextPagePtr = splitIndex;
//    unsigned freeSlotsCountPerPage = (PAGE_SIZE - SIZE_OF_PAGE_HEADER) / SIZE_OF_NUMERICAL_ENTRY; //suppose 340
    unsigned freeBytesPerPage = PAGE_SIZE - SIZE_OF_PAGE_HEADER;


//    unsigned imageIndex = splitIndex + this->g_bottomLinePtr + 1;
//    if(this->g_nextSplitPtr == 0) imageIndex = this->g_bottomLinePtr;
    unsigned primaryPageCount;
    getNumberOfPrimaryPages(primaryPageCount);
    unsigned imageIndex = primaryPageCount-1;

    IndexPage previosPage_primary;
    IndexPage previosPage_image;
    previosPage_primary.isPrimaryPage = true;
    previosPage_primary.pageNumber = splitIndex;

    unsigned tempAccumulateLength_primary = 0;
    unsigned tempAccumulateLength_image = 0;

    while(nextPagePtr != -1){

        // # Load entries
        entryList_original.clear();
        if(isFirstLoop){
            loadPage(nextPagePtr, tempPage, TypePrimaryPage);
            isFirstLoop = false;
        }else{
            loadPage(nextPagePtr, tempPage, TypeOverflowPage);
        }
        getEntryList(attribute, tempPage , entryList_original);
        free(tempPage.pageBuffer);
        // get nextPagePtr
        nextPagePtr = tempPage.NextPagePtr;
        // append nextPagePtr to vector
        overflowPageNumberToFill_primaryV.push_back(nextPagePtr);

        // ## Redistribute
        int entryCount = entryList_original.size();
        unsigned lastEntryLength_primary = 0;
        unsigned lastEntryLength_image = 0;
        for(int i = 0 ; i < entryCount ; i++)
        {
            tempEntry = entryList_original[i];
            h2Index = IndexManager::hashFunction(attribute, tempEntry.key, g_N_OriginalNumberOfBuckets, g_currentLevel, g_nextSplitPtr);

            unsigned keyLength = 0;
            unsigned entryLength = 0;
            if(attribute.type == TypeVarChar){
                keyLength = sizeof(int)+ *(unsigned*)tempEntry.key; // the first 4 bytes is the length of varchar key
                entryLength = keyLength + sizeof(RID);
            }else{
                entryLength = SIZE_OF_NUMERICAL_ENTRY;
            }

            if(h2Index == splitIndex){
                entryList_primary.push_back(tempEntry);
                tempAccumulateLength_primary += entryLength;
                lastEntryLength_primary = entryLength;
            }else{
                entryList_image.push_back(tempEntry);
                tempAccumulateLength_image += entryLength;
                lastEntryLength_image = entryLength;
            }


//            if(attribute.type == TypeReal || attribute.type == TypeInt){
            // # primary page part
            // check if the buffer entry list size can fill up one page
//            if(entryList_primary.size() >= freeSlotsCountPerPage){
            if(tempAccumulateLength_primary >= freeBytesPerPage){
                if(!isPrimaryPageFilled){
                    // for first page
//                    cout<<"1)entryList.size() from:"<<entryList_primary.size()<<" to: ";
                    fillUpPage(splitIndex, entryList_primary, attribute, TypePrimaryPage);
//                    cout<<entryList_primary.size()<<" to index: "<<splitIndex<<endl;
                    isPrimaryPageFilled = true;
                    previosPage_primary.isPrimaryPage = true;
                    previosPage_primary.pageNumber = splitIndex;
                }else{
//                    cout<<"2)entryList.size() from:"<<entryList_primary.size()<<" to: ";
                    fillUpPage(overflowPageNumberToFill_primaryV[0], entryList_primary, attribute, TypeOverflowPage);
//                    cout<<entryList_primary.size()<<" to index: "<<overflowPageNumberToFill_primaryV[0]<<endl;
                    previosPage_primary.isPrimaryPage = false;
                    previosPage_primary.pageNumber = overflowPageNumberToFill_primaryV[0];
                    overflowPageNumberToFill_primaryV.erase(overflowPageNumberToFill_primaryV.begin());
                }

                // reset tempAccumulateLength_primary
                if(tempAccumulateLength_primary == freeBytesPerPage)
                    tempAccumulateLength_primary = 0;
                if(tempAccumulateLength_primary > freeBytesPerPage)
                    tempAccumulateLength_primary = lastEntryLength_primary;
            }

            // # image page part
            // check if the buffer entry list size can fill up one page
//           if(entryList_image.size() >= freeSlotsCountPerPage){
            if(tempAccumulateLength_image >= freeBytesPerPage){
                if(!isImagePageFilled){
                    // for first page
//                    cout<<"3)entryList.size() from:"<<entryList_image.size()<<" to: ";
                    fillUpPage(imageIndex, entryList_image, attribute, TypePrimaryPage);
//                    cout<<entryList_image.size()<<" to index: "<<imageIndex<<endl;
                    isImagePageFilled = true;
                    previosPage_image.isPrimaryPage = true;
                    previosPage_image.pageNumber = imageIndex;
                }else{
                    overflowPageNumberToFill_image = allocateNewOverflowPageNumber();
                    int rc;
//                    cout<<"4)entryList.size() from:"<<entryList_image.size()<<" to: ";
                    rc = fillUpPage(overflowPageNumberToFill_image, entryList_image, attribute, TypeOverflowPage);
//                    cout<<entryList_image.size()<<" to index: "<<overflowPageNumberToFill_image<<endl;
                    if(rc == SUCCESS)
                        updateNextPagePtr(previosPage_image , overflowPageNumberToFill_image);
                    previosPage_image.isPrimaryPage = false;
                    previosPage_image.pageNumber = overflowPageNumberToFill_image;
                }

                // reset tempAccumulateLength_image
                if(tempAccumulateLength_image == freeBytesPerPage)
                    tempAccumulateLength_image = 0;
                if(tempAccumulateLength_image > freeBytesPerPage)
                    tempAccumulateLength_image = lastEntryLength_image;
            }
//            }else if(attribute.type == TypeVarChar){
//                // handle varchar
//            }
        }
    }

    // # Handle the rest entries
    // # primary page part
    // check if the buffer entry list size can fill up one page
    if(entryList_primary.size() >= 0){
//        IndexPage theLastPage; //?
        if(!isPrimaryPageFilled){
            // for first page
//            cout<<"5)entryList.size() from:"<<entryList_primary.size()<<" to: ";
            fillUpPage(splitIndex, entryList_primary, attribute, TypePrimaryPage);
//            cout<<entryList_primary.size()<<" to index: "<<splitIndex<<endl;
            previosPage_primary.isPrimaryPage = true;
            previosPage_primary.pageNumber = splitIndex;
        }else{
//            cout<<"6)entryList.size() from:"<<entryList_primary.size()<<" to: ";
            fillUpPage(overflowPageNumberToFill_primaryV[0], entryList_primary, attribute, TypeOverflowPage);
//            cout<<entryList_primary.size()<<" to index: "<<overflowPageNumberToFill_primaryV[0]<<endl;
            previosPage_primary.isPrimaryPage = false;
            previosPage_primary.pageNumber = overflowPageNumberToFill_primaryV[0];
            overflowPageNumberToFill_primaryV.erase(overflowPageNumberToFill_primaryV.begin());
        }
    }
    // set the last updated page's nextPagePtr to -1;
    updateNextPagePtr(previosPage_primary , -1);

    // # image page part
    // check if the buffer entry list size can fill up one page
    if(entryList_image.size() >= 0){
        if(!isImagePageFilled){
            // for first page
//            cout<<"7)entryList.size() from:"<<entryList_image.size()<<" to: ";
            fillUpPage(imageIndex, entryList_image, attribute, TypePrimaryPage);
//            cout<<entryList_image.size()<<" to index: "<<imageIndex<<endl;
            previosPage_image.isPrimaryPage = true;
            previosPage_image.pageNumber = imageIndex;
        }else{
            overflowPageNumberToFill_image = allocateNewOverflowPageNumber();
            int rc;
//            cout<<"8)entryList.size() from:"<<entryList_image.size()<<" to: ";
            rc = fillUpPage(overflowPageNumberToFill_image, entryList_image, attribute, TypeOverflowPage);
//            cout<<entryList_image.size()<<" to index: "<<overflowPageNumberToFill_image<<endl;
            if(rc == SUCCESS)
                updateNextPagePtr(previosPage_image , overflowPageNumberToFill_image);
            previosPage_image.isPrimaryPage = false;
            previosPage_image.pageNumber = overflowPageNumberToFill_image;
        }
    }

    // #recycle unused overflow page
    releaseOverflowPages(overflowPageNumberToFill_primaryV);

    return 0;
}
RC IXFileHandle::merge(const Attribute &attribute)
{
    vector<IndexPage> indexPageList_primary;
    vector<IndexPage> indexPageList_image;
    vector<Entry> entryList_merge;
//    vector<Entry> entryList_primary;
//    vector<Entry> entryList_image;

    // # Manage Merge Round-robin Attributes
    if(g_nextSplitPtr > 0){ //this round not finished
        this->g_nextSplitPtr--;
    }else{ // this round finished, then go back to previous level
        this->g_currentLevel--;
        this->g_bottomLinePtr = IndexManager::pow( 2 , this->g_currentLevel) * this->g_N_OriginalNumberOfBuckets - 1;
        this->g_nextSplitPtr = this->g_bottomLinePtr;
    }
    // update metadata in extra page
    updateMetadata();

    unsigned index_primary = this->g_nextSplitPtr;
    unsigned index_image = this->g_bottomLinePtr + this->g_nextSplitPtr;
    if(this->g_nextSplitPtr == 0){
        index_image = (this->g_bottomLinePtr+1)*2-1;
    }

    loadAllBucketsByIndex(index_primary, indexPageList_primary);
    getWholeEntryList(attribute, indexPageList_primary, entryList_merge);
    int pagesCount = indexPageList_primary.size();
    vector<unsigned> recycleOverflowPageNumber_V;
    for(int i = 1 ; i < pagesCount ; i++)
    {
        recycleOverflowPageNumber_V.push_back(indexPageList_primary[i].pageNumber);
    }

    loadAllBucketsByIndex(index_image, indexPageList_image);
    getWholeEntryList(attribute, indexPageList_image, entryList_merge);
    pagesCount = indexPageList_image.size();
    for(int i = 1 ; i < pagesCount ; i++)
    {
        recycleOverflowPageNumber_V.push_back(indexPageList_image[i].pageNumber);
    }

    // #recycle unused overflow page
    releaseOverflowPages(recycleOverflowPageNumber_V);


    bool isPrimaryPageFilled = false;
    IndexPage previosPage;
    unsigned overflowPageNumberToFill;
    while(true){
        if(!isPrimaryPageFilled){
            // for first page
//            cout<<"[Merge](0)entryList.size() fillUpPage from:"<<entryList_merge.size()<<" to: ";
            fillUpPage(index_primary, entryList_merge, attribute, TypePrimaryPage);
//            cout<<entryList_merge.size()<<" to index: "<<index_primary<<endl;
            isPrimaryPageFilled = true;
            previosPage.isPrimaryPage = true;
            previosPage.pageNumber = index_primary;
        }else{
            overflowPageNumberToFill = allocateNewOverflowPageNumber();
             int rc;
//             cout<<"[Merge](1)entryList.size() fillUpPage from:"<<entryList_merge.size()<<" to: ";
             rc = fillUpPage(overflowPageNumberToFill, entryList_merge, attribute, TypeOverflowPage);
//             cout<<entryList_merge.size()<<" to index: "<<overflowPageNumberToFill<<endl;
             if(rc == SUCCESS)
                 updateNextPagePtr(previosPage , overflowPageNumberToFill);
             previosPage.isPrimaryPage = false;
             previosPage.pageNumber = overflowPageNumberToFill;
        }
        if(entryList_merge.size() == 0) break;
    }

//    if(isFirstLoop){
//        loadPage(nextPagePtr, tempPage, TypePrimaryPage);
//        isFirstLoop = false;
//    }else{
//        loadPage(nextPagePtr, tempPage, TypeOverflowPage);
//    }
//    getEntryList(attribute, tempPage , entryList_original);


    return 0;
}


RC IXFileHandle::releaseOverflowPages(vector<unsigned> &pageNumbers)
{
    void *releasedPage = malloc(PAGE_SIZE);
    memset(releasedPage, 0, PAGE_SIZE);
    prepareIndexPage( releasedPage );

    int size = pageNumbers.size();
    for(int i = 0 ; i<size ; i++)
    {
        fileHandle_extra.writePage(pageNumbers[i],releasedPage);
    }
    pageNumbers.clear();
    return 0;
}

// get some of the input data to fill up a page and write to file,
// then remove the data which have been written to file.
RC IXFileHandle::fillUpPage(const unsigned index, vector<Entry> &entryList, const Attribute &attribute, const PageType pageType)
{
    RC rc = 0;

    // Sort
    sortEntryList(entryList, attribute);


    IndexPage currentPage;
    loadPage(index, currentPage, pageType); // just for the "NextPagePtr"
    currentPage.FreeSlotPointer = SIZE_OF_PAGE_HEADER;
//    prepareIndexPage(currentPage.pageBuffer);

    unsigned freeBytes = PAGE_SIZE - SIZE_OF_PAGE_HEADER;

    unsigned entryCountToFill = 0;
    unsigned entryLengthToFill = 0;
    unsigned sizeOfEntryList = entryList.size();
    // count how many entries can fit into one page
    // count how many bytes of entries can fit into one page
    if(attribute.type == TypeVarChar){
        Entry entry;
        unsigned keyLength = 0;
        unsigned entryLength = 0;
        for(unsigned i = 0; i < sizeOfEntryList; i++)
        {
            entry = entryList[i];
            keyLength = sizeof(int)+ *(unsigned*)entry.key; // the first 4 bytes is the length of varchar key
            entryLength = keyLength + sizeof(RID);
//          entryLength = sizeof(entry); // XOXOXO: It's wrong, always return 12

            if(entryLength <= (freeBytes - entryLengthToFill)){
                // still have free space to fill
                entryLengthToFill += entryLength;
                entryCountToFill++;
            }else{
                // full, ready to fill
                break;
            }
        }
    }else if(attribute.type == TypeReal || attribute.type == TypeInt){
        unsigned freeSlots = freeBytes / SIZE_OF_NUMERICAL_ENTRY; //suppose 340
        if(sizeOfEntryList < freeSlots){
            entryCountToFill = sizeOfEntryList;
         }else{
            entryCountToFill = freeSlots;
        }
        entryLengthToFill = entryCountToFill * SIZE_OF_NUMERICAL_ENTRY;
   }


    // insert entry
    Entry entry;
    unsigned keyLength = 0;
//    unsigned offset = 0;
    for(unsigned i = 0; i < entryCountToFill; i++)
    {
        entry = entryList[i];
        if(attribute.type == TypeVarChar){
            keyLength = sizeof(int) + *(int*)entry.key;
        }else{
            keyLength = sizeof(int);
        }

        // copy key
        memcpy( (char*)currentPage.pageBuffer + currentPage.FreeSlotPointer, entry.key , keyLength);
        currentPage.FreeSlotPointer += keyLength;
        // copy rid
        memcpy( (char*)currentPage.pageBuffer + currentPage.FreeSlotPointer, &entry.rid , sizeof(RID));
        currentPage.FreeSlotPointer += sizeof(RID);
    }
//    memcpy( (char*)currentPage.pageBuffer + SIZE_OF_PAGE_HEADER , &entryList[0] , entryLengthToFill);

    // update EntryCount
    memcpy(currentPage.pageBuffer, &entryCountToFill , sizeof(int));

    // update FreeSlotPointer
//    currentPage.FreeSlotPointer = SIZE_OF_PAGE_HEADER + entryLengthToFill;
    memcpy((int*)currentPage.pageBuffer+1, &currentPage.FreeSlotPointer , sizeof(int));

    // and don't update nextPagePtr


    if(pageType == TypePrimaryPage){
        rc = fileHandle_primary.writePage( index , currentPage.pageBuffer);
    }else if(pageType == TypeOverflowPage){
        rc = fileHandle_extra.writePage( index , currentPage.pageBuffer);
    }

    free(currentPage.pageBuffer);

//    cout<<"1)entryList.size() from:"<<entryList.size()<<" to: ";
    if(rc == SUCCESS){
        //remove the data which have been written to page
        entryList.erase( entryList.begin() , entryList.begin() + entryCountToFill); // vector.erase(index, length);
    }
//    cout<<entryList.size()<<" to index: "<<index<<endl;

    return rc;
}
RC IXFileHandle::updateNextPagePtr(IndexPage &previosPage, const int nextPagePtr)
{
    RC rc = 0;

    PageType pageType;
    if(previosPage.isPrimaryPage)
        pageType = TypePrimaryPage;
    else
        pageType = TypeOverflowPage;

    // load page data
    rc += loadPage(previosPage.pageNumber, previosPage, pageType);

    // just update NextPagePtr
    memcpy((int*)previosPage.pageBuffer+2, &nextPagePtr , sizeof(int));

    // write back to file
    if(pageType == TypePrimaryPage){
        rc += fileHandle_primary.writePage( previosPage.pageNumber , previosPage.pageBuffer);
    }else if(pageType == TypeOverflowPage){
        rc += fileHandle_extra.writePage( previosPage.pageNumber , previosPage.pageBuffer);
    }
    free(previosPage.pageBuffer);

    return rc;
}
unsigned IXFileHandle::allocateNewOverflowPageNumber()
{
//    void *pageBuffer = malloc(PAGE_SIZE);
    IndexPage tempPage;

    int totalPageCount = fileHandle_extra.getNumberOfPages();

    for(int i = 1 ; i < totalPageCount ; i++) // skip 0: page 0 is metadata page
    {
        loadPage( i , tempPage , TypeOverflowPage);
        free(tempPage.pageBuffer);
        if(tempPage.EntryCount == 0 && tempPage.NextPagePtr == -1){
//            cout<<"allocateNewOverflowPageNumber()::reuse: "<<i<<endl;
            return i;
        }
    }

    // # If there is no free page available then append one overflow page.
    tempPage.pageBuffer = malloc(PAGE_SIZE);
    prepareIndexPage(tempPage.pageBuffer);
    fileHandle_extra.appendPage(tempPage.pageBuffer);
    unsigned newOverflowPageNumber = fileHandle_extra.getNumberOfPages() - 1;

//    cout<<"allocateNewOverflowPageNumber()::new: "<<newOverflowPageNumber<<endl;
    return newOverflowPageNumber;
}

// check free space of the primary page
// if(exist free space) then insert directly
// else insert to an overflow page
RC IXFileHandle::insertManagement(const Attribute &attribute, const unsigned index, const Entry &entry)
{
    RC rc = 0;
    IndexPage indexPage;

//    switch(attribute.type)
//    {
//    case TypeReal:
//    case TypeInt:
    if(loadPage( index, indexPage, TypePrimaryPage) != SUCCESS){
        return -1; // fail to load primary index page
    }

    // check free space
    bool existFreeSpace = true;
    int entryLength = 0;
    if(attribute.type == TypeReal || attribute.type == TypeInt){
        if(indexPage.FreeSlotsForNumerEntry > 0){
            // # write to a primary page
            return appendEntryIntoPage(attribute, indexPage, entry);
        }else
            existFreeSpace = false;
    }else if(attribute.type == TypeVarChar){
        entryLength = sizeof(int) + *(int*)entry.key + sizeof(RID);
        if(indexPage.FreeBytes >= entryLength){
            // # write to a primary page
            return appendEntryIntoPage(attribute, indexPage, entry);
        }else
            existFreeSpace = false;
    }

    if(!existFreeSpace)
    {
        // # write to an overflow page
        int nextPagePtr = indexPage.NextPagePtr;

        // # If exist overflow page,
        // go through overflow pages to find free space for inserting.
        // If the last overflow page don't have free space,
        // exit the while loop.
        while(nextPagePtr != -1){
            rc += loadPage(indexPage.NextPagePtr , indexPage, TypeOverflowPage);

            // check free space
            existFreeSpace = true;
            if(attribute.type == TypeReal || attribute.type == TypeInt){
                if(indexPage.FreeSlotsForNumerEntry > 0){
                    // # write to a primary page
                    return appendEntryIntoPage(attribute, indexPage, entry);
                }else
                    existFreeSpace = false;
            }else if(attribute.type == TypeVarChar){
                if(indexPage.FreeBytes >= entryLength){
                    // # write to a primary page
                    return appendEntryIntoPage(attribute, indexPage, entry);
                }else
                    existFreeSpace = false;
            }

            if(!existFreeSpace)
                nextPagePtr = indexPage.NextPagePtr; // move on to the next overflow page
        }

        // # If there is no space for this index entry, then:
        // 1. append new overflow page with this index entry
        // 2. update the last page's NextPagePtr with the new page's pageNumber
        // 3. trigger split

        // 1.append new overflow page with this index entry
        unsigned newOverflowPageNumber;
        appendNewOverflowPageWithEntry(attribute, newOverflowPageNumber, entry);

        // 2.update the last page's NextPagePtr with the new page's pageNumber
        memcpy((int*)indexPage.pageBuffer+2, &newOverflowPageNumber , sizeof(int));
        if(indexPage.isPrimaryPage)
            rc += fileHandle_primary.writePage( indexPage.pageNumber , indexPage.pageBuffer);
        else
            rc += fileHandle_extra.writePage( indexPage.pageNumber , indexPage.pageBuffer);

        free(indexPage.pageBuffer);

        // 3.trigger split
//        cout<<"<<<trigger split>>>"<<endl;
        split(attribute);
//        printIndexEntriesInAPage(attribute, 1);
    }

//        break;
//    case TypeVarChar:
//        //take care of length issue when insert
//        break;
//    }

//    free(pageBuffer);
//    free(indexPage.pageBuffer);
//    delete(&indexPage);
    return rc;
}
RC IXFileHandle::appendEntryIntoPage(const Attribute &attribute, IndexPage &indexPage, const Entry &entry)
{
    RC rc = 0;

    vector<Entry> entryList;
    getEntryList(attribute, indexPage , entryList);
    entryList.push_back(entry);


/*
    // insert entry
    int keyLength;
    if(attribute.type == TypeVarChar){
        keyLength = sizeof(int) + *(int*)entry.key;
    }else{
//        entryLength = sizeof(Entry);
        keyLength = sizeof(int);
    }
    // copy key
    memcpy( (char*)indexPage.pageBuffer+indexPage.FreeSlotPointer , entry.key , keyLength);
    // copy rid
    memcpy( (char*)indexPage.pageBuffer+indexPage.FreeSlotPointer+keyLength , &entry.rid , sizeof(RID));

    // update EntryCount
    indexPage.EntryCount++;
    memcpy(indexPage.pageBuffer, &indexPage.EntryCount , sizeof(int));

    // update FreeSlotPointer
    indexPage.FreeSlotPointer += keyLength + sizeof(RID);
    memcpy((int*)indexPage.pageBuffer+1, &indexPage.FreeSlotPointer , sizeof(int));

    // a chance to update NextPagePtr
    memcpy((int*)indexPage.pageBuffer+2, &indexPage.NextPagePtr , sizeof(int));

    // write page
    if(indexPage.isPrimaryPage)
        rc = fileHandle_primary.writePage( indexPage.pageNumber , indexPage.pageBuffer);
    else
        rc = fileHandle_extra.writePage( indexPage.pageNumber , indexPage.pageBuffer);

*/

    // fillUpPage will sort the entryList
    if(indexPage.isPrimaryPage)
        rc = fillUpPage(indexPage.pageNumber, entryList, attribute, TypePrimaryPage);
    else
        rc = fillUpPage(indexPage.pageNumber, entryList, attribute, TypeOverflowPage);

    free(indexPage.pageBuffer);

    return rc;
}
RC IXFileHandle::appendNewOverflowPageWithEntry(const Attribute &attribute, unsigned &newOverflowPageNumber, const Entry &entry)
{
    IndexPage newIndexPage;
    newIndexPage.pageBuffer = malloc(PAGE_SIZE);
    newIndexPage.pageNumber = allocateNewOverflowPageNumber();
    fileHandle_extra.readPage((int)newIndexPage.pageNumber, newIndexPage.pageBuffer);
//    prepareIndexPage(newIndexPage.pageBuffer);
//    fileHandle_extra.appendPage(newIndexPage.pageBuffer);
//    newIndexPage.pageNumber = fileHandle_extra.getNumberOfPages() - 1;
    //TODO: merge

    newOverflowPageNumber = newIndexPage.pageNumber;
    newIndexPage.isPrimaryPage = false;
    newIndexPage.EntryCount = 0;
    newIndexPage.FreeSlotPointer =  SIZE_OF_PAGE_HEADER;
    newIndexPage.NextPagePtr = -1;

    appendEntryIntoPage(attribute, newIndexPage, entry);

    return 0;
}
RC IXFileHandle::deleteEntry(const Attribute &attribute, const unsigned index, const Entry &entry)
{
    //TODO: delete one then steal one & page by page
    RC rc = 0;
//    IndexPage indexPage;

//    switch(attribute.type)
//    {
//    case TypeReal:
//    case TypeInt:
//    if(loadPage( index, indexPage, TypePrimaryPage) == SUCCESS){
        // ** delete at primary page
        vector<Entry> entryList;
        vector<IndexPage> indexPageList;
        int deletedEntryCount = 0;

//        unsigned currentIndex = IndexManager::hashFunction(attribute, entry.key, g_N_OriginalNumberOfBuckets, g_currentLevel, g_nextSplitPtr);
        rc = loadAllBucketsByIndex(index, indexPageList);
        getWholeEntryList(attribute, indexPageList, entryList);

        vector<unsigned> overflowPageNumberToFill_V;
        int pagesCount = indexPageList.size();
        for(int i = 1 ; i < pagesCount ; i++)
        {
            overflowPageNumberToFill_V.push_back(indexPageList[i].pageNumber);
        }

        //        getEntryList(attribute, indexPage, entryList);

//        cout<<"[Del] original entryList.size():"<<entryList.size()<<endl;
        int keyLength = 0;
        if(attribute.type == TypeVarChar){
            keyLength = sizeof(int) + *(int*)entry.key;
        }else
            keyLength = sizeof(int);


        for( int i = entryList.size()-1 ; i >= 0  ; i-- )
        {
            int result = 0;
            result += abs(memcmp( entry.key, entryList[i].key, keyLength));
            result += abs(memcmp( &entry.rid.pageNum, &entryList[i].rid.pageNum, sizeof(int) ));
            result += abs(memcmp( &entry.rid.slotNum, &entryList[i].rid.slotNum, sizeof(int) ));
//            result += memcmp( &entry.rid, &entryList[i].rid, sizeof(RID) );
            if(result == 0)
            {
                entryList.erase(entryList.begin() + i);
                deletedEntryCount++;
            }
        }


//        Entry tempEntry;
//        int size = entryList.size();
//        for( int i = 0 ; i < size; i++)
//        {
//            tempEntry = entryList[i];
//            // debug: sizeof(entry)
//            memcpy( (char*)indexPage.pageBuffer + SIZE_OF_PAGE_HEADER + i*SIZE_OF_NUMERICAL_ENTRY, &entry , sizeof(Entry));
//        }

//        // update EntryCount
//        int newEntryCount = indexPage.EntryCount - deletedEntryCount;
//        memcpy(indexPage.pageBuffer, &newEntryCount , sizeof(int));
//
//        // update FreeSlotPointer
//        indexPage.FreeSlotPointer -= deletedEntryCount*SIZE_OF_NUMERICAL_ENTRY;
//        memcpy((int*)indexPage.pageBuffer+1, &indexPage.FreeSlotPointer , sizeof(int));

        if(deletedEntryCount > 0){
            // write page
//            fileHandle_primary.writePage( index , indexPage.pageBuffer);
            // update entry
            IndexPage previosPage;
            bool isPrimaryPageFilled = false;
            while(true){
                if(!isPrimaryPageFilled){
                    // for first page
//                    cout<<"[Del]0)entryList.size() from:"<<entryList.size()<<" to: ";
                    fillUpPage(index, entryList, attribute, TypePrimaryPage);
//                    cout<<entryList.size()<<" to index: "<<index<<endl;
                    isPrimaryPageFilled = true;
                    previosPage.isPrimaryPage = true;
                    previosPage.pageNumber = index;
                }else{
//                    cout<<"[Del]1)entryList.size() from:"<<entryList.size()<<" to: ";
                    fillUpPage(overflowPageNumberToFill_V[0], entryList, attribute, TypeOverflowPage);
//                    cout<<entryList.size()<<" to index: "<<overflowPageNumberToFill_V[0]<<endl;
                    previosPage.isPrimaryPage = false;
                    previosPage.pageNumber = overflowPageNumberToFill_V[0];
                    overflowPageNumberToFill_V.erase(overflowPageNumberToFill_V.begin());
                }
                if(entryList.size() == 0) break;
            }
            // set the last updated page's nextPagePtr to -1;
            updateNextPagePtr(previosPage , -1);

            // #recycle unused overflow page
            if(overflowPageNumberToFill_V.size()>0){
                int  mergeCount = overflowPageNumberToFill_V.size();
                releaseOverflowPages(overflowPageNumberToFill_V);
                for(int i = 0 ; i< mergeCount; i++)
                {
//                    merge(attribute);
                }
           }
            rc = 0;
        }else{ // no deleted entry then do not update page
            rc = -1;
        }
//    }else{ // fail to load primary index page
//        rc = -1;
//    }

//        break;
//    case TypeVarChar:
//        break;
//    }

//    free(indexPage.pageBuffer);
//    delete(&indexPage);
    return rc;
}

RC IXFileHandle::loadPage(const unsigned pageNumber, IndexPage &indexPage, const PageType pageType)
{
    RC rc = 0;

    // # make sure pageNumber is reasonable
    if(pageType == TypePrimaryPage  && pageNumber < 0)   return -1;
    if(pageType == TypeOverflowPage && pageNumber < 1)  return -1;

    // # read page data
    indexPage.pageBuffer = malloc(PAGE_SIZE);
    if     (pageType == TypePrimaryPage)
        rc = fileHandle_primary.readPage( pageNumber , indexPage.pageBuffer );
    else if(pageType == TypeOverflowPage)
        rc = fileHandle_extra.readPage( pageNumber , indexPage.pageBuffer );

    if(rc != SUCCESS)
        return rc;

    // # prepare page metadata
    indexPage.EntryCount        = *(unsigned*) indexPage.pageBuffer;
    indexPage.FreeSlotPointer   = *((unsigned*) indexPage.pageBuffer + 1);
    indexPage.NextPagePtr       = *((int*) indexPage.pageBuffer + 2);
    indexPage.FreeBytes         = PAGE_SIZE - indexPage.FreeSlotPointer;
    indexPage.pageNumber        = pageNumber;
    indexPage.FreeSlotsForNumerEntry = indexPage.FreeBytes / SIZE_OF_NUMERICAL_ENTRY; //suppose 340
    if    ( pageType == TypePrimaryPage)    indexPage.isPrimaryPage = true;
    else if(pageType == TypeOverflowPage)   indexPage.isPrimaryPage = false;

    return rc;
}

// fetch the overflow pages
RC IXFileHandle::loadOverflowPageList(const int firstOverflowPageNumber, vector<IndexPage> &indexPageList)
{
    RC rc = 0;

    //if first index == -1 means there is no overflow page related to the index
    if(firstOverflowPageNumber == -1) return -1;

    IndexPage tempIndexPage;
    int nextPagePtr = firstOverflowPageNumber;

    // # read all pages
    while(nextPagePtr != -1)
    {
        rc += loadPage(nextPagePtr, tempIndexPage, TypeOverflowPage);
        indexPageList.push_back(tempIndexPage);
        nextPagePtr = tempIndexPage.NextPagePtr;
    }

//    free(tempIndexPage.pageBuffer);
    return rc;
}
RC IXFileHandle::loadAllBucketsByIndex(const unsigned index, vector<IndexPage> &indexPageList)
{
    RC rc = 0;

    indexPageList.clear();

    // fetch the primary page
    IndexPage tempIndexPage;
    loadPage(index , tempIndexPage, TypePrimaryPage);
    indexPageList.push_back(tempIndexPage);

    // fetch the overflow pages
    if(tempIndexPage.NextPagePtr != -1)
        loadOverflowPageList(tempIndexPage.NextPagePtr , indexPageList);

//    free(tempIndexPage.pageBuffer);
    return rc;
}

RC IXFileHandle::getEntryList(const Attribute &attribute, IndexPage &indexPage, vector<Entry> &entryList)
{

    int entryLength = 0;
    int keyLength = 0;
    int strLength = 0;
    int offset = SIZE_OF_PAGE_HEADER;
    for(unsigned i = 0 ; i < indexPage.EntryCount ; i++)
    {
        Entry tempEntry;

        // calculate entryLength
        if(attribute.type == TypeVarChar){
            memcpy(&strLength , (char*)indexPage.pageBuffer + offset, sizeof(int));
            keyLength = sizeof(int) + strLength;
            entryLength = keyLength + sizeof(RID);
            tempEntry.key = malloc(sizeof(int)+strLength);
        }else{
            keyLength = sizeof(int);
            entryLength = SIZE_OF_NUMERICAL_ENTRY;
            tempEntry.key = malloc(sizeof(int));
        }

        // copy key
        memcpy(tempEntry.key , (char*)indexPage.pageBuffer + offset, keyLength);
        // copy rid
        memcpy(&tempEntry.rid , (char*)indexPage.pageBuffer + keyLength + offset, sizeof(RID));

        entryList.push_back(tempEntry);

//        char *tmp = IndexManager::Varchart2CString(tempEntry.key);
//        cout<<"<<"<<tmp<<">>"<<endl;


//        free(tempEntry.key);
        offset += entryLength;
    }

    return 0;
}
RC IXFileHandle::getWholeEntryList(const Attribute &attribute, vector<IndexPage> &indexPageList, vector<Entry> &entryList)
{
    IndexPage tempIndexPage;
    int pageCount = indexPageList.size();

    for(int i = 0 ; i < pageCount ; i++ ){
        tempIndexPage = indexPageList[i];
        getEntryList(attribute, tempIndexPage , entryList);
    }

    return 0;
}
RC IXFileHandle::printIndexEntriesInAPage(const Attribute &attribute, const unsigned &primaryPageNumber)
{
    unsigned totalEntries = 0;
    vector<IndexPage> indexPageList;
    vector<Entry> entryList;
    loadAllBucketsByIndex(primaryPageNumber, indexPageList);
    int bucketsCount = indexPageList.size();

    for(int i = 0 ; i < bucketsCount ; i++)
    {
        totalEntries += indexPageList[i].EntryCount;
    }
    printf("Number of total entries in the page (+ overflow pages) : %d \n" , totalEntries);

    for(int i = 0 ; i < bucketsCount ; i++)
    {
        entryList.clear();

        if(i == 0){ // primary page
            printf("primary Page No.%d \n" , primaryPageNumber);
        }else{
            printf("overflow Page No.%d \n" , indexPageList[i].pageNumber);
            if(i == 1) // the 1st overflow page
                printf("linked to primary page %d \n" , primaryPageNumber);
            else if(i > 1)
                printf("linked to overflow page %d \n" , indexPageList[i-1].pageNumber);
        }
        printf("  # of entries : %d \n" , indexPageList[i].EntryCount);

        entryList.clear(); ////////
        getEntryList(attribute, indexPageList[i] , entryList);
        printf("  entries: ");
        for(unsigned j = 0 ; j < indexPageList[i].EntryCount ; j++)
        {
            if(attribute.type == TypeInt)
                printf("[%d/%d,%d] ", *(int*)entryList[j].key , entryList[j].rid.pageNum , entryList[j].rid.slotNum);
            else if(attribute.type == TypeReal)
                printf("[%f/%d,%d] ", *(float*)entryList[j].key , entryList[j].rid.pageNum , entryList[j].rid.slotNum);
            else if(attribute.type == TypeVarChar){
                char* tempCStr = IndexManager::Varchart2CString(entryList[j].key);
                printf("[%s/%d,%d] ", tempCStr , entryList[j].rid.pageNum , entryList[j].rid.slotNum);
            }
        }
        printf("\n\n");
    }
    return 0;
}

RC IXFileHandle::printIndexEntriesCountInAPage(const Attribute &attribute, const unsigned &primaryPageNumber)
{
    unsigned totalEntries = 0;
    vector<IndexPage> indexPageList;
    vector<Entry> entryList;
    loadAllBucketsByIndex(primaryPageNumber, indexPageList);
    int bucketsCount = indexPageList.size();

    for(int i = 0 ; i < bucketsCount ; i++)
    {
        totalEntries += indexPageList[i].EntryCount;
    }
    printf("Number of total entries in the page (+ overflow pages) : %d \n" , totalEntries);

    for(int i = 0 ; i < bucketsCount ; i++)
    {
        entryList.clear();
        if(i == 0){ // primary page
            printf("primary Page No.%d \n" , primaryPageNumber);
        }else{
            printf("overflow Page No.%d \n" , indexPageList[i].pageNumber);
            if(i == 1) // the 1st overflow page
                printf("linked to primary page %d \n" , primaryPageNumber);
            else if(i > 1)
                printf("linked to overflow page %d \n" , indexPageList[i-1].pageNumber);
        }
        printf("  # of entries : %d \n" , indexPageList[i].EntryCount);

        printf("\n\n");
    }
    return 0;
}
bool IXFileHandle::sortOperate_float(Entry i, Entry j)
{
    bool result = false;
    // convert and compare key
    float i_float,j_float;
    i_float = *(float *)i.key;
    j_float = *(float *)j.key;
    result = i_float < j_float;

    return result;
}
bool IXFileHandle::sortOperate_int(Entry i, Entry j)
{
    bool result = false;
    // convert and compare key
    int i_int, j_int;
    i_int = *(int *)i.key;
    j_int = *(int *)j.key;
    result = i_int < j_int;

    return result;
}
bool IXFileHandle::sortOperate_string(Entry i, Entry j)
{

    bool result = false;
    // convert and compare key
    //take care of length issue when insert
    string i_string = string(IndexManager::Varchart2CString(i.key));
    string j_string = string(IndexManager::Varchart2CString(j.key));
    result = i_string < j_string;

    return result;
}
RC IXFileHandle::sortEntryList(vector<Entry> entryList, Attribute attribute)
{
    if(attribute.type == TypeReal)
        std::sort(entryList.begin(),entryList.end(), sortOperate_float);
    else if(attribute.type == TypeInt)
        std::sort(entryList.begin(),entryList.end(), sortOperate_int);
    else if(attribute.type == TypeVarChar)
        std::sort(entryList.begin(),entryList.end(), sortOperate_float);


//    for(int i = 0 ; i < entryList.size() ; i++)
//    {
//        cout<<"SSSSSSSSSS"<<entryList[i].rid.pageNum<<endl;
//    }
//    return 0;
}
void IX_PrintError (RC rc)
{
}

/* How to delete a struct

//Free newVideSample->buffer if it was allocated using malloc
free((void*)(newVideSample->buffer));

//if it was created with new, use `delete` to free it
delete newVideSample->buffer;

//Now you can safely delete without leaking any memory
delete newVideSample;

*/
