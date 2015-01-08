#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <math.h>
#include <algorithm>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"  //for FileHandle

#define IX_EOF (-1)  // end of the index scan

//+++
#define SUCCESS 0

//+++

typedef enum { TypePrimaryPage = 0, TypeOverflowPage } PageType;

struct Entry {
    void *key;
    RID rid; 
};
struct IndexPage {
    int NextPagePtr; // if(-1) there is no another overflow page, else point to the extra file page#
    unsigned FreeSlotPointer; // point to the first free slot
    unsigned EntryCount; // how many entries in this page
    unsigned FreeSlotsForNumerEntry; // how many free slots in this page for int and float
    unsigned FreeBytes; // for varchar calculation
    unsigned pageNumber;
    bool isPrimaryPage;
    void* pageBuffer;
};


class IX_ScanIterator;
class IXFileHandle;


class IndexManager {
 public:
  static IndexManager* instance();

  // Create index file(s) to manage an index
  RC createFile(const string &fileName, const unsigned &numberOfPages);

  // Delete index file(s)
  RC destroyFile(const string &fileName);

  // Open an index and returns an IXFileHandle
  RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

  // Close an IXFileHandle. 
  RC closeFile(IXFileHandle &ixfileHandle);


  // The following functions  are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For INT and REAL: use 4 bytes to store the value;
  //     For VarChar: use 4 bytes to store the length of characters, then store the actual characters.

  // Insert an entry to the given index that is indicated by the given IXFileHandle
  RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // Delete an entry from the given index that is indicated by the given IXFileHandle
  RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // scan() returns an iterator to allow the caller to go through the results
  // one by one in the range(lowKey, highKey).
  // For the format of "lowKey" and "highKey", please see insertEntry()
  // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
  // should be included in the scan
  // If lowKey is null, then the range is -infinity to highKey
  // If highKey is null, then the range is lowKey to +infinity
  
  // Initialize and IX_ScanIterator to supports a range search
  RC scan(IXFileHandle &ixfileHandle,
      const Attribute &attribute,
	  const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator);

  // Generate and return the hash value (unsigned) for the given key
  unsigned hash(const Attribute &attribute, const void *key);
  
  
  // Print all index entries in a primary page including associated overflow pages
  // Format should be:
  // Number of total entries in the page (+ overflow pages) : ?? 
  // primary Page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx] [xx]
  // overflow Page No.?? liked to [primary | overflow] page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx]
  // where [xx] shows each entry.
  RC printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber);
  
  // Get the number of primary pages
  RC getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages);

  // Get the number of all pages (primary + overflow)
  RC getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages);
  

  //+++
  // VarChar: use 4 bytes to store the length of characters, then store the actual characters.
  // CString: char* with the '\0' in the end.
  static char* Varchart2CString(const void *recordBasedStr);
  static int pow(int base, int power);
  static unsigned hashFunction_int(int key_int, unsigned g_N_OriginalNumberOfBuckets, unsigned g_currentLevel, unsigned g_nextSplitPtr);
  static unsigned hashFunction(const Attribute &attribute, const void *key, unsigned g_N_OriginalNumberOfBuckets, unsigned g_currentLevel, unsigned g_nextSplitPtr);

 protected:
  IndexManager   ();                            // Constructor
  ~IndexManager  ();                            // Destructor

 private:
  static IndexManager *_index_manager;

  //+++
  unsigned g_N_OriginalNumberOfBuckets;
  unsigned g_currentLevel;
  unsigned g_nextSplitPtr;
  unsigned g_bottomLinePtr;

};


class IXFileHandle {
public:
	// Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    IXFileHandle();  							// Constructor
    ~IXFileHandle(); 							// Destructor
    
    static RC createFile (const string &fileName, const unsigned &numberOfPages);
    static RC destroyFile(const string &fileName);

    RC openFile(const string &fileName);
    RC closeFile();
    RC getNumberOfAllPages(unsigned &numberOfAllPages);
    RC getNumberOfPrimaryPages(unsigned &numberOfPrimaryPages);

    //+++
    static RC prepareIndexPage(void *bufferPage);
    static RC prepareInitMetadataPage(void *pageBuffer, unsigned initNumberOfPages);
    RC loadMetadata(unsigned &g_N_OriginalNumberOfBuckets, unsigned &g_currentLevel, unsigned &g_nextSplitPtr, unsigned &g_bottomLinePtr);
    RC updateMetadata();

    // Print
    RC printIndexEntriesInAPage(const Attribute &attribute, const unsigned &primaryPageNumber);
    RC printIndexEntriesCountInAPage(const Attribute &attribute, const unsigned &primaryPageNumber);

    // Split
    RC split(const Attribute &attribute);
    RC redistributeEntries(const unsigned splitIndex, const Attribute &attribute);
    RC fillUpPage(const unsigned index, vector<Entry> &entryList, const Attribute &attribute, const PageType pageType);
//    RC bulkUpdateEntries(IndexPage &indexPage, const Attribute &attribute, vector<Entry> &entryList);
    RC updateNextPagePtr(IndexPage &previosPage, const int nextPagePtr);
    unsigned allocateNewOverflowPageNumber();
    RC releaseOverflowPages(vector<unsigned> &pageNumbers);

    // Merge
    RC merge(const Attribute &attribute);


    // Sort
    static bool sortOperate_float(Entry i, Entry j);
    static bool sortOperate_int(Entry i, Entry j);
    static bool sortOperate_string(Entry i, Entry j);
    RC sortEntryList(vector<Entry> entryList, Attribute attribute);

    // Insert
    RC insertManagement( const Attribute &attribute , const unsigned index, const Entry &entry);
    RC appendEntryIntoPage(const Attribute &attribute, IndexPage &indexPage, const Entry &entry);
    RC appendNewOverflowPageWithEntry(const Attribute &attribute, unsigned &newOverflowPageNumber, const Entry &entry);

    RC deleteEntry( const Attribute &attribute , const unsigned index, const Entry &entry);

//    RC getAllEntriesByIndexForNumerical(const unsigned indexNumber, vector<Entry> &entryList);
    RC loadAllBucketsByIndex(const unsigned index, vector<IndexPage> &indexPageList);
    RC loadPage(const unsigned index, IndexPage &indexPage, const PageType pageType);
    RC loadOverflowPageList(const int index, vector<IndexPage> &indexPageList);
    RC loadOverflowPage(const int overflowPageNumber, IndexPage &indexPage);
    RC getEntryList(const Attribute &attribute, IndexPage &indexPage, vector<Entry> &entryList);
    RC getWholeEntryList(const Attribute &attribute, vector<IndexPage> &indexPageList, vector<Entry> &entryList);

    //+++
    unsigned g_N_OriginalNumberOfBuckets;
    unsigned g_currentLevel;
    unsigned g_nextSplitPtr;
    unsigned g_bottomLinePtr;
    static const int SIZE_OF_NUMERICAL_ENTRY = 3 * sizeof(int); // key + rid
    static const unsigned SIZE_OF_PAGE_HEADER = 3 * sizeof(int);

private:
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;

    //+++
    FileHandle fileHandle_primary;
    FileHandle fileHandle_extra;
};

class IX_ScanIterator {
 public:
  IX_ScanIterator();                            // Constructor
  ~IX_ScanIterator();                           // Destructor

  RC getNextEntry(RID &rid, void *key);         // Get next matching entry
  RC close();                                   // Terminate index scan

  //+++
  bool compare(int key);
  bool compare(float key);
  bool compare(string key);

  //+++
  IXFileHandle *ixfileHandlePtr;
  Attribute attribute;
  const void *lowKey;
  const void *highKey;
  bool  lowKeyInclusive;
  bool  highKeyInclusive;
  unsigned currentIndex;
  unsigned currentEntryPtrOfList;
  unsigned maxIndex;
  unsigned maxEntryPtr;
  bool ifMoveToNextIndex;
  bool ifFirstIteration;
  vector<IndexPage> indexPageList;
  vector<Entry> entryList;

  unsigned ixs_N_OriginalNumberOfBuckets;
  unsigned ixs_currentLevel;
  unsigned ixs_nextSplitPtr;
//  unsigned ixs_bottomLinePtr;

};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
