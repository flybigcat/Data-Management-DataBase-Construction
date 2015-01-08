#include "rbfm.h"

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
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

/* This method creates a record-based file called fileName.
 * The file should not already exist.
 * Please note that this method should internally use the method PagedFileManager::createFile (const char *fileName).
 * */
RC RecordBasedFileManager::createFile(const string &fileName)
{
	return pfm->createFile(fileName.c_str());
}

/* This method destroys the record-based file whose name is fileName.
 * The file should exist.
 * Please note that this method should internally use the method PagedFileManager::destroyFile (const char *fileName).
 * */
RC RecordBasedFileManager::destroyFile(const string &fileName)
{
    return pfm->destroyFile(fileName.c_str());
}

/* This method opens the record-based file whose name is fileName.
 *  The file must already exist
 *  and it must have been created using the RecordBasedFileManager::createFile method.
 *  If the method is successful, the fileHandle object whose address is passed as a parameter becomes a "handle" for the open file.
 *  The file handle rules in the method PagedFileManager::openFile applies here too.
 *  Also note that this method should internally use the method PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle).
*/
RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return pfm->openFile(fileName.c_str(), fileHandle);
}

/* This method closes the open file instance referred to by fileHandle.
 * The file must have been opened using the RecordBasedFileManager::openFile method.
 * Note that this method should internally use the method PagedFileManager::closeFile(FileHandle &fileHandle).
 * */
RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
    return pfm->closeFile(fileHandle);
}

/* Given a record descriptor, insert a record into a given file identifed by the provided handle.
 * You can assume that the input is always correct and free of error.
 * That is, you do not need to check if the input record has the right number of attributes and if the attribute types match.
 * Furthermore, you may use system-sequenced file organization.
 * That is, find the first page with free space large enough to store the record and store the record at that location.
 * RID here is the record ID which is used to uniquely identify records in a file.
 * An RID consists of: 1) the page number that the record resides in within the file, and
 * 2) the slot number that the record resides in within the page.
 * The insertRecord method accepts an RID object and fills it with the RID of the record that is target for insertion.
 * */
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
	//actual data length
	int dataLength = getDataLength(recordDescriptor, data);
	//number of record elements != fields, for varchar, there are two elements represent this one field, e.g. Tom, in the disk, (3, Tom), 3 is the number of characters of Tom
	int numberOfRecordElements = getNumberOfRecordElements(recordDescriptor, data);
	//number of record element + header(total element #)
	int sizeOfRecordHeader = (1 + numberOfRecordElements) * sizeof(int);

	void * buffer = malloc(PAGE_SIZE);

	//if the file has enough free space for dataLength
	//read this page into buffer and
	//organize this page: insert data, rewrite free space pointer, increase slot number, put slotcell inside
		
	int pageCount = fileHandle.getNumberOfPages();

	if(pageCount > 0){
		//find a page i has enough free space
		for(int i = 0; i < pageCount; i++){
			//read into buffer
			if(fileHandle.readPage(i, buffer)!=0)
			{
				free(buffer);
				return -1;
			}

			// free space pointer is the physical address offset from the very beginning of the page to the "free space"
			int freeSpacePointer;
			freeSpacePointer = * (int * )((char * )buffer+PAGE_SIZE-sizeof(int));

			unsigned totalNumberOfSlots;
			totalNumberOfSlots = * (int *)((char *)buffer + PAGE_SIZE-2*sizeof(int));

			int freeSpace = PAGE_SIZE - freeSpacePointer - 2 * sizeof(int) - totalNumberOfSlots * sizeof(SlotCell);

			//check if free space enough
			if(freeSpace >= (dataLength + sizeof(SlotCell) + sizeOfRecordHeader))
			{
				//insert header
			    	void * recordHeader=malloc(sizeOfRecordHeader) ;
				//make a header to this record by using a method "recordHeaderMaker"
				recordHeaderMaker(recordDescriptor, data, recordHeader);
				memcpy((char *)buffer +freeSpacePointer, recordHeader, sizeOfRecordHeader);
				//insert data
				//void * memcpy ( void * destination, const void * source, size_t num );
				memcpy( (char *)buffer +freeSpacePointer+sizeOfRecordHeader, data, dataLength);
                
                		//make a new slot cell
                		SlotCell sc;
                		sc.length = sizeOfRecordHeader + dataLength;
                		sc.offset = freeSpacePointer;
                
                		//check if there is a "deleted" slot directory for this slot cell
                		bool reused = false;
                
                		for(int j = 1; j <= totalNumberOfSlots; j ++)
                		{
                    			int offset = * (int *)((char *)buffer + PAGE_SIZE - 2*sizeof(int) - j * sizeof(SlotCell) );
                    
                    			//TODO: delete -1
                    			//there is a delete slot cell one can be resused
                    			if(offset == R_Del)
                    			{
						//re use this slot cell
                        			memcpy((char *)buffer + PAGE_SIZE - 2*sizeof(int) - j*sizeof(SlotCell), &sc, sizeof(SlotCell));
                        			reused = true;
						break;

                    			}
                		}
                		
				//there is no deleted slot cell can be reused
                		if(!reused)
                		{
                    			//increase total slot number
                    			totalNumberOfSlots +=1;
                    			memcpy((char *)buffer + PAGE_SIZE - 2*sizeof(int), &totalNumberOfSlots, sizeof(int));
                
                    			//add a new slot cell in the slot directory
                    			memcpy((char *)buffer + PAGE_SIZE - 2*sizeof(int) - totalNumberOfSlots*sizeof(SlotCell), &sc, sizeof(SlotCell));
                		}

                
                		//move fsp
                		freeSpacePointer += dataLength + sizeOfRecordHeader;
                		memcpy((char *)buffer + PAGE_SIZE - sizeof(int), &freeSpacePointer, sizeof(int));
                
				//write "dirty" page back to disk
				if(fileHandle.writePage(i, buffer) != 0)
				{
					free(buffer);
					return -1;
				}

				//get RID
				rid.pageNum = i;
				rid.slotNum = totalNumberOfSlots;

				//all done
				free(recordHeader);
				free(buffer);
				return 0;

			}//end of if

			//else break;
		}//end of for
	}//end of if

	//if fileHandle's file is empty
	//or all the page don't have enough free space
	//append a new page

	//insert record header
	void * recordHeader=malloc(sizeOfRecordHeader) ;
	recordHeaderMaker(recordDescriptor, data, recordHeader);
	memcpy((char *)buffer, recordHeader, sizeOfRecordHeader);

	//insert data
	memcpy((char *)buffer + sizeOfRecordHeader, data, dataLength);

	//initialization of header/slot directory
	int freeSpacePointer;
	freeSpacePointer = dataLength + sizeOfRecordHeader;
	memcpy((char *)buffer + PAGE_SIZE - sizeof(int), &freeSpacePointer, sizeof(int));

	int totalNumberOfSlots =1;
	memcpy((char *)buffer + PAGE_SIZE -2*sizeof(int), &totalNumberOfSlots, sizeof(int));

	SlotCell sc;
	sc.length = sizeOfRecordHeader + dataLength;
	sc.offset = 0;
	memcpy((char *)buffer + PAGE_SIZE - 2*sizeof(int) - totalNumberOfSlots*sizeof(SlotCell), &sc, sizeof(SlotCell));

	//write "dirty" new page append to file back to disk
	if(fileHandle.appendPage(buffer) != 0 )
	{
		free(buffer);
		free(recordHeader);
		return -1;
	}

	rid.pageNum = fileHandle.getNumberOfPages()-1;
	rid.slotNum = 1;

	free(buffer);
	free(recordHeader);

	return 0;
}

/*Given a record descriptor, read the record identified by the given rid.*/
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
	void * buffer = malloc(PAGE_SIZE);
        
	//check if this page of rid exist 
	if(fileHandle.readPage(rid.pageNum, buffer) != 0) //not exist this record
	{
		cout << "can't read page:" << rid.pageNum << endl;
		free(buffer);
		return -1;
	}

	unsigned totalNumberOfSlots;
	totalNumberOfSlots = * (int *)((char *)buffer + PAGE_SIZE - 2* sizeof(int));
	
	if(totalNumberOfSlots >= rid.slotNum)
	{
		int offset;

		SlotCell sc;
		sc.offset = * (int *)((char *)buffer + PAGE_SIZE - 2*sizeof(int) - rid.slotNum * sizeof(SlotCell) );

                if(sc.offset == -1)  //this record has been deleted;
                {
                         free(buffer);
                         return -1;
                }

                int numberofattr = *(int *)((char*)buffer + sc.offset);

		//tombstone---R_Tombstone
                if( numberofattr ==-2 )
		{
			//read tombstone infor, new rid: page number, slot number
                	int rpagenumber=*(int *)((char*)buffer + sc.offset+sizeof(int));
                	int rslotnumber=*(int *)((char*)buffer + sc.offset+2*sizeof(int));
			
			//This buffer1 is for the tombstone pointed "new" page
			void *buffer1 = malloc(PAGE_SIZE);

                	if(fileHandle.readPage(rpagenumber, buffer1)!=0)
                	{
                        	free(buffer1);
				free(buffer);
                        	return -1;
                	}
                	int totalNumberOfSlots1 = * (int *)((char *)buffer1 + PAGE_SIZE - 2* sizeof(int));
                	if(totalNumberOfSlots1 >=rslotnumber)
			{
				SlotCell sc1;
                        	sc1.offset = * (int *)((char *)buffer1 + PAGE_SIZE - 2*sizeof(int) - rslotnumber * sizeof(SlotCell) );
                        	offset = ((*(int *)((char*)buffer1 + sc1.offset)) +1)*sizeof(int) + sc1.offset;
				int dataLength = getDataLength(recordDescriptor, (char*)buffer1 + offset);
                        	memcpy(data, (char*)buffer1 + offset, dataLength);
				free(buffer1);
				free(buffer);
                        	return 0;
                	}
			else{
				free(buffer);
				free(buffer1);
				return -1;
			}
                }//end if tombstone condition
		
		//else, not tombstone condition, normal condition
                else
		{
			//This offset is pointing to actual data position, 1st term is for the headerlength, 2nd term is for the record header pointer
			offset = ((*(int *)((char*)buffer + sc.offset) )+1)*sizeof(int) + sc.offset;
			int dataLength = getDataLength(recordDescriptor, (char*)buffer + offset);
			memcpy(data,  (char*)buffer + offset, dataLength);

			free(buffer);
			return 0;
		}
	}

	free(buffer);
	return -1;
}

//This method is for get actual data length
int RecordBasedFileManager::getDataLength(const vector<Attribute> &recordDescriptor, const void *data){
	int dataLength = 0;

	for(int i = 0; i < recordDescriptor.size(); i ++)
	{
		//check each element type: AttrType
		switch (recordDescriptor[i].type) {

		case TypeInt:
				dataLength+=sizeof(int);
				break;

		case 1:
				dataLength+=sizeof(int);
				break;

		case 2:
				int * aa;
				aa = (int *)((char *)data+dataLength);
				int length = *aa;

				dataLength+=length+sizeof(int);
				break;

		}//end of switch

	}//end of for each
	return dataLength;
}

//This method is for get number of record elements, elements != fields, varchar has two elements represent it (number of characters, varchar itself)
int RecordBasedFileManager::getNumberOfRecordElements(const vector<Attribute> &recordDescriptor, const void *data)
{
	int numberOfRecordElements = 0;

	 for(int i = 0; i < recordDescriptor.size(); i ++)
   	 {
       		 //check each element type: AttrType
		switch (recordDescriptor[i].type)
		 {
	
       		 case 0:
               		 numberOfRecordElements++;
               		 break;

        	case 1:
                	numberOfRecordElements++;
               		 break;

       		case 2:
               		 numberOfRecordElements+=2;
               		 break;
       		 }//end of switch
    	}//end of for each

	    return numberOfRecordElements;
}

//This method is to make a header to a record
//1st, number of element, 2nd -, offset of each elements,(actual data)
//e.g. (3, Tom, 30, 170.5)
//(5, offset for 3, offset for Tom, offset for 30, offset for 170.5) This is header
//Here, the offset is the beginning of this record to the specific element
RC RecordBasedFileManager::recordHeaderMaker(const vector<Attribute> &recordDescriptor, const void *data, void *recordHeader)
{
		int numberOfRecordElements = getNumberOfRecordElements(recordDescriptor, data);
		int sizeOfRecordHeader = (1 + numberOfRecordElements) * sizeof(int);

		memcpy(recordHeader, &numberOfRecordElements, sizeof(int));

		int offset = 0;
		int attrPointer;
		int j = 1;

		//depending on each field/attr type, make 1/2 offset/offsets for it
		for(int i = 0; i < recordDescriptor.size(); i ++)
			{
//				cout<< i<<endl;
				//check each element type: AttrType
				switch (recordDescriptor[i].type) {

				case 0:
						attrPointer = sizeOfRecordHeader + offset;
						memcpy((char * )recordHeader + j*sizeof(int), &attrPointer, sizeof(int));
						offset+=sizeof(int);
						j++;
						break;

				case 1:
						attrPointer = sizeOfRecordHeader+ offset;
						memcpy((char * )recordHeader + j*sizeof(int), &attrPointer, sizeof(int));
						offset+=sizeof(int);
						j++;
						break;

				case 2:
					    int * aa;
						aa = (int *)((char *)data+offset);
						int length = *aa;

						attrPointer = sizeOfRecordHeader + offset;
						memcpy((char * )recordHeader + j*sizeof(int), &attrPointer, sizeof(int));
					        offset+=sizeof(int);
						j++;

						attrPointer = sizeOfRecordHeader + offset;
						memcpy((char * )recordHeader + j*sizeof(int), &attrPointer, sizeof(int));
						offset+=length;
						j++;
						break;

				}//end of switch

			}//end of for each

	return 0;
}

/* This is a utility method that will be mainly used for debugging/testing.
 * It should be able to interpret the bytes of each record using the passed record descriptor
 * and then print its content to the screen.
 * For instance suppose a record consists of two fields: int and float,
 * Which means the record will be of size 8
 * (ignoring any other metadata information that might be kept with every record).
 * The printRecord method will be able to recognize the record format using the record descriptor.
 * It then will be able to convert the first four bytes to an int object
 * and the last four bytes to a float object and print their values.
 * */
RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
	int offset = 0;

	int * aa;
	float * bb;
	char * cc;

	for(int i = 0; i < recordDescriptor.size(); i ++)
	{
		//check each element type: AttrType
		switch (recordDescriptor[i].type) {

		case 0:
				aa = (int * )((char *)data+offset);
				cout << *aa<<endl;
				offset+=sizeof(int);
				break;

		case 1:
				bb = (float * )((char *)data+offset);
				cout <<  *bb <<endl;
				offset+=sizeof(int);
				break;

		case 2:
				aa = (int *)((char *)data+offset);
				int length = *aa;

//				cout << *aa <<endl;

				cc = ((char *)data+offset+sizeof(int));
				for(int j = 0; j < length; j++)
				{
					cout << *cc;
					cc++;
				}
				cout << endl;

				offset+=length+sizeof(int);
				break;


		}//end of switch

	}//end of for each

	cout << endl;
    return 0;
}

/* Delete all records in the file.*/
RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle)
{
	//getNumberOfPages
    int numberOfPages = fileHandle.getNumberOfPages();

	//remove everything in these pages
    void * pageBuffer = malloc(PAGE_SIZE);
    const int zero = 0;
    const int offset_EndOfPage = PAGE_SIZE - sizeof(int);
    const int offset_SlotsCount = PAGE_SIZE - sizeof(int)*2;

    //TODO: set all slot offset to -1
    for(int i = 0; i < numberOfPages; i++){
        fileHandle.readPage(i, pageBuffer);

        //set the slot number to zero
        memcpy((char*)pageBuffer + offset_SlotsCount, &zero , sizeof(int));

       //set the free space pointer to the beginning of page
        memcpy((char*)pageBuffer + offset_EndOfPage, &zero , sizeof(int));

        fileHandle.writePage(i, pageBuffer);
    }
    free(pageBuffer);
	return 0;
}

/* Given a record descriptor, delete the record identified by the given rid.*/
//This is not really "delete" the record, but set slotcell.offset to -1, tells the world this record is gone
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    //read this page (rid.pageNum) into buffer
    void * pageBuffer = malloc(PAGE_SIZE);
    
    //readPage fail
    if(fileHandle.readPage(rid.pageNum, pageBuffer) != 0 )
    {
        free(pageBuffer);
        return -1;
    }
    
    const int offset_SlotCell =  *(int *)((char *)pageBuffer + PAGE_SIZE - 2*sizeof(int) - rid.slotNum * sizeof(SlotCell));
    unsigned totalNumberOfSlots = *(int*)((char *)pageBuffer + PAGE_SIZE - sizeof(int)*2);

    //slotNum out of range
    if( totalNumberOfSlots < rid.slotNum )
    {
        free(pageBuffer);
        return -1;
    }

	//delete this part--only set the sc.offset to -1
    const int negativeOne = -1;
    memcpy((char*)pageBuffer +  PAGE_SIZE - 2*sizeof(int) - rid.slotNum * sizeof(SlotCell), &negativeOne , sizeof(int));

	//write this page back to disk
    if(fileHandle.writePage(rid.pageNum, pageBuffer) != 0)
    {
        free(pageBuffer);
        return -1;
    }

    free(pageBuffer);
	return 0;
}

/* Given a record descriptor, update the record identified by the given rid with the passed data.
 * If the record grows and there is no space in the page to store the record,
 * the record is migrated to a new page with enough free space.
 * Since you will soon be implementing a B-tree structure or any indexing mechanism,
 * assume that records are identified by their rids and when they migrate,
 * they leave a tombstone behind pointing to the new location of the record.*/
//Assume the rid does not change after update
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    //read this page (rid.pageNum) into buffer
    void * pageBuffer = malloc(PAGE_SIZE);
    //readPage fail or slotNum out of range
    if(fileHandle.readPage(rid.pageNum, pageBuffer)!=0)
    {
        free(pageBuffer);
        return -1;
    }
    
    //calculate new data info
    int updatedDataLength = getDataLength(recordDescriptor, data);
    int numberOfRecordElements = getNumberOfRecordElements(recordDescriptor, data);
    int sizeOfRecordHeader = (1 + numberOfRecordElements) * sizeof(int);
    int updatedRecordSize = updatedDataLength + sizeOfRecordHeader;
    
    void * recordHeader=malloc(sizeOfRecordHeader) ;
    recordHeaderMaker(recordDescriptor, data, recordHeader);
    
    //slotcell
    SlotCell sc;
    sc.offset = * (int *)((char *)pageBuffer + PAGE_SIZE - 2*sizeof(int) - rid.slotNum * sizeof(SlotCell));
    sc.length = * (int *)((char *)pageBuffer + PAGE_SIZE - 2*sizeof(int) - rid.slotNum * sizeof(SlotCell) + sizeof(int));
   	//get dataLength by slotcell.length, slotNum = rid.slotNum
    int oldRecordSize = sc.length;
    
	//check if newDataLength <= dataLength
	//write this record in the same "offset" position
    if(updatedRecordSize <= oldRecordSize)
    {
        //change datalength to new length
        sc.length = updatedRecordSize;
        
        //record header
        memcpy((char *)pageBuffer + sc.offset, recordHeader, sizeOfRecordHeader);
        //record
        memcpy((char *)pageBuffer + sc.offset + sizeOfRecordHeader, data, updatedDataLength);
        //slot cell
        memcpy((char *)pageBuffer + PAGE_SIZE - 2*sizeof(int) - rid.slotNum*sizeof(SlotCell), &sc, sizeof(SlotCell));
    }

    //check if newDataLength > dataLength
    //put tombstone in the old reocord position: tombstone rid = insertRecord()
    if(updatedRecordSize > oldRecordSize)
    {
        //put the "longer" size record to another place and get rid back newRid
        RID newRid;
        if(insertRecord(fileHandle, recordDescriptor, data, newRid)!=0)
            return -1;
        
        //make a tombstone
        //R_Tombstone = -2
        RecordStatus rs = R_Tombstone;
        memcpy((char *)pageBuffer + sc.offset, &rs, sizeof(int));
        memcpy((char *)pageBuffer + sc.offset + sizeof(int), &newRid, sizeof(RID));
        
        //slot cell
        sc.length = sizeof(RID) + sizeof(int);
        memcpy((char *)pageBuffer + PAGE_SIZE - 2*sizeof(int) - rid.slotNum*sizeof(SlotCell), &sc, sizeof(SlotCell));
    }
    
    //write "dirty" page back to disk
    if(fileHandle.writePage(rid.pageNum, pageBuffer) != 0)
    {
        free(pageBuffer);
	free(recordHeader);
        return -1;
    }
        free(pageBuffer);
        free(recordHeader);    
	return 0;
}

//This method is  used to get the field/attribute position from recordDescriptor and attributeName
int ReadattributeinRecord(const vector<Attribute> &recordDescriptor, const string attributeName, void *data, void *rq){
        //get the field/attribute position from recordDescriptor and attributeName
        //int ind=0; // the position of the attibuteName
        int cwm=0; //check whether a attribute name match the input  attributeName
        int offset=0; // the type of required attribute
	int attrlength=0;
	
	
        for(int i = 0; i < recordDescriptor.size(); i ++)
        {
		switch(recordDescriptor[i].type) 
		{
			case 0:
				attrlength = sizeof(int);
				break;
			case 1:
				attrlength = sizeof(float);
				break;
			case 2:
				int numv=*(int*)((char*)rq+offset);
				attrlength = numv * sizeof(char) + sizeof(int);
				break;
		}
		//find it!
        	if(recordDescriptor[i].name==attributeName)
		{
                        cwm=1;
			memcpy(data, (char *)rq+offset, attrlength);     

			break;
                }
		else
		{
			offset+=attrlength;
		}
        }

	//return the  length                                                                   	                                                     
	return attrlength;
}

/* Given a record descriptor, read a specific attribute of a record identified by a given rid.*/
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data)
{
    void * rq = malloc(PAGE_SIZE); //record corresponding to the rid 
    readRecord(fileHandle, recordDescriptor, rid, rq);
    ReadattributeinRecord(recordDescriptor, attributeName, data, rq);
    free(rq);
    return 0;                    	
}

/* Given a record descriptor, reorganize a page, i.e., push the free space towards the end of the page.*/
//read record by record, pay attention here, rid not change, so the slot directory not change much, only fsp and offset of each slot
RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber)
{
	//recordDescriptor is not used.
	//check if this pageNumber exist or not(getNumberOfPages)
	int pageCount = fileHandle.getNumberOfPages();
	
	if(pageNumber > pageCount-1)
	{
		cout << "Error: page not exist!" << endl;
		return -1;
	}
	if(pageCount == 0)	
	{
		cout << "No page available" << endl;
		return -1;	
	}

	//if exists
	//read this page into RAM buffer
	void * pageBuffer = malloc(PAGE_SIZE);
	if(fileHandle.readPage(pageNumber, pageBuffer)!=0)
        {
  		free(pageBuffer);
   		return -1;
    	}

	//allocate another buffer2 in RAM
	void * pageBuffer2 = malloc(PAGE_SIZE); //new page
	
	//copy the whole slot directory first, if any change, update it later
	unsigned totalNumberOfSlots;
        totalNumberOfSlots = * (int *)((char *)pageBuffer + PAGE_SIZE - 2* sizeof(int));

	unsigned lengthOfSlotRecord;
	lengthOfSlotRecord=totalNumberOfSlots*sizeof(SlotCell)+2*sizeof(int);

	memcpy((char *)pageBuffer2+PAGE_SIZE-lengthOfSlotRecord, (char *)pageBuffer+PAGE_SIZE-lengthOfSlotRecord, lengthOfSlotRecord); //number of slot not change
	int freespacepointer = 0; //new freespace pointer
	for (int i=0; i< totalNumberOfSlots; i++)
	{
		SlotCell sc;
		sc.offset = * (int *)((char *)pageBuffer + PAGE_SIZE - 2*sizeof(int) - (i+1) * sizeof(SlotCell));
		sc.length = * (int *)((char *)pageBuffer + PAGE_SIZE - 2*sizeof(int) - (i+1) * sizeof(SlotCell) + sizeof(int));
		SlotCell sc2; //new slot cell
		
		//if this is "alive" record
		if(sc.offset != -1)
		{
			//copy this record to newbuffer 2
			memcpy((char *)pageBuffer2+freespacepointer, (char *)pageBuffer+sc.offset, sc.length);
			//update offset of this record in the slot cell in the slot directory
			memcpy((char *)pageBuffer2 + PAGE_SIZE - 2*sizeof(int) - (i+1) * sizeof(SlotCell), &freespacepointer, sizeof(int));
			freespacepointer += sc.length;
		}		
	}
	
	//update free space pointer
	memcpy((char *)pageBuffer2 + PAGE_SIZE - sizeof(int), &freespacepointer, sizeof(int));
	if(fileHandle.writePage(pageNumber, pageBuffer2) != 0){
		free(pageBuffer2);
		return -1;
	} 

	free(pageBuffer2);
	free(pageBuffer);

	//put buffer's record into buffer2 one by one???
	return 0;
}

/* Given a record descriptor, scan a file, i.e., sequentially read all the entries in the file.
 * A scan has a filter condition associated with it,
 * e.g., it consists of a list of attributes to project out as well as a predicate on an attribute ("Sal > 40000").
 * Specifically, the parameter conditionAttribute here is the attribute's name that you are going to apply the filter on.
 * The compOp parameter is the comparison type that is going to be used in the filtering process.
 * The value parameter is the value of the conditionAttribute that is going to be used to filter out records.
 * Note that the retrieved records should only have the fields that are listed in the vector attributeNames.
 * Please take a look at the test cases for more information on how to use this method.*/
// scan returns an iterator to allow the caller to go through the results one by one.
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
	      const vector<Attribute> &recordDescriptor,
	      const string &conditionAttribute,
	      const CompOp compOp,                  // comparision type such as "<" and "="
	      const void *value,                    // used in the comparison
	      const vector<string> &attributeNames, // a list of projected attributes
	      RBFM_ScanIterator &rbfm_ScanIterator)
{
    /*
	//create vector<unsigned> ridList in the rbfm_ScanIterator object
	//for each page i
	//read into RAM buffer
	//for each record in this page *data
	//check if it satisfy the condition
	//if satisfy, put its rid in the ridList
    */

	rbfm_ScanIterator.fileHandle = fileHandle;
    	rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    	rbfm_ScanIterator.conditionAttribute = conditionAttribute;
	rbfm_ScanIterator.compOp = compOp;
    	rbfm_ScanIterator.attributeNames = attributeNames;
	rbfm_ScanIterator.value = value;
    	rbfm_ScanIterator.pagesCount = fileHandle.getNumberOfPages();
    	rbfm_ScanIterator.currentPage = 0;
    	rbfm_ScanIterator.currentSlotNum = 1;
    	//calculSlotsCount = true;  //act at every page loaded
	return 0;
}


RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
//    bool RecordSatisfied = false;
    void * pageBuffer = malloc(PAGE_SIZE);
    void * attributeValue;  //TODO:
    void * currentrecord = malloc(PAGE_SIZE);
    int slotsCount = 0;
    RID currentRid;
    int attr_type;
    //bool 0 = false, 1= true;
    int index = 0;
    int i = 0;
    int j = 0;
    RC rc;
   
    //get what type of the condition attr
    for(int s=0; s<recordDescriptor.size();s++)
    {
	if(recordDescriptor[s].name == conditionAttribute)
	{
		attr_type = recordDescriptor[s].type;	
	}
    }
//    cout << "attr_type" << attr_type << endl;
    if(currentPage > pagesCount - 1)
    {                           //if currentPage is bigger than the number of pages indicate that we have arrived at the end
	return -1;
    }
  // cout << "currentPage:" << currentPage << endl;
    
    //start from current page
    //cout << "pagecount:" << pagesCount << endl;
    //cout << "slotcount:" << slotsCount << endl;
    for (j = currentPage; j < pagesCount; j++)
    {	
    	if(fileHandle.readPage(j, pageBuffer)!=0)
	{
		free(pageBuffer);
		return -1;
    	}
  	
	//get number of records
    	slotsCount = * (int *)((char *)pageBuffer + PAGE_SIZE - 2* sizeof(int));
//	if(j<16){
//	cout << "j:" << j << endl;
//	cout << "slotcount:" << slotsCount << endl;
//	cout << "currentSlotNum:" << currentSlotNum << endl;
//	}

	SlotCell sc;
//	cout << "OK5" << endl;    
//    	cout << "currrentpage slot:"<< currentPage<< "\t" <<  currentSlotNum << endl;
	int startSlot = 1;
	if(j==currentPage){
		startSlot=currentSlotNum;
	}
	
	for (i = startSlot; i < slotsCount+1; i++)
	{
		RID ridx;
		ridx.pageNum=j;
		ridx.slotNum=i;
		rc=rbfm->readRecord(fileHandle,recordDescriptor,ridx,currentrecord);
		
//		cout << "j,i,record sucess" << j << "\t" << i << "\t" << rc << endl;
		if(rc==0)
		{
		//read this ridx, put the attr value in the conditionAttrbuteValue
		void * conditionAttributeValue=malloc(PAGE_SIZE);
		ReadattributeinRecord(recordDescriptor, conditionAttribute, conditionAttributeValue, currentrecord);
//                cout << "i:" << i << endl;
//                cout << "j:" << j << endl;
		//compare
                if(filterRecord(conditionAttributeValue, compOp, value, attr_type))
		{
			rid.pageNum=j;
			rid.slotNum=i;
//			cout << "i:" << i << endl;
//			cout << "j:" << j << endl;

			int outOffset = 0;
			//the attr needed return
			for(int k=0; k<attributeNames.size(); k++)
			{
				//this attr length, and cuurentrecord
				outOffset+=ReadattributeinRecord(recordDescriptor, attributeNames[k], (char *)data+outOffset, currentrecord);
			}
			
			//got it, index is  flag
			index = 1;
			break;
		}
		free(conditionAttributeValue);
		}
	}
	if(index==1)
	{
		break;
	}
   }
   if (index ==1)
   {	
	currentPage=j;
	currentSlotNum=i+1;
	if (currentSlotNum==(slotsCount+1) && currentPage < pagesCount-1){
		currentSlotNum=1;
		currentPage=j+1;
	}
	index=0;	
	//cout <<currentPage <<"\t" << currentSlotNum<<"\t"<<slotsCount << "\t" << pagesCount <<endl;
	free(pageBuffer);
	free(currentrecord);
	return 0;
   }
   //scan all the slots, can not find it, return -1=the bottom 
   else
   {
//	cout << "arrived at the end of file!"<< endl;
	currentPage=pagesCount;
	currentSlotNum=slotsCount+1;
	free(pageBuffer);
        free(currentrecord);
   	return -1;
   }
}

//compasion, return true or false, both int in c++
RC RBFM_ScanIterator::filterRecord(void * conditionAttributeValue, const CompOp compOp, const void *value, int attr_type)
{
	//all true, no compare	
	if(compOp==NO_OP)
        return true;
	//if int
	if(attr_type == 0)
	{
		int xx = *(int*)conditionAttributeValue;
		int yy = *(int*)value;

		switch(compOp)
		{
			case 0:
				return xx == yy;
				break;
			case 1:
				return xx < yy;
				break;
			case 2:
				return xx > yy;
				break;
			case 3:
				return xx <= yy;
				break;
			case 4:
				return xx >= yy;
				break;
			case 5:
				return xx != yy;
				break;
			case 6:
				return true;
			default: 
				return false;
		}	
	}

	//real
	if(attr_type == 1)
	{
		float xx = *(float*)conditionAttributeValue;
		float yy = *(float*)value;
		switch(compOp){
                        case 0:
                                return xx == yy;
                                break;
                        case 1:
                                return xx < yy;
                                break;
                        case 2:
                                return xx > yy;
                                break;
                        case 3:
                                return xx <= yy;
                                break;
                        case 4:
                                return xx >= yy;
                                break;
                        case 5:
                                return xx != yy;
                                break;
                        case 6:
                                return true;
                        default:
                                return false;	
		}
	}
	//string/varchar
	if(attr_type == 2){
		string xx = "";
		string yy = "";
		//only read string
		for (int i = 0; i < *(int*)conditionAttributeValue; i++)
		{
			xx += *((char *)conditionAttributeValue+sizeof(int)+i);
		}
		for (int i = 0; i < *(int*)value; i++)
		{
			yy += *((char *)value+sizeof(int)+i);
		}
		//directly compare two strings, c++
//		cout <<"xx:" << xx <<endl;
//		cout << "yy:" << yy << endl;
                switch(compOp){
                        case 0:
                                return xx == yy;
                                break;
                        case 1:
                                return xx < yy;
                                break;
                        case 2:
                                return xx > yy;
                                break;
                        case 3:
                                return xx <= yy;
                                break;
                        case 4:
                                return xx >= yy;
                                break;
                        case 5:
                                return xx != yy;
                                break;
                        case 6:
                                return true;
                        default:
                                return false;
                }
	}
    return 0;
}


/* Given a record descriptor, reorganize the file which causes reorganization
 * of the records such that the records are collected towards the beginning of the file.
 * Also, record redirection is eliminated. (In this case, and only this case, it is okay for rids to change.)*/
// Extra credit for part 2 of the project, please ignore for part 1 of the project
// reorganizePage + handle tombstone
RC RecordBasedFileManager::reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor)
{
    void * oldBufferPage = malloc(PAGE_SIZE);
    void * newBufferPage = malloc(PAGE_SIZE);
    
    //keep track of new reorganized file page number
    int validPageNum = 0;
    
    //getNumberOfPages
    int numberOfPages = fileHandle.getNumberOfPages();
    
    const int offset_SlotsCount = PAGE_SIZE - sizeof(int)*2;
    const int offset_FreeSpacePoint = PAGE_SIZE - sizeof(int);
    
    const int zero = 0;
    //set the slot number to zero
    memcpy((char*)newBufferPage + offset_SlotsCount, &zero , sizeof(int));
    //set the free space pointer to the beginning of page
    memcpy((char*)newBufferPage + offset_FreeSpacePoint, &zero , sizeof(int));
    
    //new free space pointer
    int newFSP = 0;
    
    //new slots number
    unsigned newSN = 0;
    
    int NOO =0;
    //reorganize page by page
    for(int i = 0; i < numberOfPages; i++)
    {
        //read page
        int rc = fileHandle.readPage(i, oldBufferPage);

        int numberOfSlots = * (int *)((char *)oldBufferPage + offset_SlotsCount);
        
        //read slot by slot
        for(int j = 1; j <= numberOfSlots; j++)
        {
            SlotCell oldSC;
            oldSC.offset = * (int *)((char *)oldBufferPage + offset_SlotsCount - j * sizeof(SlotCell));
            oldSC.length = * (int *)((char *)oldBufferPage + offset_SlotsCount - j * sizeof(SlotCell) + sizeof(int));
            
            //check if offset = -1 (deleted)
            if(oldSC.offset != -1)
            {
                //check if the tombstone
                int numberofattr = *(int *)((char*)oldBufferPage +oldSC.offset);
                
                //if this is "alive" record (not tombstone)
                if(numberofattr != -2)
                {
                    //read the whole record = actual record + recordHeader
                    int recordLength = oldSC.length;

                        newFSP= * (int * )((char * )newBufferPage + offset_FreeSpacePoint);
                        
                        newSN = * (int *)((char *)newBufferPage + offset_SlotsCount);
                        
                        int freeSpace = PAGE_SIZE - newFSP - 2 * sizeof(int) - newSN * sizeof(SlotCell);
                        
                        //check if free space enough
                        if(freeSpace >= (recordLength + sizeof(SlotCell)))
                        {
                            //copy into new buffer page
                            memcpy((char *)newBufferPage + newFSP, (char *)oldBufferPage + oldSC.offset, recordLength);
                            
                            //update SN(slots number)
                            newSN +=1;
                            memcpy((char *)newBufferPage + offset_SlotsCount, &newSN, sizeof(int));
                            
                            //put new slot cell into slot directory
                            //new slot cell
                            SlotCell newSC;
                            newSC.length = recordLength;
                            newSC.offset = newFSP;
                            memcpy((char *)newBufferPage + offset_SlotsCount - newSN * sizeof(SlotCell), &newSC, sizeof(SlotCell));
                            
                            //update fsp
                            newFSP += recordLength;
                            memcpy((char *)newBufferPage + offset_FreeSpacePoint, &newFSP, sizeof(int));
                            NOO++;

                        }//end of if
                        else
                        {
                            fileHandle.writePage(validPageNum, newBufferPage);
                            
                            validPageNum++;
                            
                            //set the slot number to zero
                            memcpy((char*)newBufferPage + offset_SlotsCount, &zero , sizeof(int));
                            //set the free space pointer to the beginning of page
                            memcpy((char*)newBufferPage + offset_FreeSpacePoint, &zero , sizeof(int));
                            
                            //new free space pointer
                            newFSP = 0;
                            //new slots number
                            newSN = 0;
                            
                            // copy into new buffer page
                            memcpy((char *)newBufferPage + newFSP, (char *)oldBufferPage + oldSC.offset, recordLength);
                            
                            //update SN(slots number)
                            newSN +=1;
                            memcpy((char *)newBufferPage + offset_SlotsCount, &newSN, sizeof(int));
                            
                            //put new slot cell into slot directory
                            //new slot cell
                            SlotCell newSC;
                            newSC.length = recordLength;
                            newSC.offset = newFSP;
                            memcpy((char *)newBufferPage + offset_SlotsCount - newSN * sizeof(SlotCell), &newSC, sizeof(SlotCell));
                            
                            //update fsp
                            newFSP += recordLength;
                            memcpy((char *)newBufferPage + offset_FreeSpacePoint, &newFSP, sizeof(int));
                            NOO++;

                            
                        }//end of else
                    
                }
            }
        }
    }
    
    cout<<"++++++++++++++++++++++++++++"<<NOO<<"+++++++++++++++";
    
    //write the last page
    fileHandle.writePage(validPageNum, newBufferPage);
    
    void * emptyBufferPage = malloc(PAGE_SIZE);
    
    //set the slot number to zero
    memcpy((char*)emptyBufferPage + offset_SlotsCount, &zero , sizeof(int));
    
    //set the free space pointer to the beginning of page
    memcpy((char*)emptyBufferPage + offset_FreeSpacePoint, &zero , sizeof(int));
    
    for(int i = validPageNum + 1; i < numberOfPages; i++)
    {
        fileHandle.writePage(i, emptyBufferPage);
    }

    //free
    free(emptyBufferPage);
    free(oldBufferPage);
    free(newBufferPage);
    
    return 0;
}
