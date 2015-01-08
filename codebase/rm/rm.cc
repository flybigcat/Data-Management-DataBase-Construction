#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

//initialize catalog/Tables and Columns recordDescriptors
RelationManager::RelationManager()
{
        catalog="Tables";
        column_name="Columns";
	
	//Tables, set attr
        Attribute attr;
        attr.name = "TableId";
        attr.type = TypeInt;
        attr.length = (AttrLength)4;
        catalog_v.push_back(attr);
        catalog_s.push_back(attr.name);

        attr.name = "TableName";
        attr.type = TypeVarChar;
        attr.length = (AttrLength)30;
        catalog_v.push_back(attr);
        catalog_s.push_back(attr.name);

        attr.name = "FileName";
        attr.type = TypeVarChar;
        attr.length = (AttrLength)30;
        catalog_v.push_back(attr);
        catalog_s.push_back(attr.name);

        attr.name = "TableId";
        attr.type = TypeInt;
        attr.length = (AttrLength)4;
        column_name_v.push_back(attr);
        column_name_s.push_back(attr.name);

        attr.name = "ColumnName";
        attr.type = TypeVarChar;
        attr.length = (AttrLength)30;
        column_name_v.push_back(attr);
        column_name_s.push_back(attr.name);

        attr.name = "ColumnType";
        attr.type = TypeInt;
        attr.length = (AttrLength)4;
        column_name_v.push_back(attr);
        column_name_s.push_back(attr.name);

        attr.name = "ColumnLength";
        attr.type = TypeInt;
        attr.length = (AttrLength)4;
        column_name_v.push_back(attr);
        column_name_s.push_back(attr.name);

        attr.name = "Position";
        attr.type = TypeInt;
        attr.length = (AttrLength)4;
        column_name_v.push_back(attr);
        column_name_s.push_back(attr.name);

	initial();

}

RelationManager::~RelationManager()
{
}

//initilize catalog systems files (catalog/Tables, Columns)
void RelationManager::initial()
{
	// use rbfm
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
        RC rc;
        FileHandle fileHandle,fileHandle1,fileHandle2,fileHandle3;
        RID rid;

        unsigned int temp;
        unsigned int offset = 0;
        void *cat_data = (void *)malloc(PAGE_SIZE);
	void *cat_data1 = (void *)malloc(PAGE_SIZE);
        void *col_data = (void *)malloc(PAGE_SIZE);
	
//	cout << "initialilzation start" << endl;	
        //create Tables file
	rc = rbfm->createFile(catalog);
	//not exits, this is first create Tables
	if(rc==0)
	{
		tableId=0;
		//tableId
		memcpy((char*)cat_data,&tableId,sizeof(int));
                offset +=sizeof(int);
		
		//Tables length
                unsigned int tableNameLength = (unsigned int)catalog.length();
                memcpy((char*)cat_data + offset,&tableNameLength,sizeof(int));
                offset+=sizeof(int);

		//Talbes name
                memcpy((char*)cat_data+offset,catalog.c_str(),sizeof(char) * tableNameLength);
                offset += tableNameLength;

		//fileName length
                unsigned int fileNameLength = (unsigned int)catalog.length();
                memcpy((char*)cat_data + offset,&fileNameLength,sizeof(int));
                offset +=sizeof(int);

		//fileName itself
                memcpy((char*)cat_data+offset,catalog.c_str(),sizeof(char) * fileNameLength);
                offset += tableNameLength;
	
		int tableId1=tableId+1;
		offset=0;
                memcpy((char*)cat_data1,&tableId1,sizeof(int));
                offset +=sizeof(int);

                tableNameLength = (unsigned int)column_name.length();
                memcpy((char*)cat_data1 + offset,&tableNameLength,sizeof(int));
                offset+=sizeof(int);

                memcpy((char*)cat_data1+offset,column_name.c_str(),sizeof(char) * tableNameLength);
                offset += tableNameLength;

                fileNameLength = (unsigned int)column_name.length();
                memcpy((char*)cat_data1 + offset,&fileNameLength,sizeof(int));
                offset +=sizeof(int);

                memcpy((char*)cat_data1+offset,column_name.c_str(),sizeof(char) * fileNameLength);
                offset += tableNameLength;
	
		//use fileHandle to take care of (open) catalog/Tables
		rbfm->openFile(catalog,fileHandle);
		rbfm->insertRecord(fileHandle,catalog_v,cat_data,rid);
		rbfm->insertRecord(fileHandle,catalog_v,cat_data1,rid);
		rbfm->closeFile(fileHandle);
	
//		RecordBasedFileManager *rbfm3 = RecordBasedFileManager::instance();
//		rbfm3->openFile(catalog,fileHandle3);
//		rbfm3->insertRecord(fileHandle3,catalog_v,cat_data1,rid);
//		rbfm3->closeFile(fileHandle3);

		//manapulate one file, use one rbfm
		RecordBasedFileManager *rbfm1 = RecordBasedFileManager::instance();
		rbfm1->createFile(column_name);
		rbfm1->openFile(column_name,fileHandle1);
		unsigned int numOfCol = (unsigned int)catalog_v.size();
		
		//one column, insert one record
                for (int i=0;i<(int)numOfCol;i++)
                {
            		offset = 0;
           	 	memcpy((char*)col_data,&tableId,sizeof(int));
            		offset +=sizeof(int);
                        temp = (unsigned int)catalog_v[i].name.length();
                        memcpy((char *)col_data+offset,&temp,sizeof(int));
                        offset +=sizeof(int);
                        memcpy((char *)col_data+offset,catalog_v[i].name.c_str(),temp);
                        offset +=temp;
                        memcpy((char *)col_data+offset,&catalog_v[i].type,sizeof(int));
                        offset +=sizeof(int);
                        memcpy((char *)col_data+offset,&catalog_v[i].length,sizeof(int));
                        offset +=sizeof(int);
                        memcpy((char *)col_data+offset,&i,sizeof(int));
			rbfm1->insertRecord(fileHandle1,column_name_v,col_data,rid);
		}
		
		numOfCol = (unsigned int)column_name_v.size();
                for (int i=0;i<(int)numOfCol;i++)
                {
                        offset = 0;
                        memcpy((char*)col_data,&tableId1,sizeof(int));
                        offset +=sizeof(int);
                        temp = (unsigned int)column_name_v[i].name.length();
                        memcpy((char *)col_data+offset,&temp,sizeof(int));
                        offset +=sizeof(int);
                        memcpy((char *)col_data+offset,column_name_v[i].name.c_str(),temp);
                        offset +=temp;
                        memcpy((char *)col_data+offset,&column_name_v[i].type,sizeof(int));
                        offset +=sizeof(int);
                        memcpy((char *)col_data+offset,&column_name_v[i].length,sizeof(int));
                        offset +=sizeof(int);
                        memcpy((char *)col_data+offset,&i,sizeof(int));
                        rbfm1->insertRecord(fileHandle1,column_name_v,col_data,rid);
                }


		tableId=tableId1;
		rbfm1->closeFile(fileHandle1);
	}
	//if catalog/Tables and Columns are here!
	//read TableId
	else
	{
//		tableId=-1;
		RecordBasedFileManager *rbfm2 = RecordBasedFileManager::instance();
//		RBFM_ScanIterator rbfm2_ScanIterator;
                rbfm2->openFile(catalog,fileHandle2);
                int l=catalog.length();//only l this variable
		int pageCount = fileHandle2.getNumberOfPages();	
//		cout << "pageCount:" << pageCount << endl;
		if(pageCount > 0){
			void * buffer = malloc(PAGE_SIZE);
			for (int i = 0; i < pageCount; i++){
				fileHandle2.readPage(i, buffer);
				int totalNumberOfSlots = * (int *)((char *)buffer + PAGE_SIZE-2*sizeof(int));	
//				cout << "slotNum:" << totalNumberOfSlots << endl;			
				tableId+=totalNumberOfSlots;	
			}
			free(buffer);
		}
		tableId=tableId-1;	
		//cout << tableId <<endl;

//               	memcpy((char*)cat_data,&l,sizeof(int));//no use anymore
//               	memcpy((char*)cat_data+sizeof(int),catalog.c_str(),l);//no use anymore
		//scan all
//                rbfm2->scan(fileHandle2,catalog_v,"", NO_OP, NULL,catalog_s,rbfm2_ScanIterator);
               //all the way down to end 
//		while(rbfm2_ScanIterator.getNextRecord(rid,col_data)!=RM_EOF)
//                {
			//read all the infor in Tables: TableId, TableName, fileName
                //        memcpy(&l,(char*)col_data,sizeof(int));
//                	l=rid.slotNum;
//                        if(l-1>(int)tableId)
//    	                            tableId=l-1;
//                }

                rbfm->closeFile(fileHandle2);	
	}

//	cout << "initialilzation end" << endl;
	free(cat_data);
	free(cat_data1);
	free(col_data);	
}

/*This method creates a table called tableName with a vector of attributes (attrs).*/
RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    FileHandle fileHandle,fileHandle1;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    RID rid;

    //cache->insert(pair<string,vector<Attribute> >(tableName,attrs));

    unsigned int temp;
    unsigned int offset = 0;
   
    //one record
    char *cat_data = (char *)malloc(PAGE_SIZE);
    char *col_data = (char *)malloc(PAGE_SIZE);
    rc = rbfm->createFile(tableName);

    if (rc != 0)
    {
    	cout << "file is already exist" << endl;
    	free(cat_data);
    	free(col_data);
    	return -1;
    }

    //write this record, one record in Tables
    tableId ++;
    cout << "tableid:" << tableId << endl;
    memcpy((char*)cat_data,&tableId,sizeof(int));
    offset +=sizeof(int);
    unsigned int tableNameLength = (unsigned int)tableName.length();
    memcpy((char*)cat_data + offset,&tableNameLength,sizeof(int));
    offset+=sizeof(int);
    memcpy((char*)cat_data+offset,tableName.c_str(),sizeof(char) * tableNameLength);
    offset += tableNameLength;
    unsigned int fileNameLength = (unsigned int)tableName.length();
    memcpy((char*)cat_data + offset,&fileNameLength,sizeof(int));
    offset +=sizeof(int);
    memcpy((char*)cat_data+offset,tableName.c_str(),sizeof(char) * fileNameLength);
    offset += tableNameLength;
    
    RecordBasedFileManager *rbfm1 = RecordBasedFileManager::instance();
    rbfm1->openFile(catalog,fileHandle);
    rbfm1->insertRecord(fileHandle,catalog_v,cat_data,rid);
    rbfm1->closeFile(fileHandle);
  
    //insert column into Columns	
    RecordBasedFileManager *rbfm2 = RecordBasedFileManager::instance();
    rbfm2->openFile(column_name,fileHandle1);
    unsigned int numOfCol = (unsigned int)attrs.size();

    for (int i=0;i<(int)numOfCol;i++)
    {
        offset = 0;
        memcpy((char*)col_data,&tableId,sizeof(int));
        offset +=sizeof(int);
        temp = (unsigned int)attrs[i].name.length();
        memcpy((char *)col_data+offset,&temp,sizeof(int));
        offset +=sizeof(int);
        memcpy((char *)col_data+offset,attrs[i].name.c_str(),temp);
        offset +=temp;

        memcpy((char *)col_data+offset,&attrs[i].type,sizeof(int));

        offset +=sizeof(int);
        memcpy((char *)col_data+offset,&attrs[i].length,sizeof(int));

        offset +=sizeof(int);
        memcpy((char *)col_data+offset,&i,sizeof(int));
	rbfm2->insertRecord(fileHandle1,column_name_v,col_data,rid);
    }
    rbfm2->closeFile(fileHandle1);
    free(cat_data);
    free(col_data);
    return 0;
}

/*This method deletes a table called tableName.*/
RC RelationManager::deleteTable(const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
  
    rc=rbfm->destroyFile(tableName);
    if(rc != 0)
    {
    	cout << "no such file: " << tableName << endl;
	return -10;  	
    }	
    else{
	RecordBasedFileManager *rbfm1 = RecordBasedFileManager::instance();		
	RBFM_ScanIterator rbfm1_ScanIterator;
	FileHandle fileHandle1;
	RID rid;
        void *ret=malloc(PAGE_SIZE);//catalog/Tables
	void *set=malloc(PAGE_SIZE);//columns
	void *re=malloc(PAGE_SIZE);// not really in use, but used in scan columns part
        int len=(int)tableName.length();
        memcpy((char*)ret,&len,sizeof(int));
	memcpy((char*)ret+sizeof(int),tableName.c_str(),len*sizeof(char)); 
        
	rbfm1->openFile(catalog,fileHandle1);
        rbfm1->scan(fileHandle1,catalog_v,catalog_v[1].name, EQ_OP, ret,catalog_s,rbfm1_ScanIterator);
	//get this table infor, find it and delete from Tables
	if(rbfm1_ScanIterator.getNextRecord(rid,set)!=RM_EOF)
	{
		//ret is the whole record about this table in the Tables
		rbfm1->deleteRecord(fileHandle1,catalog_v,rid);
		rbfm1_ScanIterator.close();		
	}
	else{
		free(ret);
		free(set);
		free(re);
		return -1;
	}
	
	//delete all the infor in the Columns
	RecordBasedFileManager *rbfm2 = RecordBasedFileManager::instance();
        RBFM_ScanIterator rbfm2_ScanIterator;
        FileHandle fileHandle2;
	rbfm2->openFile(column_name,fileHandle2);
	rbfm2->scan(fileHandle2,column_name_v,column_name_v[0].name, EQ_OP, set,column_name_s,rbfm2_ScanIterator);
	//delete not only one record, but all the records infor in the Columns
	while(rbfm2_ScanIterator.getNextRecord(rid,re)!=RM_EOF)
	{
		rbfm2->deleteRecord(fileHandle2,column_name_v,rid);
	}

	rbfm2_ScanIterator.close();

	free(set);
	free(re);
	free(ret);
    }
    
    return 0;
}

/* This method gets the attributes (attrs) of a table called tableName.*/
//get tableId
//get all the infor in the Columns
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
        RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        RBFM_ScanIterator rbfm_ScanIterator;

        void * returndata=(void*)malloc(PAGE_SIZE);//tableName elements, tableName
        void * ret=(void*)malloc(PAGE_SIZE);//table infor in the Tables, but only use tableId
        void * re=(void*)malloc(PAGE_SIZE);

        int l= (int)tableName.length();
        memcpy((char*)returndata,&l,sizeof(int));
        memcpy((char*)returndata+sizeof(int),tableName.c_str(),sizeof(char) * l);
        RID rid;
        rbfm->openFile(catalog,fileHandle);
	//cout << "OK2" << endl;
        rbfm->scan(fileHandle,catalog_v,catalog_v[1].name, EQ_OP, returndata,catalog_s,rbfm_ScanIterator);
	//cout << "OK3" << endl;
        if(rbfm_ScanIterator.getNextRecord(rid,ret)!=RM_EOF)
        {
                rbfm_ScanIterator.close();
        }
        else
        {
                
//		cout << "OK4" << endl;
		free(returndata);
                free(ret);
                free(re);
                return -1;
        }
//	cout << "OK" << endl;
        FileHandle fileHandle1;
        RecordBasedFileManager *rbfm1 = RecordBasedFileManager::instance();
        rbfm1->openFile(column_name,fileHandle1);
        map<int,Attribute> *att_pos=new map<int,Attribute>();//position is variable here, so use map to keep track of position
        Attribute attr;
        RBFM_ScanIterator rbfm1_ScanIterator;
        rbfm1->scan(fileHandle1,column_name_v,column_name_v[0].name, EQ_OP, ret,column_name_s,rbfm1_ScanIterator);
//	cout << "OK1" << endl;
        while(rbfm1_ScanIterator.getNextRecord(rid,re)!=RM_EOF)
        {
			//tableId, no use
                        memcpy(&l,(char*)re,sizeof(int));
                        unsigned int offset = 0;
			//column  name length
                        unsigned int namelenght;
          		//buffer
	                int intval;
                        offset+=sizeof(int);
                        memcpy(&namelenght,(char *)re+offset,sizeof(int));

                        offset +=sizeof(int);
                        char *name = (char *)malloc(namelenght+1);
			memset(name,'\0',namelenght+1);
                        memcpy(name,(char *)re+offset,namelenght);
                        offset+= namelenght;

                        attr.name=name;
			
			//int val is the column/attr type
                        memcpy(&intval,(char *)re+offset,sizeof(int));
                        offset+=sizeof(int);
                        attr.type=(AttrType)intval;

			//max length of this attr
                        memcpy(&intval,(char *)re+offset,sizeof(int));
                        offset+=sizeof(int);
                        attr.length=intval;

                        memcpy(&intval,(char *)re+offset,sizeof(int));
                        offset+=sizeof(int);

                        att_pos->insert(pair<int,Attribute>(intval,attr));
        }
        rbfm1_ScanIterator.close();

        attrs.clear();//vector method, like free it
//	cout << "OK1" << endl;
	//use the order in the map
        for(int i=0;i<(int)att_pos->size();i++)
        {
                attrs.push_back(att_pos->at(i));
        }

        free(returndata);
        free(ret);
        free(re);
        return 0;
}

/*This method inserts a tuple into a table called tableName. 
You can assume that the input is always correct and free of error. 
That is, you do not need to check if the input tuple has the right number of attributes and if the attribute types match.*/
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> attrs;
	FileHandle fileHandle;
	RC rc;
	
	rc=getAttributes(tableName, attrs);

	rbfm->openFile(tableName,fileHandle);

	rbfm->insertRecord(fileHandle,attrs,data,rid);
	rbfm->closeFile(fileHandle);
   
	return 0;
}

/*This method deletes all tuples in a table called tableName. This command should result in an empty table.*/
RC RelationManager::deleteTuples(const string &tableName)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;

        rbfm->openFile(tableName,fileHandle);
        rbfm->deleteRecords(fileHandle);
        rbfm->closeFile(fileHandle);

    	return 0;
}

/*This method deletes a tuple with a given rid.*/
RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
        vector<Attribute> attrs;
        FileHandle fileHandle;

        getAttributes(tableName, attrs);
        rbfm->openFile(tableName,fileHandle);
        rbfm->deleteRecord(fileHandle,attrs,rid);
        rbfm->closeFile(fileHandle);

        return 0;
}

/*This method updates a tuple identified by a given rid. Note: 
if the tuple grows (i.e., the size of tuple increases) and there is no space in the page to store the tuple (after the update),
 then, the tuple is migrated to a new page with enough free space.
 Since you will implement an index structure (e.g., B-tree) in project 3,
 you can assume that tuples are identified by their rids and when they migrate,
 they leave a tombstone behind pointing to the new location of the tuple.*/
RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
        RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
        vector<Attribute> attrs;
        FileHandle fileHandle;

        getAttributes(tableName, attrs);
        rbfm->openFile(tableName,fileHandle);
        rbfm->updateRecord(fileHandle,attrs,data,rid);
        rbfm->closeFile(fileHandle);    
	return 0;
}

/*This method reads a tuple identified by a given rid.*/
RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    vector<Attribute> attrs;
    FileHandle fileHandle;
    RC rc;

    if(rbfm->openFile(tableName,fileHandle)!=0){
	return -1;	
    }
    getAttributes(tableName, attrs);		
    rc=rbfm->readRecord(fileHandle,attrs,rid,data);
    rbfm->closeFile(fileHandle);
    if(rc!=0){
    	return -1;
    }	
    return 0;
}

/*This method reads a specific attribute of a tuple identified by a given rid.*/
RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    vector<Attribute> attrs;
    FileHandle fileHandle;
    RC rc;

    getAttributes(tableName, attrs);
    rbfm->openFile(tableName,fileHandle);
    rc=rbfm->readAttribute(fileHandle,attrs,rid,attributeName,data);
    rbfm->closeFile(fileHandle);
	
    return 0;
}

/*This method reorganizes the tuples in a page. 
That is, it pushes the free space towards the end of the page. 
Note: In this method you are NOT allowed to change the rids, since they might be used by other external index structures, 
and it's too expensive to modify those structures for each such a function call. 
It's OK to keep those deleted tuples and their slots.*/
RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    vector<Attribute> attrs;
    FileHandle fileHandle;
	
    if(rbfm->openFile(tableName,fileHandle)!=0){
	return -1;	
    }
    getAttributes(tableName, attrs);
    rbfm->reorganizePage(fileHandle, attrs, pageNumber);
    rbfm->closeFile(fileHandle);
    return 0;
}

/* The same requirment with rbfm.scan
This method scans a table called tableName. 
ehat is, it sequentially reads all the entries in the table. 
This method returns an iterator called rm_ScanIterator to allow the caller to go through the records in the table one by one. 
A scan has a filter condition associated with it, 
e.g., it consists of a list of attributes to project out as well as a predicate on an attribute (“Sal > 40000”). 
Note: the RBFM_ScanIterator should not cache the entire scan result in memory. 
In fact, you need to be looking at one (or a few) page(s) of data at a time, ever. In this project, let the OS do the memory-management work for you.
*/
RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
                RC rc;
                vector<Attribute> attrs;
                rc = getAttributes(tableName,attrs);
                if (rc != 0)
                {
                        cout << rc<<"  Error in getAttributes()" << endl;
                        return -1;
                }
                FileHandle fileHandle;
                RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
                rc = rbfm->openFile(tableName, fileHandle);
                rc = rbfm->scan(fileHandle,attrs,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.rbfm_ScanIterator);
                //rbfm->closeFile(fileHandle);
                return 0;
}

// indexScan returns an iterator to allow the caller to go through qualified entries in index
RC RelationManager::indexScan(const string &tableName,
      const string &attributeName,
      const void *lowKey,
      const void *highKey,
      bool lowKeyInclusive,
      bool highKeyInclusive,
      RM_IndexScanIterator &rm_IndexScanIterator)
{
    indexManager = IndexManager::instance();
    IXFileHandle ixfileHandle;
    string indexFileName = tableName + "_" + attributeName;
    
    // get attribute by attributeName
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    Attribute attribute;
    for(int i = 0 ; i < attrs.size(); i++)
    {
        if(attrs[i].name == attributeName){
            attribute = attrs[i];
            break;
        }
    }
    attrs.clear();

    RC rc;
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    rc += indexManager->scan(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_ScanIterator);

    return rc;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
    RC rc;
    int numberOfPages = 4;
    indexManager = IndexManager::instance();
    string indexFileName = tableName + "_" + attributeName;
    rc = indexManager->createFile(indexFileName, numberOfPages);
    return rc;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
    RC rc;
    indexManager = IndexManager::instance();
    string indexFileName = tableName + "_" + attributeName;
    rc = indexManager->destroyFile(indexFileName);
    return rc;
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return 0;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return 0;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    vector<Attribute> attrs;
    FileHandle fileHandle;
    
    if(rbfm->openFile(tableName,fileHandle)!=0){
        return -1;
    }
    
    getAttributes(tableName, attrs);
    
    RC rc = rbfm->reorganizeFile(fileHandle, attrs);
    rbfm->closeFile(fileHandle);
    return rc;
}
