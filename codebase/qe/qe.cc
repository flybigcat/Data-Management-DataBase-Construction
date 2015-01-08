
#include "qe.h"

//                             //
//   Grace Hash Join operator  //
//                             //
GHJoin::GHJoin(Iterator *leftIn,          // Iterator of input R
               Iterator *rightIn,         // Iterator of input S
        const Condition &condition,       // Join condition (CompOp is always EQ)
         const unsigned numPartitions )   // # of partitions for each relation (decided by the optimizer)
{
    it_leftIn = leftIn;
    it_rightIn = rightIn;
    this->condition = condition;

    //if r > s, right and left may change
    it_leftIn->getAttributes(attrs_leftIn);
    it_rightIn->getAttributes(attrs_rightIn);
    attrs_out.reserve( attrs_leftIn.size() + attrs_rightIn.size());
    attrs_out.insert(attrs_out.end(), attrs_leftIn.begin(), attrs_leftIn.end());
    attrs_out.insert(attrs_out.end(), attrs_rightIn.begin(), attrs_rightIn.end());
    partition(numPartitions);
    probe();

    it_result = new TableScan(*rm, resultName);
}

RC GHJoin::partition(const unsigned numPartitions)
{
    // cout<<"GHJoin::partition()"<<endl;

    //init all temp file
    int suffix_left = 0;
    int suffix_right = 0;
    partition_left_prefix = "left_join";
    partition_right_prefix = "right_join";
    rm = RelationManager::instance();
    tempRecord = malloc(4096);
    tempRecord2 = malloc(4096);

    RC rc = -1;
    //create left partition files
    string tmpStr;
    for( unsigned i = 0 ; i < numPartitions ; i++)
    {
        rc = -1;
        tmpStr = partition_left_prefix + to_string(suffix_left) + "_" + to_string(i);
        rc = rm->createTable(tmpStr, attrs_leftIn);
        while(rc != SUCCESS){
            suffix_left++;
            tmpStr = partition_left_prefix + to_string(suffix_left) + "_" + to_string(i);
            rc = rm->createTable(tmpStr, attrs_leftIn);
        }
        partitionName_left.push_back(tmpStr);
    }
    //create right partition files
    suffix_right = suffix_left;
    for( unsigned i = 0 ; i < numPartitions ; i++)
    {
        rc = -1;
        tmpStr = partition_right_prefix + to_string(suffix_right) + "_" + to_string(i);
        rc = rm->createTable(tmpStr, attrs_rightIn);
        while(rc != SUCCESS){
            suffix_left++;
            tmpStr = partition_right_prefix + to_string(suffix_right) + "_" + to_string(i);
            rc = rm->createTable(tmpStr, attrs_rightIn);
        }
        partitionName_right.push_back(tmpStr);
    }


    RID rid;
    vector<AttrValue> attrsList;
    keyIndex_left = 0;
    unsigned partitionIndex = 0;
    // get keyIndex for left
    for(unsigned i = 0 ; i < attrs_leftIn.size() ; i++){
        if( attrs_leftIn[i].name == condition.lhsAttr){
            keyIndex_left = i;
            break;
        }
    }
    // cout<<"GHJoin::partition():insert left tuples"<<endl;
    //insert left tuples
    while(it_leftIn->getNextTuple(tempRecord) != QE_EOF){
        record2AttrsList(attrs_leftIn, tempRecord, attrsList);
        partitionIndex = hashFunction(attrsList[keyIndex_left].attribute, attrsList[keyIndex_left].data, numPartitions);
        rm->insertTuple(partitionName_left[partitionIndex], tempRecord, rid);
        clearAttrsList(attrsList);
    }
    // cout<<"GHJoin::partition():insert left tuples:done"<<endl;

    keyIndex_right = 0;
    // get keyIndex for right
    for(unsigned i = 0 ; i < attrs_rightIn.size() ; i++){
        if( attrs_rightIn[i].name == condition.rhsAttr){
            keyIndex_right = i;
            break;
        }
    }
    // cout<<"GHJoin::partition():keyIndex_right:"<<keyIndex_right<<endl;
    // cout<<"GHJoin::partition():insert right tuples"<<endl;
    //insert right tuples
    while(it_rightIn->getNextTuple(tempRecord) != QE_EOF){
        record2AttrsList(attrs_rightIn, tempRecord, attrsList);
        // cout<<"GHJoin::partition():insert right tuples:getNextTuple:"<<endl;
        partitionIndex = hashFunction(attrsList[keyIndex_right].attribute, attrsList[keyIndex_right].data, numPartitions);
        // cout<<"GHJoin::partition():insert right tuples:partitionIndex:"<<partitionIndex<<endl;
        // cout<<"GHJoin::partition():insert right tuples:partitionName_right.size():"<<partitionName_right.size()<<endl;
        // cout<<"GHJoin::partition():insert right tuples:partitionName_right[partitionIndex]:"<<partitionName_right[partitionIndex]<<endl;
        rm->insertTuple(partitionName_right[partitionIndex], tempRecord, rid);
        clearAttrsList(attrsList);
    }
    // cout<<"GHJoin::partition():insert right tuples:done"<<endl;

    return 0;
}

RC GHJoin::probe()
{
    // cout<<"GHJoin::probe()"<<endl;
    int suffix = 0;
    resultName = "result_" + to_string(suffix);
    //create result
    RC rc = -1;
    rc = rm->createTable(resultName, attrs_out);
    while(rc != SUCCESS){
        suffix++;
        resultName = "result_" + to_string(suffix);
        rc = rm->createTable(resultName, attrs_out);
    }

    //retrieve every partitions
    std::map<int, void*> map_int;
    std::map<float, void*> map_float;
    std::map<string, void*> map_string;
    TableScan *it_left_temp;
    TableScan *it_right_temp;
    vector<AttrValue> attrsList;
    vector<AttrValue> attrsList_right_temp;
    unsigned size_l = 0;
    unsigned size_r = 0;
    std::vector<vector<AttrValue>> attrsLists;
    RID rid;
    for(unsigned i = 0 ; i < partitionName_left.size() ; i++){
        it_left_temp = new TableScan(*rm, partitionName_left[i]);
        while(it_left_temp->getNextTuple(tempRecord) != QE_EOF){
            // cout<<"GHJoin::probe():it_left_temp->getNextTuple(tempRecord)"<<endl;
            size_l = record2AttrsList(attrs_leftIn, tempRecord, attrsList);
            //
            switch(attrs_leftIn[keyIndex_left].type){
            case TypeInt:
                int key_int;
                key_int = *(int*)attrsList[keyIndex_left].data;
                map_int.insert(std::pair<int,void*>( key_int , tempRecord));
                break;
            case TypeReal:
                float key_float;
                key_float = *(float*)attrsList[keyIndex_left].data;
                map_float.insert(std::pair<float,void*>( key_float , tempRecord));
                break;
            case TypeVarChar:
                char* key_cstring = Varchart2CString(attrsList[keyIndex_left].data);
                string key_string = string(key_cstring);
                map_string.insert(std::pair<string,void*>( key_string , tempRecord));
                break;
            }

            // cout<<"GHJoin::probe():after hash"<<endl;
            it_right_temp = new TableScan(*rm, partitionName_right[i]);
            while(it_right_temp->getNextTuple(tempRecord2) != QE_EOF){
                size_r = record2AttrsList(attrs_rightIn, tempRecord2, attrsList_right_temp);
                int result;
                result = memcmp(attrsList[keyIndex_left].data , attrsList_right_temp[keyIndex_right].data, attrsList[keyIndex_left].exactBytes);
                // cout<<"GHJoin::probe():memcmp():"<<result<<endl;
                if(result == 0){
                    memcpy((char*)tempRecord + size_l , tempRecord2, size_r);
                    // cout<<"GHJoin::probe():rm->insertTuple(resultName, tempRecord, rid);"<<result<<endl;
                    rm->insertTuple(resultName, tempRecord, rid);
                    // cout<<"GHJoin::probe():rm->insertTuple(resultName, tempRecord, rid); done"<<result<<endl;
                }
                clearAttrsList(attrsList_right_temp);
            }            
            // delete(it_right_temp);

            clearAttrsList(attrsList);
        }
        // delete(it_left_temp);

        // // cout<<"GHJoin::probe():after hash"<<endl;
        // std::map<int, void*>::iterator iter_in;
        // std::map<float, void*>::iterator iter_float;
        // std::map<string, void*>::iterator iter_string;
        // it_right_temp = new TableScan(*rm, partitionName_right[i]);
        // while(it_right_temp->getNextTuple(tempRecord2) != QE_EOF){
        //     size_r = record2AttrsList(attrs_rightIn, tempRecord2, attrsList_right_temp);
        //     int result = -1;
        //     int tmp_size_l = 0;
        //     // result = memcmp(attrsList[keyIndex_left].data , attrsList_right_temp[keyIndex_right].data, attrsList[keyIndex_left].exactBytes);
        //     switch(attrsList_right_temp[keyIndex_right].attribute.type){
        //     case TypeInt:
        //         int key_int;
        //         key_int = *(int*)attrsList_right_temp[keyIndex_right].data;
        //         iter_in = map_int.find(key_int); 
        //         if (iter_in != map_int.end()){ 
        //         // if(map_int.find(key_int) != map_int.end()){
        //             tmp_size_l = getDataLength(attrs_leftIn,iter_in->second);
        //             memcpy((char*)tempRecord, iter_in->second, tmp_size_l);
        //             // memcpy((char*)tempRecord + size_l , map_int[key_int], size_r);
        //             // cout<<"GHJoin::probe():match(key_int): "<< key_int<<endl;
        //             result = 0;
        //         }
        //         break;
        //     case TypeReal:
        //         float key_float;
        //         key_float = *(float*)attrsList_right_temp[keyIndex_right].data;
        //         iter_float = map_float.find(key_float); 
        //         if (iter_float != map_float.end()){ 
        //         // if(map_float.find(key_float) != map_float.end()){
        //             tmp_size_l = getDataLength(attrs_leftIn,iter_float->second);
        //             memcpy((char*)tempRecord, iter_float->second, tmp_size_l);
        //             // memcpy((char*)tempRecord + size_l , map_float[key_float], size_r);
        //             result = 0;
        //         }
        //         break;
        //     case TypeVarChar:
        //         char* key_cstring = Varchart2CString(attrsList_right_temp[keyIndex_right].data);
        //         string key_string = string(key_cstring);
        //         iter_string = map_string.find(key_string); 
        //         if (iter_string != map_string.end()){ 
        //         // if(map_string.find(key_string) != map_string.end()){
        //             tmp_size_l = getDataLength(attrs_leftIn,iter_string->second);
        //             memcpy((char*)tempRecord, iter_string->second, tmp_size_l);
        //             // memcpy((char*)tempRecord + size_l , map_string[key_string], size_r);
        //             result = 0;
        //         }
        //         break;
        //     }
        //     // cout<<"GHJoin::probe():memcmp():"<<result<<endl;
        //     if(result == 0){
        //         memcpy((char*)tempRecord + tmp_size_l , tempRecord2, size_r);
        //         // cout<<"GHJoin::probe():rm->insertTuple(resultName, tempRecord, rid);"<<result<<endl;
        //         rm->insertTuple(resultName, tempRecord, rid);
        //         cout<<"GHJoin::probe():rm->insertTuple(resultName, tempRecord, rid); done"<<endl;
        //     }
        //     clearAttrsList(attrsList_right_temp);
        // }            
        // delete(it_right_temp);

        map_int.clear();
        map_float.clear();
        map_string.clear();
    }

    return 0;
}

RC GHJoin::getNextTuple(void *data)
{
    RC rc = 0;
    rc = it_result->getNextTuple(data);
    if(rc == QE_EOF)
        clearAll();
    return rc;
   
    // // Set up the iterator
    // RM_ScanIterator rmsi;
    // RID rid;
    // rm->scan(tableName, "", NO_OP, NULL, stringAttributes, rmsi);
    // while( (rc = rmsi.getNextTuple(rid, data)) != RM_EOF) {
    // }
    // rmsi.close();
}

// For attribute in vector<Attribute>, name it as rel.attr
void GHJoin::getAttributes(vector<Attribute> &attrs) const { attrs = attrs_out; }

void GHJoin::clearAll()
{
    free(tempRecord);
    free(tempRecord2);

    attrs_out.clear();
    attrs_leftIn.clear();
    attrs_rightIn.clear();

    //delete all temp file
    // cout<<"partitionName_left.size()"<<partitionName_left.size()<<endl;
    for(unsigned i = 0 ; i < partitionName_left.size() ; i++)
        rm->deleteTable(partitionName_left[i]);
    for(unsigned i = 0 ; i < partitionName_right.size() ; i++)
        rm->deleteTable(partitionName_right[i]);
    rm->deleteTable(resultName);

    partitionName_left.clear();
    partitionName_right.clear();

}
GHJoin::~GHJoin()
{
    free(tempRecord);
    free(tempRecord2);

    attrs_out.clear();
    attrs_leftIn.clear();
    attrs_rightIn.clear();

    //delete all temp file
    // cout<<"partitionName_left.size()"<<partitionName_left.size()<<endl;
    for(unsigned i = 0 ; i < partitionName_left.size() ; i++)
        rm->deleteTable(partitionName_left[i]);
    for(unsigned i = 0 ; i < partitionName_right.size() ; i++)
        rm->deleteTable(partitionName_right[i]);
    rm->deleteTable(resultName);

    partitionName_left.clear();
    partitionName_right.clear();
}


//                         //
//   Aggregation operator  //
//                         //
// Mandatory for graduate teams only
// Basic aggregation
Aggregate::Aggregate(Iterator *input,          // Iterator of input R
          Attribute aggAttr,        // The attribute over which we are computing an aggregate
          AggregateOp op)           // Aggregate operation
{
    this->input = input;
    this->op = op;
    this->aggAttr = aggAttr;
    input->getAttributes(inAttrs);
    tempRecord = malloc(1024);


    isDone = false;
   
    // get aggrAttrIndex && prepare outAttrs
    for(unsigned i = 0 ; i < inAttrs.size() ; i++)
    {
        if( inAttrs[i].name == aggAttr.name){
            aggAttrIndex = i;

            Attribute attr;
            attr = aggAttr;
            // attr.length = aggAttr.length;
            // attr.type = aggAttr.type;
            switch (op) {
              case MIN:
                  attr.name = "MIN(" + aggAttr.name + ")";
                  break;
              case MAX:
                  attr.name = "MAX(" + aggAttr.name + ")";
                  break;
              case SUM:
                  attr.name = "SUM(" + aggAttr.name + ")";
                  break;
              case AVG:
                  attr.name = "AVG(" + aggAttr.name + ")";
                  attr.type = TypeReal;
                  break;
              case COUNT:
                  attr.name = "COUNT(" + aggAttr.name + ")";
                  attr.type = TypeInt;
                  break;
              }
            outAttrs.push_back(attr);

            break;
        }
    }

}

// Optional for everyone. 5 extra-credit points
// Group-based hash aggregation
Aggregate::Aggregate(Iterator *input,             // Iterator of input R
          Attribute aggAttr,           // The attribute over which we are computing an aggregate
          Attribute groupAttr,         // The attribute over which we are grouping the tuples
          AggregateOp op,              // Aggregate operation
          const unsigned numPartitions)// Number of partitions for input (decided by the optimizer)
{

}

RC Aggregate::getNextTuple(void *data)
{

    if(isDone)
        return QE_EOF;

    // MIN_int;
    // MAX_int;
    // SUM_int = 0;
    COUNT_int = 0;

    // MIN_float;
    // MAX_float;
    // SUM_float = 0;
    AVG_float = 0;

    bool isFirstLoop = true;

    // bool isFinished = false;
    while(input->getNextTuple(tempRecord) == SUCCESS)
    {
        vector<AttrValue> attrsList;
        record2AttrsList(inAttrs, tempRecord, attrsList);
        //AttrValue tempAttrValue = attrsList[aggAttrIndex];
        int value_int;
        float value_float;

        if(aggAttr.type == TypeInt){
            // cout<<"0=================value_int"<<value_int<<endl;           
            value_int = *(int*)attrsList[aggAttrIndex].data; 
            // cout<<"1=================value_int"<<value_int<<endl;           
            if(isFirstLoop){
                MIN_int = value_int;
                MAX_int = value_int;
                SUM_int = value_int; 
                isFirstLoop = false;
            }else{
                if(MIN_int > value_int)
                    MIN_int = value_int;
                if(MAX_int < value_int)
                    MAX_int = value_int;
                SUM_int += value_int; 
            }  
        }else{
            // cout<<"0=================value_float"<<value_float<<endl;           
            value_float = *(float*)attrsList[aggAttrIndex].data;
            // cout<<"1=================value_float"<<value_float<<endl;           
            if(isFirstLoop){
                MIN_float = value_float;
                MAX_float = value_float;
                SUM_float = value_float; 
                isFirstLoop = false;
            }else{
                if(MIN_float > value_float)
                    MIN_float = value_float;
                if(MAX_float < value_float)
                    MAX_float = value_float;
                SUM_float += value_float; 
            }
        }
        COUNT_int++;
        clearAttrsList(attrsList);
    }

    switch(op){
    case MIN:
        if(aggAttr.type == TypeInt)
            *(int*)data = (int)MIN_int;
        else
            *(float*)data = (float)MIN_float;
        break;
    case MAX:
        if(aggAttr.type == TypeInt)
            *(int*)data = (int)MAX_int;
        else
            *(float*)data = (float)MAX_float;
        break;
    case SUM:
        if(aggAttr.type == TypeInt)
            *(int*)data = (int)SUM_int;
        else
            *(float*)data = (float)SUM_float;
        break;
    case AVG:
        if(aggAttr.type == TypeInt)
            *(float*)data = (float)SUM_int/COUNT_int;
        else
            *(float*)data = (float)SUM_float/COUNT_int;
        break;
    case COUNT:
        *(int*)data = COUNT_int;
        break;
    }

    isDone = true;
    return SUCCESS;
}

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const { attrs = outAttrs; }
Aggregate::~Aggregate(){ free(tempRecord); }

//			//
//	Filter  //
//			//
Filter::Filter(Iterator* input, const Condition &condition)
{
	this->input = input;
	this->condition = condition;
	input->getAttributes(inAttrs);
	tempRecord = malloc(1024);

	// get lhsAttrIndex
	for(unsigned i = 0 ; i < inAttrs.size() ; i++)
	{
		if( inAttrs[i].name == condition.lhsAttr){
			lhsAttrIndex = i;
			break;
		}
	}

	//for compare()
	lowKey = NULL;
    highKey = NULL;
    lowKeyInclusive = false;
    highKeyInclusive = false;
    unsigned dataSize = 0;

    if (condition.rhsValue.type == TypeInt || condition.rhsValue.type == TypeReal) { 
        dataSize = sizeof(int);
    }else{
        int varcharLength = *(int*)condition.rhsValue.data;
        dataSize = sizeof(int) + varcharLength;
    }
	
    switch(condition.op){
	case EQ_OP: // =
        lowKeyInclusive = true;
        highKeyInclusive = true;
        lowKey = malloc(dataSize);
        memcpy( lowKey , (char*)condition.rhsValue.data, dataSize);
        highKey = malloc(dataSize);
        memcpy( highKey , (char*)condition.rhsValue.data, dataSize);
    	break;
    case LT_OP: // <
        lowKey = NULL;
        highKey = malloc(dataSize);
        memcpy( highKey , (char*)condition.rhsValue.data, dataSize);
        break;
    case GT_OP: // >
        lowKey = malloc(dataSize);
        memcpy( lowKey , (char*)condition.rhsValue.data, dataSize);    
        highKey = NULL;
        break;
    case LE_OP: // <=
        lowKey = NULL;
        highKey = malloc(dataSize);
        memcpy( highKey , (char*)condition.rhsValue.data, dataSize);
        highKeyInclusive = true;
        break;
    case GE_OP: // >=
        lowKey = malloc(dataSize);
        memcpy( lowKey , (char*)condition.rhsValue.data, dataSize);    
        lowKeyInclusive = true;
        highKey = NULL;
        break;
    case NE_OP: // !=
        lowKeyInclusive = false;
        highKeyInclusive = false;
        lowKey = malloc(dataSize);
        memcpy( lowKey , (char*)condition.rhsValue.data, dataSize);
        highKey = malloc(dataSize);
        memcpy( highKey , (char*)condition.rhsValue.data, dataSize);
        break;
    case NO_OP: // no condition
        lowKey = NULL;
        highKey = NULL;
        break;
    }

}

/*
// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0,  // =
           LT_OP,      // <
           GT_OP,      // >
           LE_OP,      // <=
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;
struct Condition {
    string lhsAttr;         // left-hand side attribute                     
    CompOp  op;             // comparison operator                          
    //bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise
    //string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};
*/
RC Filter::getNextTuple(void *data) 
{

while(input->getNextTuple(tempRecord) == SUCCESS)
{
    // if(input->getNextTuple(tempRecord) == SUCCESS)
	{

        unsigned recordSize = 0;
        vector<AttrValue> attrsList;
        recordSize = record2AttrsList(inAttrs, tempRecord, attrsList);

        bool ifPass = false;
        int key_int = 0;
        float key_float = 0.0f;
        string key_string;
        char* key_cstring;

        // convert and compare key
        switch(condition.rhsValue.type)
        {
        case TypeReal:
            key_float = *(float *)attrsList[lhsAttrIndex].data;
            ifPass = compare(key_float);
            break;
        case TypeInt:
            key_int = *(int *)attrsList[lhsAttrIndex].data;
            ifPass = compare(key_int);
            break;
        case TypeVarChar:
            key_cstring = Varchart2CString(attrsList[lhsAttrIndex].data);
            key_string = string(key_cstring);
            ifPass = compare(key_string);
            break;
        }


        clearAttrsList(attrsList);

        // return SUCCESS or goto next iteration
        if(ifPass){            
            memcpy(data , tempRecord , recordSize);                    
            return SUCCESS;
        }else{
            continue;
        }
	}
}
	return QE_EOF;
}
bool Filter::compare(string key)
{
    string lowKey_string;
    string highKey_string;
    bool isNot_LowKeyNull = false;
    bool isNot_HighKeyNull = false;

    // # prepare variable
    if(lowKey != NULL){
        isNot_LowKeyNull = true;
        lowKey_string = string(Varchart2CString(lowKey));
    }
    if(highKey != NULL){
        isNot_HighKeyNull = true;
        highKey_string = string(Varchart2CString(highKey));
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
bool Filter::compare(int key)
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
    if(isNot_LowKeyNull && !isNot_HighKeyNull){
        if( key > lowKey_int)
            return true;
        else if(lowKeyInclusive && key == lowKey_int)
            return true;
    }
    // <= highKey
    if(isNot_HighKeyNull && !isNot_LowKeyNull){
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
bool Filter::compare(float key)
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

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const { attrs = inAttrs; }
Filter::~Filter(){ free(tempRecord);}

//			//
//	Project //
//			//
Project::Project(Iterator *input, const vector<string> &attrNames)
{
	this->input = input;
	input->getAttributes(inAttrs);
	pjAttrNames = attrNames; //copy vector
	// for (int i = 0 ; i < inAttrs.size() ; i++) {
	// 	pjAttrNames.insert(attrNames[i]);
	// }
	tempRecord = malloc(1024);
} 
RC Project::getNextTuple(void *data) 
{
	if(input->getNextTuple(tempRecord) == SUCCESS)
	{
		unsigned offset = 0;
		vector<AttrValue> attrsList;
		record2AttrsList(inAttrs, tempRecord, attrsList);

		for(unsigned i = 0 ; i < attrsList.size() ; i++)
		{
			if(find(pjAttrNames.begin(), pjAttrNames.end(), attrsList[i].attribute.name) != pjAttrNames.end()) 
			{
				memcpy((char*)data + offset , attrsList[i].data , attrsList[i].exactBytes);
				offset += attrsList[i].exactBytes;
			}
		}
		clearAttrsList(attrsList);

		return SUCCESS;
	}
	else
		return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void Project::getAttributes(vector<Attribute> &attrs) const
{

	attrs.clear();
	for(unsigned i = 0 ; i < inAttrs.size() ; i++)
	{
		if(find(pjAttrNames.begin(), pjAttrNames.end(), inAttrs[i].name) != pjAttrNames.end())
			attrs.push_back(inAttrs[i]);
	}
}
Project::~Project() { free(tempRecord); }

unsigned record2AttrsList(const vector<Attribute> &attrs, const void* record, vector<AttrValue> &attrsList)
{
	attrsList.clear();
	unsigned offset = 0;
	for(unsigned i = 0 ; i < attrs.size() ; i++)
	{
		Attribute tmpAttr = attrs[i];
		AttrValue attrValue;
		attrValue.attribute = tmpAttr;
		attrValue.attribute.type = tmpAttr.type;

		unsigned dataSize = 0;
		if (tmpAttr.type == TypeInt || tmpAttr.type == TypeReal) { 
			dataSize = sizeof(int);
		}else{
			int varcharLength = *(int*)((char*)record + offset);
			dataSize = sizeof(int) + varcharLength;
		}
        attrValue.data = malloc(dataSize);
        memcpy( attrValue.data , (char*)record + offset, dataSize);     
        attrValue.exactBytes = dataSize;
        offset += dataSize;
		
		attrsList.push_back(attrValue);
	}
    return offset;
}
void clearAttrsList(vector<AttrValue> &attrsList)
{
	for(unsigned i = 0 ; i < attrsList.size() ; i++)
	{
		free(attrsList[i].data);
	}
	attrsList.clear();
}
char* Varchart2CString(const void *recordBasedStr)
{
    int strLength = 0;
    memcpy(&strLength,recordBasedStr,sizeof(int));
    char* cstring = (char*)malloc(strLength+1);
    memcpy(cstring, (char*)recordBasedStr + sizeof(int) , strLength);
    cstring[strLength] = '\0';
    return cstring;
}
unsigned hashFunction(const Attribute &attribute, const void *key, unsigned numPartitions)
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

        hash = key_float;
        break;
    case TypeInt:
        int key_int;
        memcpy( &key_int, key, sizeof(int));
        hash = key_int;

        break;
    case TypeVarChar:
        char* keyStr = Varchart2CString(key);
        int key_varchar = 5381;
        int c;

        while (c = *keyStr++)
            key_varchar = ((key_varchar << 5) + key_varchar) + c; /* hash * 33 + c */

        hash = key_varchar;
        break;
    }

    return hash % numPartitions;
}

//This method is for get actual data length
int getDataLength(const vector<Attribute> &recordDescriptor, const void *data){
    int dataLength = 0;

    for(int i = 0; i < recordDescriptor.size(); i ++)
    {
        //check each element type: AttrType
        switch (recordDescriptor[i].type) {

        case TypeInt:
                dataLength+=sizeof(int);
                break;

        case TypeReal:
                dataLength+=sizeof(int);
                break;

        case TypeVarChar:
                int * aa;
                aa = (int *)((char *)data+dataLength);
                int length = *aa;

                dataLength+=length+sizeof(int);
                break;

        }//end of switch

    }//end of for each
    return dataLength;
}
